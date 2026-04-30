"""Cross-pod command bus over Redis pub/sub (E2-10).

The gRPC command surface (E2-5, E2-6) routes charger-targeted RPCs to
the pod that owns the charger's WebSocket. Same-pod path is direct;
off-pod path uses this module.

Wire model
----------
Two channel families:

- ``cp:cmd:{cp_id}``         — request channel. The pod that owns the
                               WS receives this via a *pattern*
                               subscription (``cp:cmd:*``) and filters
                               by local ``ConnectionMap`` membership.
- ``cp:reply:{request_id}``  — reply channel, one per in-flight RPC.
                               Listened to via a pattern subscription
                               (``cp:reply:*``) so we don't need to
                               subscribe per request.

Why pattern subscriptions: at 10k chargers per pod (E4-6 target),
per-charger subscriptions burn one Redis subscription each. A single
pattern subscription per pod is O(1) regardless of charger count. The
trade-off is that all pods see all replies; we discard ones not in
``_inflight``. For two-pod scale this is trivial; if it ever bites us
we switch to per-request subscribe-then-publish.

Envelope (JSON):

  request:  {"v": 1, "request_id": "<uuid4>", "reply_to_pod": "<pod>",
             "rpc": "RemoteStart", "cp_id": "...",
             "payload": {...}, "deadline_ms": <epoch_ms>}
  reply:    {"v": 1, "request_id": "<uuid4>",
             "status": "ok"|"error",
             "ocpp_status": "Accepted"|...,        (status=ok)
             "error_code": "NOT_FOUND"|"DEADLINE_EXCEEDED"|"INTERNAL",
             "error_message": "..."}              (status=error)

The bus is internal-only — chargers never see it. Plain JSON keeps it
cheap to evolve; ``v`` is a hard reject on mismatch so a partial rollout
fails loudly rather than silently.

Failure model
-------------
Redis pub/sub is at-most-once. A pod that dies between request publish
and the charger reply produces ``DEADLINE_EXCEEDED`` on the requester —
same outcome as a flaky charger; callers already handle it. See
ADR-0016.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from eveys_ocpp.connections import ConnectionMap

log = get_logger(__name__)

ENVELOPE_VERSION = 1
CMD_CHANNEL_PREFIX = "cp:cmd:"
REPLY_CHANNEL_PREFIX = "cp:reply:"
CMD_CHANNEL_PATTERN = f"{CMD_CHANNEL_PREFIX}*"
REPLY_CHANNEL_PATTERN = f"{REPLY_CHANNEL_PREFIX}*"


def cmd_channel(cp_id: str) -> str:
    return f"{CMD_CHANNEL_PREFIX}{cp_id}"


def reply_channel(request_id: str) -> str:
    return f"{REPLY_CHANNEL_PREFIX}{request_id}"


@dataclass(frozen=True, slots=True)
class BusReply:
    """Result of a cross-pod RPC, in the form the requester needs.

    ``ok`` carries an OCPP status string ("Accepted", "Rejected", ...)
    that the caller's existing translator turns into a proto enum.
    ``error`` carries a stable ``error_code`` so the caller can map to
    the right gRPC status.
    """

    ok: bool
    ocpp_status: str | None = None
    error_code: str | None = None
    error_message: str | None = None


# (rpc_name, cp_id, payload) -> BusReply
LocalDispatcher = Callable[[str, str, dict[str, Any]], Awaitable[BusReply]]


class CommandBus:
    """Pub/sub command bus shared across pods.

    One instance per process. Started in ``__main__`` once the
    ``ConnectionMap`` exists, stopped on shutdown. There are no
    per-charger subscribe/unsubscribe calls — the bus filters by
    ``ConnectionMap`` membership at receive time.
    """

    def __init__(
        self,
        redis: Redis,
        *,
        pod_id: str,
        connections: ConnectionMap,
        local_dispatcher: LocalDispatcher | None = None,
        request_timeout_seconds: float = 30.0,
    ) -> None:
        self._redis = redis
        self._pod_id = pod_id
        self._connections = connections
        self._local_dispatcher = local_dispatcher
        self._request_timeout = request_timeout_seconds

        self._cmd_subscriber: asyncio.Task[None] | None = None
        self._reply_subscriber: asyncio.Task[None] | None = None
        # Per-message dispatch tasks — tracked so they aren't GC'd
        # mid-flight (asyncio holds only weak refs to bare create_task).
        self._cmd_handler_tasks: set[asyncio.Task[None]] = set()
        # request_id -> Future awaited by the requesting side.
        self._inflight: dict[str, asyncio.Future[BusReply]] = {}

    # ---- lifecycle -----------------------------------------------------

    def set_local_dispatcher(self, dispatcher: LocalDispatcher) -> None:
        """Inject the owning-side dispatcher.

        Done out-of-band so this module doesn't import from
        ``transport.grpc_server`` (which holds the dispatch registry).
        Avoids a circular import: grpc_server -> bus -> grpc_server.
        """
        self._local_dispatcher = dispatcher

    async def start(self) -> None:
        """Begin listening for inbound commands and replies.

        psubscribes synchronously *before* spawning the listener tasks so
        that ``start()`` returning means the bus is genuinely ready to
        receive — a subsequent ``request()`` won't lose the reply to a
        race against a half-started subscriber. Two background tasks,
        one per channel family. Idempotent.
        """
        if self._cmd_subscriber is not None:
            return

        self._cmd_pubsub = self._redis.pubsub()
        await self._cmd_pubsub.psubscribe(CMD_CHANNEL_PATTERN)
        self._reply_pubsub = self._redis.pubsub()
        await self._reply_pubsub.psubscribe(REPLY_CHANNEL_PATTERN)
        log.info("bus.psubscribed", patterns=[CMD_CHANNEL_PATTERN, REPLY_CHANNEL_PATTERN])

        self._cmd_subscriber = asyncio.create_task(
            self._run_cmd_subscriber(self._cmd_pubsub),
            name="bus-cmd-subscriber",
        )
        self._reply_subscriber = asyncio.create_task(
            self._run_reply_subscriber(self._reply_pubsub),
            name="bus-reply-subscriber",
        )
        log.info("bus.started", pod_id=self._pod_id)

    async def stop(self) -> None:
        """Cancel subscribers and fail any in-flight requests."""
        for task in (self._cmd_subscriber, self._reply_subscriber):
            if task is not None and not task.done():
                task.cancel()
        for task in (self._cmd_subscriber, self._reply_subscriber):
            if task is not None:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._cmd_subscriber = None
        self._reply_subscriber = None
        for pubsub in (
            getattr(self, "_cmd_pubsub", None),
            getattr(self, "_reply_pubsub", None),
        ):
            if pubsub is not None:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()
        for fut in self._inflight.values():
            if not fut.done():
                fut.set_exception(RuntimeError("bus stopped"))
        self._inflight.clear()
        log.info("bus.stopped", pod_id=self._pod_id)

    # ---- requesting side -----------------------------------------------

    async def request(
        self,
        *,
        cp_id: str,
        owning_pod: str,
        rpc: str,
        payload: dict[str, Any],
        timeout: float | None = None,
    ) -> BusReply:
        """Send an RPC to the pod owning ``cp_id`` and await the reply.

        ``owning_pod`` is informational (logging only). The actual delivery
        is fan-out via ``cp:cmd:{cp_id}``; the one subscriber whose
        ``ConnectionMap`` contains ``cp_id`` answers.
        """
        request_id = str(uuid.uuid4())
        deadline = self._request_timeout if timeout is None else timeout
        envelope = {
            "v": ENVELOPE_VERSION,
            "request_id": request_id,
            "reply_to_pod": self._pod_id,
            "rpc": rpc,
            "cp_id": cp_id,
            "payload": payload,
            "deadline_ms": int((time.time() + deadline) * 1000),
        }

        loop = asyncio.get_running_loop()
        future: asyncio.Future[BusReply] = loop.create_future()
        self._inflight[request_id] = future
        try:
            log.info(
                "bus.request.publish",
                rpc=rpc,
                cp_id=cp_id,
                owning_pod=owning_pod,
                request_id=request_id,
            )
            await self._redis.publish(cmd_channel(cp_id), json.dumps(envelope))
            try:
                return await asyncio.wait_for(future, timeout=deadline)
            except TimeoutError:
                log.warning("bus.request.timeout", rpc=rpc, cp_id=cp_id, request_id=request_id)
                return BusReply(
                    ok=False,
                    error_code="DEADLINE_EXCEEDED",
                    error_message=f"no reply within {deadline}s",
                )
        finally:
            self._inflight.pop(request_id, None)

    # ---- owning side ---------------------------------------------------

    async def _run_cmd_subscriber(self, pubsub: Any) -> None:
        """Consume ``cp:cmd:*``, dispatch ones we own, ignore the rest.

        ``pubsub`` is psubscribed by ``start()`` before this task runs;
        cleanup is also done in ``stop()``.
        """
        async for message in pubsub.listen():
            if message.get("type") != "pmessage":
                continue
            # Spawn — don't block the listener on a slow OCPP round-trip.
            # Track tasks to avoid them being garbage-collected mid-flight.
            task = asyncio.create_task(self._handle_cmd_message(message))
            self._cmd_handler_tasks.add(task)
            task.add_done_callback(self._cmd_handler_tasks.discard)

    async def _handle_cmd_message(self, message: dict[str, Any]) -> None:
        envelope = _decode_envelope(message)
        if envelope is None:
            return
        if envelope.get("v") != ENVELOPE_VERSION:
            log.warning("bus.cmd.version_skew", got=envelope.get("v"))
            return

        cp_id = envelope.get("cp_id")
        request_id = envelope.get("request_id")
        rpc = envelope.get("rpc")
        reply_to = envelope.get("reply_to_pod")
        payload = envelope.get("payload") or {}
        deadline_ms = envelope.get("deadline_ms")

        if not (cp_id and request_id and rpc and reply_to):
            log.warning("bus.cmd.incomplete_envelope", request_id=request_id)
            return

        # Only respond if the WS is on this pod. Otherwise some other pod
        # is the owner and will answer; multiple "not me" replies would
        # just spam the requester.
        if cp_id not in self._connections:
            return

        if isinstance(deadline_ms, int) and deadline_ms < int(time.time() * 1000):
            log.warning("bus.cmd.expired", rpc=rpc, cp_id=cp_id, request_id=request_id)
            await self._publish_reply(
                request_id,
                BusReply(
                    ok=False,
                    error_code="DEADLINE_EXCEEDED",
                    error_message="request expired before owning pod picked it up",
                ),
            )
            return

        if self._local_dispatcher is None:
            log.error("bus.cmd.no_dispatcher", rpc=rpc, cp_id=cp_id)
            await self._publish_reply(
                request_id,
                BusReply(
                    ok=False,
                    error_code="INTERNAL",
                    error_message="owning pod has no local dispatcher configured",
                ),
            )
            return

        try:
            reply = await self._local_dispatcher(rpc, cp_id, payload)
        except Exception as exc:
            log.exception("bus.cmd.dispatch_failed", rpc=rpc, cp_id=cp_id)
            reply = BusReply(
                ok=False,
                error_code="INTERNAL",
                error_message=f"{type(exc).__name__}: {exc}",
            )
        await self._publish_reply(request_id, reply)

    async def _publish_reply(self, request_id: str, reply: BusReply) -> None:
        envelope: dict[str, Any] = {
            "v": ENVELOPE_VERSION,
            "request_id": request_id,
            "status": "ok" if reply.ok else "error",
        }
        if reply.ok:
            envelope["ocpp_status"] = reply.ocpp_status
        else:
            envelope["error_code"] = reply.error_code
            envelope["error_message"] = reply.error_message
        await self._redis.publish(reply_channel(request_id), json.dumps(envelope))

    # ---- reply subscriber ----------------------------------------------

    async def _run_reply_subscriber(self, pubsub: Any) -> None:
        async for message in pubsub.listen():
            if message.get("type") != "pmessage":
                continue
            self._handle_reply_message(message)

    def _handle_reply_message(self, message: dict[str, Any]) -> None:
        envelope = _decode_envelope(message)
        if envelope is None or envelope.get("v") != ENVELOPE_VERSION:
            return

        request_id = envelope.get("request_id")
        future = self._inflight.get(request_id) if request_id else None
        if future is None or future.done():
            return  # not ours, or already resolved (timeout)

        if envelope.get("status") == "ok":
            future.set_result(BusReply(ok=True, ocpp_status=envelope.get("ocpp_status")))
        else:
            future.set_result(
                BusReply(
                    ok=False,
                    error_code=envelope.get("error_code"),
                    error_message=envelope.get("error_message"),
                )
            )


def _decode_envelope(message: dict[str, Any]) -> dict[str, Any] | None:
    raw = message.get("data")
    if isinstance(raw, bytes):
        raw = raw.decode()
    if not isinstance(raw, str):
        log.warning("bus.envelope.not_string", got=type(raw).__name__)
        return None
    try:
        envelope = json.loads(raw)
    except ValueError as exc:
        log.warning("bus.envelope.malformed", error=str(exc))
        return None
    if not isinstance(envelope, dict):
        log.warning("bus.envelope.not_object")
        return None
    return envelope


__all__ = [
    "CMD_CHANNEL_PATTERN",
    "CMD_CHANNEL_PREFIX",
    "ENVELOPE_VERSION",
    "REPLY_CHANNEL_PATTERN",
    "REPLY_CHANNEL_PREFIX",
    "BusReply",
    "CommandBus",
    "LocalDispatcher",
    "cmd_channel",
    "reply_channel",
]
