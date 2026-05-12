"""In-process fan-out for the SSE `/charge-points/{cp_id}/events` stream.

One ``AIOKafkaConsumer`` per gateway pod subscribes to every topic
that carries a ``cp_id`` in its envelope. Each open SSE response
registers a per-subscriber ``asyncio.Queue`` keyed by ``cp_id``;
the bus drops messages into every queue subscribed to that ``cp_id``
and lets the endpoint do the SSE framing.

Why in-process instead of a separate sidecar:

- A Console session is short-lived (open the page, look, close it).
  A sidecar would mean a network hop + a second protocol for what's
  already a fan-out of bytes the gateway already has in memory.
- The bus is per-pod, non-durable. Cross-pod fan-out is unnecessary:
  every pod subscribed to the topic sees every event, so a Console
  attached to *any* pod sees the full event stream for any CP.

Backpressure: per-subscriber bounded queue. ``put_nowait`` is used so
a slow consumer doesn't block the Kafka consumer — when ``QueueFull``
is raised the subscription is marked ``dropped`` and the endpoint
closes the stream with an ``error`` event. Same shape the OCPP
handlers use for misbehaving chargers (drop, don't stall).

Group id is non-durable: ``<prefix>-<pod_id>-<uuid>`` with
``auto_offset_reset='latest'``. SSE is strictly "tail from now," not
"replay history". Restarting a pod loses no offsets because there are
none to lose.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from aiokafka import AIOKafkaConsumer

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.observability import bind_contextvars, get_logger

if TYPE_CHECKING:
    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


@dataclass(eq=False)
class _Subscription:
    """One open SSE response's view of the bus.

    The endpoint owns the ``queue`` and the ``dropped`` flag — when
    ``dropped`` flips True the endpoint sees a sentinel in the next
    read and closes the stream.

    ``eq=False`` so instances hash by identity — they live in a
    ``set[_Subscription]`` per cp_id and identity is what we want
    (two different open responses for the same CP must be distinct).
    """

    cp_id: str
    queue: asyncio.Queue[dict[str, object] | None]
    dropped: bool = False
    # Tag for logs: a uuid so a single Console session is traceable
    # across pod logs.
    subscription_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])


class SseBus:
    """Process-wide singleton; one consumer task drains Kafka and
    fans out to every active subscriber.

    Lifecycle:
        bus = SseBus(settings)
        await bus.start()
        # ...serve traffic...
        await bus.stop()

    Subscribers attach via ``subscribe(cp_id) -> _Subscription`` and
    detach via ``unsubscribe(sub)``. Each subscription's queue holds
    either dict messages (one per envelope) or ``None`` as the
    end-of-stream sentinel.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task[None] | None = None
        self._shutdown = asyncio.Event()
        # Subscribers keyed by cp_id → set of _Subscription. A single
        # cp_id may have multiple open Console tabs; each gets its own
        # queue. Set semantics so unsubscribe is O(1).
        self._subs: dict[str, set[_Subscription]] = {}
        # Module-level lock so subscribe + unsubscribe + the consumer
        # loop never see a half-mutated dict. The critical sections
        # are tiny so contention is negligible.
        self._lock = asyncio.Lock()

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        """Open the Kafka consumer and launch the drain task."""
        if self.running:  # idempotent — double-start is harmless
            return

        topics = self._topics()
        group_id = (
            f"{self._settings.sse_kafka_group_prefix}-"
            f"{self._settings.pod_id}-{uuid.uuid4().hex[:8]}"
        )
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self._settings.kafka_brokers,
            group_id=group_id,
            client_id=f"eveys-ocpp-sse-{self._settings.pod_id}",
            enable_auto_commit=True,
            # Tail from now — SSE is real-time, not a backfill channel.
            auto_offset_reset="latest",
        )
        await consumer.start()
        self._consumer = consumer
        self._task = asyncio.create_task(self._drain_loop(), name="sse_bus_drain")
        log.info(
            "sse_bus.started",
            topics=list(topics),
            group_id=group_id,
            queue_max_size=self._settings.sse_queue_max_size,
        )

    async def stop(self) -> None:
        """Cancel the drain task, close the Kafka consumer, and signal
        every active subscriber so the endpoint can clean up."""
        self._shutdown.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        # Wake every still-active subscriber so the endpoint can
        # close. Sentinel = None.
        async with self._lock:
            for subs in self._subs.values():
                for sub in subs:
                    # Already saturated → the endpoint will exit on
                    # the next read regardless.
                    with contextlib.suppress(asyncio.QueueFull):
                        sub.queue.put_nowait(None)
            self._subs.clear()
        log.info("sse_bus.stopped")

    async def subscribe(self, cp_id: str) -> _Subscription:
        """Register a new subscriber for the given CP. The returned
        ``_Subscription.queue`` is what the SSE response reads from.
        """
        sub = _Subscription(
            cp_id=cp_id,
            queue=asyncio.Queue(maxsize=self._settings.sse_queue_max_size),
        )
        async with self._lock:
            self._subs.setdefault(cp_id, set()).add(sub)
        log.info(
            "sse_bus.subscribe",
            cp_id=cp_id,
            subscription_id=sub.subscription_id,
            subscribers_for_cp=len(self._subs[cp_id]),
        )
        return sub

    async def unsubscribe(self, sub: _Subscription) -> None:
        """Detach a subscriber. Idempotent."""
        async with self._lock:
            bucket = self._subs.get(sub.cp_id)
            if bucket is None:
                return
            bucket.discard(sub)
            if not bucket:
                self._subs.pop(sub.cp_id, None)
        log.info(
            "sse_bus.unsubscribe",
            cp_id=sub.cp_id,
            subscription_id=sub.subscription_id,
            dropped=sub.dropped,
        )

    def _topics(self) -> tuple[str, ...]:
        s = self._settings
        return (
            s.kafka_topic_cp_connected,
            s.kafka_topic_cp_disconnected,
            s.kafka_topic_cp_offline_duration,
            s.kafka_topic_cp_boot,
            s.kafka_topic_cp_status,
            s.kafka_topic_cp_meter,
            s.kafka_topic_cp_firmware_status,
            s.kafka_topic_cp_diagnostics_status,
            s.kafka_topic_tx_started,
            s.kafka_topic_tx_stopped,
            s.kafka_topic_cp_security_event,
        )

    async def _drain_loop(self) -> None:
        """Read messages forever, fan each one to its cp_id's queues."""
        assert self._consumer is not None  # narrowed by start()
        try:
            async for record in self._consumer:
                if self._shutdown.is_set():
                    break
                try:
                    envelope = events_pb2.EventEnvelope()
                    envelope.ParseFromString(record.value)
                except Exception as exc:
                    log.warning(
                        "sse_bus.parse_failed",
                        topic=record.topic,
                        offset=record.offset,
                        error=str(exc),
                    )
                    continue

                payload = _envelope_to_sse_payload(envelope)
                if payload is None:
                    continue
                await self._fan_out(envelope.cp_id, payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("sse_bus.drain_loop_crashed")
            raise

    async def _fan_out(self, cp_id: str, payload: dict[str, object]) -> None:
        """Push the rendered payload into every queue subscribed to
        this cp_id. Slow consumers are marked dropped and woken with
        the None sentinel so the endpoint closes their stream."""
        async with self._lock:
            bucket = self._subs.get(cp_id)
            if not bucket:
                return
            slow: list[_Subscription] = []
            for sub in bucket:
                if sub.dropped:
                    # Already marked; don't double-log or re-enqueue
                    # a sentinel.
                    continue
                try:
                    sub.queue.put_nowait(payload)
                except asyncio.QueueFull:
                    sub.dropped = True
                    slow.append(sub)
            for sub in slow:
                # Drop one queued message to make room for the
                # sentinel — the subscriber is already going to be
                # closed, so losing one event is acceptable. Without
                # this the sentinel never lands and the endpoint
                # waits on a queue that no one will fill.
                with contextlib.suppress(asyncio.QueueEmpty):
                    sub.queue.get_nowait()
                with contextlib.suppress(asyncio.QueueFull):
                    sub.queue.put_nowait(None)
        if slow:
            for sub in slow:
                bind_contextvars(cp_id=cp_id, subscription_id=sub.subscription_id)
                log.warning(
                    "sse_bus.slow_consumer_dropped",
                    queue_max_size=self._settings.sse_queue_max_size,
                )


def _envelope_to_sse_payload(env: events_pb2.EventEnvelope) -> dict[str, object] | None:
    """Convert one `EventEnvelope` into the dict the endpoint serializes
    as an SSE event.

    Returns ``{"event": <type>, "data": <json-shape>}``; ``None`` for
    payload variants we don't surface (e.g. credential-rotated audit
    events that operators shouldn't see in the live UI).

    Event-type strings match the proto ``oneof`` field name without the
    ``cp_`` / ``tx_`` prefix so the Console can switch on a short
    label: ``connected``, ``disconnected``, ``offline_duration``,
    ``boot``, ``status``, ``meter``, ``firmware_status_changed``,
    ``diagnostics_status_changed``, ``tx_started``, ``tx_stopped``,
    ``security_event``.
    """
    which = env.WhichOneof("payload")
    if which is None:
        return None
    # Strip the proto-side prefix so the Console event label matches
    # the URL slug pattern (`tx.started` → `tx_started`).
    label = which
    if label.startswith("cp_"):
        label = label[len("cp_") :]

    common = {
        "event_id": env.event_id,
        "occurred_at": env.occurred_at,
        "cp_id": env.cp_id,
        "schema_version": env.schema_version,
    }
    data: dict[str, object] = {**common}

    if which == "cp_connected":
        data["subprotocol"] = env.cp_connected.subprotocol
        data["pod_id"] = env.cp_connected.pod_id
    elif which == "cp_disconnected":
        data["pod_id"] = env.cp_disconnected.pod_id
        data["reason"] = env.cp_disconnected.reason
    elif which == "cp_offline_duration":
        p = env.cp_offline_duration
        data["went_offline_at"] = p.went_offline_at
        data["came_online_at"] = p.came_online_at
        data["offline_seconds"] = p.offline_seconds
        data["prior_pod_id"] = p.prior_pod_id
        data["prior_reason"] = p.prior_reason
    elif which == "cp_boot":
        p = env.cp_boot
        data["vendor"] = p.vendor
        data["model"] = p.model
        data["firmware_version"] = p.firmware_version
        data["serial_number"] = p.serial_number
        data["status"] = events_pb2.CpBootStatus.Name(p.status)
    elif which == "cp_status":
        p = env.cp_status
        data["connector_id"] = p.connector_id
        data["status"] = p.status
        data["error_code"] = p.error_code or None
        data["info"] = p.info or None
        data["vendor_id"] = p.vendor_id or None
        data["vendor_error_code"] = p.vendor_error_code or None
        data["charger_reported_at"] = p.charger_reported_at or None
    elif which == "cp_meter":
        p = env.cp_meter
        data["connector_id"] = p.connector_id
        data["transaction_id"] = p.transaction_id or None
        data["charger_reported_at"] = p.charger_reported_at or None
        data["sampled_values"] = [
            {
                "value": sv.value,
                "context": _enum_or_none(events_pb2.Context, sv.context),
                "format": _enum_or_none(events_pb2.Format, sv.format),
                "measurand": _enum_or_none(events_pb2.Measurand, sv.measurand),
                "phase": _enum_or_none(events_pb2.Phase, sv.phase),
                "location": _enum_or_none(events_pb2.Location, sv.location),
                "unit": _enum_or_none(events_pb2.Unit, sv.unit),
            }
            for sv in p.sampled_values
        ]
    elif which == "cp_firmware_status_changed":
        data["status"] = env.cp_firmware_status_changed.status
    elif which == "cp_diagnostics_status_changed":
        data["status"] = env.cp_diagnostics_status_changed.status
    elif which == "tx_started":
        p = env.tx_started
        data["transaction_id"] = p.transaction_id
        data["connector_id"] = p.connector_id
        data["id_tag"] = p.id_tag
        data["meter_start_wh"] = p.meter_start_wh
        data["charger_reported_at"] = p.charger_reported_at or None
    elif which == "tx_stopped":
        p = env.tx_stopped
        data["transaction_id"] = p.transaction_id
        data["id_tag"] = p.id_tag
        data["meter_stop_wh"] = p.meter_stop_wh
        data["consumed_wh"] = p.consumed_wh
        data["stop_reason"] = p.stop_reason or None
        data["charger_reported_at"] = p.charger_reported_at or None
    elif which == "cp_security_event":
        p = env.cp_security_event
        data["type"] = p.type
        data["tech_info"] = p.tech_info or None
        data["charger_reported_at"] = p.charger_reported_at or None
    else:
        # Credential-rotated and CSR-submitted are operator-audit
        # events. Leaving them off the live UI surface intentionally;
        # add explicit cases here if the Console grows a need.
        return None

    return {"event": label, "data": data}


def _enum_or_none(enum_descriptor: object, value: int) -> str | None:
    """Translate a proto enum integer back to its OCPP wire-form name.

    The proto's UNSPECIFIED variant (value 0) maps to ``None`` so SSE
    consumers don't see the proto sentinel leak through. Anything else
    surfaces as the proto enum *name* (e.g. ``MEASURAND_VOLTAGE``) —
    same form the ClickHouse rows use; the Console already knows how
    to render it.
    """
    if value == 0:
        return None
    name: str = enum_descriptor.Name(value)  # type: ignore[attr-defined]
    return name
