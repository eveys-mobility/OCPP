"""Unit tests for the cross-pod command bus (E2-10).

Covers envelope encode/decode, owning-side dispatch behaviour, and the
requester's wait-for-reply path. Tests run against a real Redis when
reachable and skip otherwise (same pattern as `tests/e2e/test_local_smoke.py`)
so `make tests` stays green on machines without the data plane up.
"""

from __future__ import annotations

import asyncio
import os
import socket
import time
from collections.abc import AsyncIterator
from contextlib import closing
from typing import Any
from unittest.mock import MagicMock

import pytest
from redis.asyncio import Redis

from eveys_ocpp.bus import (
    CMD_CHANNEL_PATTERN,
    ENVELOPE_VERSION,
    BusReply,
    CommandBus,
    cmd_channel,
    reply_channel,
)
from eveys_ocpp.connections import ConnectionMap

_REDIS_HOST = os.environ.get("E2E_REDIS_HOST", "localhost")
_REDIS_PORT = int(os.environ.get("E2E_REDIS_PORT", "6379"))
# In CI we set this to "1" so a missing Redis service surfaces as a real
# test failure instead of a silent skip that would let coverage drop.
# Mirrors the env var that the tests:e2e job already uses.
_REDIS_REQUIRED = os.environ.get("E2E_REQUIRE") == "1"


def _redis_reachable() -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.settimeout(0.5)
        try:
            s.connect((_REDIS_HOST, _REDIS_PORT))
        except OSError:
            return False
        return True


# When Redis is unreachable we either skip (dev laptop) or hard-fail (CI).
# A required-but-missing Redis on the unit job would otherwise let the bus
# module's coverage silently fall below the 80% gate.
if not _redis_reachable():
    _msg = f"Redis at {_REDIS_HOST}:{_REDIS_PORT} unreachable; bus tests need it"
    if _REDIS_REQUIRED:
        pytest.fail(
            f"{_msg}. E2E_REQUIRE=1 — the CI tests job must declare a "
            "`redis:7-alpine` service. CI config bug, not env issue.",
            pytrace=False,
        )
    pytestmark = pytest.mark.skip(reason=_msg)


# ---- fixtures ---------------------------------------------------------------


@pytest.fixture
async def redis_client() -> AsyncIterator[Redis]:
    client = Redis.from_url(
        f"redis://{_REDIS_HOST}:{_REDIS_PORT}/0",
        decode_responses=True,
    )
    yield client
    await client.aclose()


def _connected_cp(cp_id: str, ocpp_status: str) -> tuple[Any, ConnectionMap]:
    """Build a fake EveysChargePoint that replies with the given OCPP status."""
    cp = MagicMock()
    cp.id = cp_id
    response = MagicMock()
    response.status = ocpp_status
    cp.call = MagicMock(return_value=_done_future(response))
    cm = ConnectionMap()
    cm.add(cp)
    return cp, cm


def _done_future(value: Any) -> asyncio.Future[Any]:
    fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
    fut.set_result(value)
    return fut


async def _await_psubscribe(bus: CommandBus, redis: Redis, pattern: str) -> None:
    """Spin briefly so the subscriber tasks have actually subscribed.

    Redis ``PUBSUB NUMPAT`` returns the global pattern count; waiting for
    it to reflect this client's psubscribes is more reliable than a fixed
    sleep.
    """
    await bus.start()
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        # The bus uses a fresh pubsub object; we just need a moment for
        # the listen() coroutine to issue the SUBSCRIBE.
        await asyncio.sleep(0.05)
        try:
            count = await redis.pubsub_numpat()
            if int(count) >= 2:  # cmd pattern + reply pattern
                return
        except Exception:
            pass
    raise RuntimeError("bus subscriber didn't come up in 1s")


# ---- tests ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_round_trip_happy_path(redis_client: Redis) -> None:
    """Requester publishes, owning side dispatches, reply resolves the future."""
    _, owning_cm = _connected_cp("CP_A", "Accepted")
    requesting_cm = ConnectionMap()  # no local cp on this pod

    owning_bus = CommandBus(
        redis_client, pod_id="pod-A", connections=owning_cm, request_timeout_seconds=2.0
    )
    requesting_bus = CommandBus(
        redis_client, pod_id="pod-B", connections=requesting_cm, request_timeout_seconds=2.0
    )

    async def owning_dispatcher(rpc: str, cp_id: str, payload: dict[str, Any]) -> BusReply:
        assert rpc == "RemoteStart"
        assert cp_id == "CP_A"
        assert payload["id_tag"] == "TAG_42"
        return BusReply(ok=True, ocpp_status="Accepted")

    owning_bus.set_local_dispatcher(owning_dispatcher)

    await _await_psubscribe(owning_bus, redis_client, CMD_CHANNEL_PATTERN)
    await _await_psubscribe(requesting_bus, redis_client, CMD_CHANNEL_PATTERN)

    try:
        reply = await requesting_bus.request(
            cp_id="CP_A",
            owning_pod="pod-A",
            rpc="RemoteStart",
            payload={"id_tag": "TAG_42"},
            timeout=2.0,
        )
        assert reply.ok is True
        assert reply.ocpp_status == "Accepted"
    finally:
        await owning_bus.stop()
        await requesting_bus.stop()


@pytest.mark.asyncio
async def test_request_times_out_when_no_owner(redis_client: Redis) -> None:
    """No subscriber claims the cp_id → requester gets DEADLINE_EXCEEDED."""
    requesting_cm = ConnectionMap()
    requesting_bus = CommandBus(
        redis_client, pod_id="pod-B", connections=requesting_cm, request_timeout_seconds=0.3
    )
    await _await_psubscribe(requesting_bus, redis_client, CMD_CHANNEL_PATTERN)

    try:
        reply = await requesting_bus.request(
            cp_id="GHOST",
            owning_pod="pod-A",
            rpc="RemoteStart",
            payload={"id_tag": "X"},
            timeout=0.3,
        )
        assert reply.ok is False
        assert reply.error_code == "DEADLINE_EXCEEDED"
    finally:
        await requesting_bus.stop()


@pytest.mark.asyncio
async def test_owning_side_silent_when_cp_not_local(redis_client: Redis) -> None:
    """If the cp_id isn't in our ConnectionMap, we don't reply at all."""
    owning_cm = ConnectionMap()  # empty
    requesting_cm = ConnectionMap()

    owning_bus = CommandBus(
        redis_client, pod_id="pod-A", connections=owning_cm, request_timeout_seconds=0.3
    )
    requesting_bus = CommandBus(
        redis_client, pod_id="pod-B", connections=requesting_cm, request_timeout_seconds=0.3
    )

    dispatched = False

    async def should_not_run(*_: Any) -> BusReply:
        nonlocal dispatched
        dispatched = True
        return BusReply(ok=True, ocpp_status="Accepted")

    owning_bus.set_local_dispatcher(should_not_run)

    await _await_psubscribe(owning_bus, redis_client, CMD_CHANNEL_PATTERN)
    await _await_psubscribe(requesting_bus, redis_client, CMD_CHANNEL_PATTERN)

    try:
        reply = await requesting_bus.request(
            cp_id="CP_A",
            owning_pod="pod-A",
            rpc="RemoteStart",
            payload={"id_tag": "X"},
            timeout=0.3,
        )
        assert reply.ok is False
        assert reply.error_code == "DEADLINE_EXCEEDED"
        assert dispatched is False
    finally:
        await owning_bus.stop()
        await requesting_bus.stop()


@pytest.mark.asyncio
async def test_dispatcher_exception_yields_internal_error(redis_client: Redis) -> None:
    _, owning_cm = _connected_cp("CP_A", "Accepted")
    requesting_cm = ConnectionMap()

    owning_bus = CommandBus(
        redis_client, pod_id="pod-A", connections=owning_cm, request_timeout_seconds=2.0
    )
    requesting_bus = CommandBus(
        redis_client, pod_id="pod-B", connections=requesting_cm, request_timeout_seconds=2.0
    )

    async def boom(*_: Any) -> BusReply:
        raise RuntimeError("kaboom")

    owning_bus.set_local_dispatcher(boom)
    await _await_psubscribe(owning_bus, redis_client, CMD_CHANNEL_PATTERN)
    await _await_psubscribe(requesting_bus, redis_client, CMD_CHANNEL_PATTERN)

    try:
        reply = await requesting_bus.request(
            cp_id="CP_A",
            owning_pod="pod-A",
            rpc="RemoteStart",
            payload={"id_tag": "X"},
            timeout=2.0,
        )
        assert reply.ok is False
        assert reply.error_code == "INTERNAL"
        assert "kaboom" in (reply.error_message or "")
    finally:
        await owning_bus.stop()
        await requesting_bus.stop()


@pytest.mark.asyncio
async def test_version_skew_envelope_is_dropped(redis_client: Redis) -> None:
    """A v=2 inbound envelope must not be acted on (forward-compat guard)."""
    _, owning_cm = _connected_cp("CP_A", "Accepted")

    owning_bus = CommandBus(
        redis_client, pod_id="pod-A", connections=owning_cm, request_timeout_seconds=0.5
    )

    dispatched = False

    async def should_not_run(*_: Any) -> BusReply:
        nonlocal dispatched
        dispatched = True
        return BusReply(ok=True, ocpp_status="Accepted")

    owning_bus.set_local_dispatcher(should_not_run)
    await _await_psubscribe(owning_bus, redis_client, CMD_CHANNEL_PATTERN)

    try:
        # Hand-craft a future-version envelope and publish directly.
        import json as _json

        bad_envelope = _json.dumps(
            {
                "v": 2,
                "request_id": "req-0",
                "reply_to_pod": "pod-B",
                "rpc": "RemoteStart",
                "cp_id": "CP_A",
                "payload": {"id_tag": "X"},
                "deadline_ms": int((time.time() + 5) * 1000),
            }
        )
        await redis_client.publish(cmd_channel("CP_A"), bad_envelope)
        await asyncio.sleep(0.2)
        assert dispatched is False
    finally:
        await owning_bus.stop()


@pytest.mark.asyncio
async def test_expired_deadline_short_circuits_to_deadline_exceeded(
    redis_client: Redis,
) -> None:
    """An envelope whose deadline has already passed gets a fast error reply."""
    _, owning_cm = _connected_cp("CP_A", "Accepted")
    owning_bus = CommandBus(
        redis_client, pod_id="pod-A", connections=owning_cm, request_timeout_seconds=2.0
    )

    dispatched = False

    async def should_not_run(*_: Any) -> BusReply:
        nonlocal dispatched
        dispatched = True
        return BusReply(ok=True, ocpp_status="Accepted")

    owning_bus.set_local_dispatcher(should_not_run)
    await _await_psubscribe(owning_bus, redis_client, CMD_CHANNEL_PATTERN)

    # Subscribe directly to the reply channel for a known request_id.
    pubsub = redis_client.pubsub()
    request_id = "expired-1"
    await pubsub.subscribe(reply_channel(request_id))

    try:
        import json as _json

        envelope = _json.dumps(
            {
                "v": ENVELOPE_VERSION,
                "request_id": request_id,
                "reply_to_pod": "pod-B",
                "rpc": "RemoteStart",
                "cp_id": "CP_A",
                "payload": {"id_tag": "X"},
                "deadline_ms": int((time.time() - 5) * 1000),  # already past
            }
        )
        await redis_client.publish(cmd_channel("CP_A"), envelope)

        deadline = time.monotonic() + 2.0
        got: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)
            if msg is None:
                continue
            got = _json.loads(msg["data"])
            break

        assert got is not None
        assert got["status"] == "error"
        assert got["error_code"] == "DEADLINE_EXCEEDED"
        assert dispatched is False
    finally:
        await pubsub.unsubscribe(reply_channel(request_id))
        await pubsub.aclose()
        await owning_bus.stop()
