"""Unit tests for the Heartbeat handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result

from eveys_ocpp.handlers.v16 import heartbeat


@pytest.mark.asyncio
async def test_returns_current_time_iso(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(heartbeat, "update_heartbeat", AsyncMock())

    result = await heartbeat.handle(fake_cp)

    assert isinstance(result, call_result.Heartbeat)
    # Reasonable shape: ISO 8601 with timezone.
    assert "T" in result.current_time
    assert result.current_time.endswith("+00:00")


@pytest.mark.asyncio
async def test_refreshes_last_heartbeat(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    update = AsyncMock()
    monkeypatch.setattr(heartbeat, "update_heartbeat", update)

    await heartbeat.handle(fake_cp)

    update.assert_awaited_once()
    assert update.await_args is not None
    assert update.await_args.kwargs["cp_id"] == "TEST_CP_001"


@pytest.mark.asyncio
async def test_refreshes_redis_ttl_when_registry_attached(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(heartbeat, "update_heartbeat", AsyncMock())

    fake_registry = AsyncMock()
    fake_registry.refresh = AsyncMock(return_value=True)
    fake_cp.registry = fake_registry

    await heartbeat.handle(fake_cp)

    fake_registry.refresh.assert_awaited_once_with("TEST_CP_001")
    fake_registry.mark_online.assert_not_awaited()


@pytest.mark.asyncio
async def test_reclaims_registry_when_key_expired(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If TTL expired between heartbeats, re-mark the charger as online."""
    monkeypatch.setattr(heartbeat, "update_heartbeat", AsyncMock())

    fake_registry = AsyncMock()
    fake_registry.refresh = AsyncMock(return_value=False)
    fake_cp.registry = fake_registry

    await heartbeat.handle(fake_cp)

    fake_registry.refresh.assert_awaited_once_with("TEST_CP_001")
    fake_registry.mark_online.assert_awaited_once_with("TEST_CP_001")


@pytest.mark.asyncio
async def test_no_registry_calls_when_registry_is_none(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """W1-style local stack without Redis: handler must still work."""
    monkeypatch.setattr(heartbeat, "update_heartbeat", AsyncMock())
    fake_cp.registry = None

    result = await heartbeat.handle(fake_cp)
    assert result is not None  # no exception


@pytest.mark.asyncio
async def test_publishes_cp_heartbeat_envelope_when_event_producer_attached(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Per ADR-0027 the gateway emits a cp.heartbeat envelope on every
    Heartbeat so the backend can refresh `last_online` between
    connect/disconnect lifecycle events."""
    from eveys_ocpp._generated.events.v1 import events_pb2

    monkeypatch.setattr(heartbeat, "update_heartbeat", AsyncMock())

    fake_cp.event_producer = AsyncMock()
    fake_cp.event_producer.publish = AsyncMock()

    await heartbeat.handle(fake_cp)

    fake_cp.event_producer.publish.assert_awaited_once()
    call = fake_cp.event_producer.publish.await_args
    assert call.kwargs["topic"] == fake_cp.settings.kafka_topic_cp_heartbeat
    assert call.kwargs["key"] == "TEST_CP_001"

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(call.kwargs["value"])
    assert envelope.cp_id == "TEST_CP_001"
    assert envelope.WhichOneof("payload") == "cp_heartbeat"


@pytest.mark.asyncio
async def test_heartbeat_publish_failure_is_swallowed(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Kafka broker drop MUST NOT block the Heartbeat response —
    same best-effort pattern boot / meter_values use."""
    monkeypatch.setattr(heartbeat, "update_heartbeat", AsyncMock())

    fake_cp.event_producer = AsyncMock()
    fake_cp.event_producer.publish = AsyncMock(side_effect=RuntimeError("broker down"))

    result = await heartbeat.handle(fake_cp)

    assert result is not None  # response still returned, exception logged


@pytest.mark.asyncio
async def test_pending_cp_raises_security_error(fake_cp: Any) -> None:
    """A pending device must be refused with a CALLERROR and never
    touch Postgres or Redis."""
    from unittest.mock import MagicMock

    from ocpp.exceptions import SecurityError

    fake_cp.is_pending = True
    fake_cp.session_factory = MagicMock(
        side_effect=AssertionError("session_factory must not be used while pending")
    )
    fake_cp.registry = AsyncMock()
    fake_cp.registry.refresh = AsyncMock(
        side_effect=AssertionError("registry must not be touched while pending")
    )

    with pytest.raises(SecurityError):
        await heartbeat.handle(fake_cp)

    fake_cp.registry.refresh.assert_not_awaited()
