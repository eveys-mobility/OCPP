"""Unit tests for the WS server's `cp.offline_duration` publisher.

Same shape as `test_ws_lifecycle_events.py` — `_on_connect` itself
needs a real ServerConnection (compose-smoke territory); the helper
that computes the seconds + serializes the envelope is what we cover
here.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.transport.ws_server import _publish_offline_duration


@pytest.mark.asyncio
async def test_publishes_envelope_with_computed_seconds() -> None:
    producer = AsyncMock()
    marker = {
        "went_offline_at": "2026-05-11T12:00:00+00:00",
        "pod_id": "pod-prev",
        "reason": "clean",
    }
    came_online_at = datetime(2026, 5, 11, 12, 5, 30, tzinfo=UTC)

    await _publish_offline_duration(
        event_producer=producer,
        cp_id="CP_TEST",
        topic="cp.offline_duration",
        marker=marker,
        came_online_at=came_online_at,
    )

    producer.publish.assert_awaited_once()
    kwargs = producer.publish.await_args.kwargs
    assert kwargs["topic"] == "cp.offline_duration"
    assert kwargs["key"] == "CP_TEST"

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(kwargs["value"])
    assert envelope.WhichOneof("payload") == "cp_offline_duration"
    payload = envelope.cp_offline_duration
    assert payload.offline_seconds == 5 * 60 + 30
    assert payload.went_offline_at == "2026-05-11T12:00:00+00:00"
    assert payload.came_online_at == came_online_at.isoformat()
    assert payload.prior_pod_id == "pod-prev"
    assert payload.prior_reason == "clean"


@pytest.mark.asyncio
async def test_skips_publish_on_missing_timestamp() -> None:
    """A marker stored before this field was set (legacy / bug) must
    not crash the connect path. We drop the event and continue."""
    producer = AsyncMock()
    await _publish_offline_duration(
        event_producer=producer,
        cp_id="CP_TEST",
        topic="cp.offline_duration",
        marker={"pod_id": "pod-prev", "reason": "clean"},
        came_online_at=datetime.now(UTC),
    )
    producer.publish.assert_not_called()


@pytest.mark.asyncio
async def test_skips_publish_on_bad_timestamp() -> None:
    producer = AsyncMock()
    await _publish_offline_duration(
        event_producer=producer,
        cp_id="CP_TEST",
        topic="cp.offline_duration",
        marker={"went_offline_at": "not-a-date"},
        came_online_at=datetime.now(UTC),
    )
    producer.publish.assert_not_called()


@pytest.mark.asyncio
async def test_no_producer_is_noop() -> None:
    await _publish_offline_duration(
        event_producer=None,
        cp_id="CP_TEST",
        topic="cp.offline_duration",
        marker={"went_offline_at": "2026-05-11T12:00:00+00:00"},
        came_online_at=datetime(2026, 5, 11, 12, 1, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_broker_drop_does_not_raise() -> None:
    producer = AsyncMock()
    producer.publish = AsyncMock(side_effect=RuntimeError("kafka down"))
    await _publish_offline_duration(
        event_producer=producer,
        cp_id="CP_TEST",
        topic="cp.offline_duration",
        marker={"went_offline_at": "2026-05-11T12:00:00+00:00"},
        came_online_at=datetime(2026, 5, 11, 12, 1, tzinfo=UTC),
    )
