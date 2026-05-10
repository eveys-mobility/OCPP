"""Unit tests for the WS server's `cp.online` / `cp.offline` lifecycle
publishers (#167).

The full `_on_connect` coroutine needs a real `ServerConnection` to
exercise — that's compose-smoke territory. What's worth unit-testing
is the `_publish_lifecycle_event` helper itself: best-effort guard
under broker drop, no-op on `event_producer is None`, correct
envelope shape on the happy path.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.transport.ws_server import _publish_lifecycle_event


@pytest.mark.asyncio
async def test_publishes_envelope_on_happy_path() -> None:
    """`event_producer` set → one publish with the correct topic, key,
    and a parseable EventEnvelope wrapping the payload."""
    producer = AsyncMock()

    await _publish_lifecycle_event(
        event_producer=producer,
        cp_id="CP_TEST",
        topic="cp.connected",
        payload_field="cp_connected",
        payload=events_pb2.CpConnected(subprotocol="ocpp1.6", pod_id="pod-1"),
    )

    producer.publish.assert_awaited_once()
    kwargs = producer.publish.await_args.kwargs
    assert kwargs["topic"] == "cp.connected"
    assert kwargs["key"] == "CP_TEST"

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(kwargs["value"])
    assert envelope.cp_id == "CP_TEST"
    assert envelope.WhichOneof("payload") == "cp_connected"
    assert envelope.cp_connected.subprotocol == "ocpp1.6"
    assert envelope.cp_connected.pod_id == "pod-1"


@pytest.mark.asyncio
async def test_no_producer_is_noop() -> None:
    """Compose-smoke / unit-test contexts can run without a Kafka
    producer; the lifecycle path must not raise on a None producer."""
    # No assertion target — the absence of a raised exception IS the test.
    await _publish_lifecycle_event(
        event_producer=None,
        cp_id="CP_TEST",
        topic="cp.connected",
        payload_field="cp_connected",
        payload=events_pb2.CpConnected(),
    )


@pytest.mark.asyncio
async def test_broker_drop_does_not_raise() -> None:
    """A flaky broker MUST NOT crash the WS lifecycle. Without this
    guard, every disconnect against a down Kafka would raise out of
    the `_on_connect` finally-block, masking the real disconnect
    reason and leaving the metrics + log lines half-emitted."""
    producer = AsyncMock()
    producer.publish = AsyncMock(side_effect=RuntimeError("kafka down"))

    # Returns normally; the warning is logged via structlog, not raised.
    await _publish_lifecycle_event(
        event_producer=producer,
        cp_id="CP_TEST",
        topic="cp.disconnected",
        payload_field="cp_disconnected",
        payload=events_pb2.CpDisconnected(pod_id="pod-1", reason="error"),
    )


@pytest.mark.asyncio
async def test_disconnect_envelope_carries_reason() -> None:
    """`reason` distinguishes clean (1000-OK close) from error (any
    unhandled exception out of the connection task) — the same dimension
    `eveys_ocpp_ws_disconnects_total` already labels by, so a CDR
    consumer can correlate."""
    producer = AsyncMock()

    await _publish_lifecycle_event(
        event_producer=producer,
        cp_id="CP_TEST",
        topic="cp.disconnected",
        payload_field="cp_disconnected",
        payload=events_pb2.CpDisconnected(pod_id="pod-1", reason="error"),
    )

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(producer.publish.await_args.kwargs["value"])
    assert envelope.WhichOneof("payload") == "cp_disconnected"
    assert envelope.cp_disconnected.reason == "error"
    assert envelope.cp_disconnected.pod_id == "pod-1"
