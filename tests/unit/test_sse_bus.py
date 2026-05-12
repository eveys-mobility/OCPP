"""Unit tests for the SSE bus.

The Kafka consumer / AIOKafkaConsumer integration is exercised in
compose-smoke; here we cover the pure-Python pieces:

- The envelope → SSE-payload projection (one case per event variant).
- subscribe / unsubscribe lifecycle.
- Slow-consumer drop (queue saturation marks the subscriber dropped
  and wakes it with the None sentinel).
- Stop wakes every still-active subscriber.

The bus's internal Kafka consumer is never started in these tests;
we exercise the fan-out and lifecycle methods directly.
"""

from __future__ import annotations

import asyncio

import pytest

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.settings import Settings
from eveys_ocpp.sse_bus import SseBus, _envelope_to_sse_payload


def _envelope(**payload_kwargs: object) -> events_pb2.EventEnvelope:
    return events_pb2.EventEnvelope(
        event_id="evt-1",
        occurred_at="2026-05-12T12:00:00+00:00",
        cp_id="CP_TEST",
        schema_version="v1",
        **payload_kwargs,
    )


# ---- envelope → payload projection ---------------------------------


def test_payload_for_tx_started() -> None:
    env = _envelope(
        tx_started=events_pb2.TxStarted(
            transaction_id=42,
            connector_id=1,
            id_tag="RFID_001",
            meter_start_wh=1_000_000,
        ),
    )
    payload = _envelope_to_sse_payload(env)
    assert payload is not None
    assert payload["event"] == "tx_started"
    data = payload["data"]
    assert isinstance(data, dict)
    assert data["transaction_id"] == 42
    assert data["id_tag"] == "RFID_001"
    assert data["cp_id"] == "CP_TEST"


def test_payload_for_cp_meter_emits_wire_form_enum_names() -> None:
    """Storage form for measurand/phase/unit is the proto enum *name*.
    UNSPECIFIED → None so the proto sentinel never leaks to clients."""
    env = _envelope(
        cp_meter=events_pb2.CpMeter(
            connector_id=1,
            transaction_id=42,
            sampled_values=[
                events_pb2.SampledValue(
                    value="230.5",
                    measurand=events_pb2.MEASURAND_VOLTAGE,
                    unit=events_pb2.UNIT_V,
                    phase=events_pb2.PHASE_L1,
                ),
                events_pb2.SampledValue(value="0", measurand=0, unit=0, phase=0),
            ],
        ),
    )
    payload = _envelope_to_sse_payload(env)
    assert payload is not None
    sv = payload["data"]["sampled_values"]  # type: ignore[index]
    assert sv[0]["measurand"] == "MEASURAND_VOLTAGE"
    assert sv[0]["unit"] == "UNIT_V"
    assert sv[0]["phase"] == "PHASE_L1"
    # UNSPECIFIED variants → None
    assert sv[1]["measurand"] is None
    assert sv[1]["unit"] is None


def test_payload_for_offline_duration() -> None:
    env = _envelope(
        cp_offline_duration=events_pb2.CpOfflineDuration(
            went_offline_at="2026-05-12T11:55:00+00:00",
            came_online_at="2026-05-12T12:00:00+00:00",
            offline_seconds=300,
            prior_pod_id="pod-A",
            prior_reason="clean",
        ),
    )
    payload = _envelope_to_sse_payload(env)
    assert payload is not None
    assert payload["event"] == "offline_duration"
    assert payload["data"]["offline_seconds"] == 300


def test_payload_for_cp_disconnected_strips_cp_prefix_for_event_label() -> None:
    env = _envelope(
        cp_disconnected=events_pb2.CpDisconnected(pod_id="pod-A", reason="clean"),
    )
    payload = _envelope_to_sse_payload(env)
    assert payload is not None
    assert payload["event"] == "disconnected"
    assert payload["data"]["reason"] == "clean"


def test_payload_for_credential_rotated_is_skipped() -> None:
    """Operator-audit events shouldn't surface on the live UI stream.
    The bus returns None and the consumer drops the message."""
    env = _envelope(
        cp_credential_rotated=events_pb2.CpCredentialRotated(action="set", actor="ops@x"),
    )
    assert _envelope_to_sse_payload(env) is None


def test_payload_for_empty_envelope_is_skipped() -> None:
    env = _envelope()  # no oneof variant set
    assert _envelope_to_sse_payload(env) is None


# ---- bus lifecycle ---------------------------------------------------


@pytest.fixture
def settings() -> Settings:
    """Small queue so slow-consumer tests hit saturation fast."""
    return Settings(sse_queue_max_size=2, sse_heartbeat_seconds=0.05)


@pytest.mark.asyncio
async def test_subscribe_returns_queue_with_bounded_size(settings: Settings) -> None:
    bus = SseBus(settings)
    sub = await bus.subscribe("CP_A")
    assert sub.cp_id == "CP_A"
    assert sub.queue.maxsize == settings.sse_queue_max_size
    await bus.unsubscribe(sub)


@pytest.mark.asyncio
async def test_unsubscribe_is_idempotent(settings: Settings) -> None:
    bus = SseBus(settings)
    sub = await bus.subscribe("CP_A")
    await bus.unsubscribe(sub)
    # Second call is a no-op, not a crash.
    await bus.unsubscribe(sub)


@pytest.mark.asyncio
async def test_fan_out_delivers_to_every_subscriber_of_a_cp(settings: Settings) -> None:
    bus = SseBus(settings)
    sub_a = await bus.subscribe("CP_A")
    sub_b = await bus.subscribe("CP_A")
    other = await bus.subscribe("CP_B")

    await bus._fan_out("CP_A", {"event": "tx_started", "data": {"x": 1}})

    msg_a = await asyncio.wait_for(sub_a.queue.get(), timeout=0.5)
    msg_b = await asyncio.wait_for(sub_b.queue.get(), timeout=0.5)
    assert msg_a == msg_b == {"event": "tx_started", "data": {"x": 1}}
    # CP_B subscriber must NOT see CP_A events.
    assert other.queue.empty()


@pytest.mark.asyncio
async def test_slow_consumer_is_marked_dropped_and_woken(settings: Settings) -> None:
    """Saturating a subscriber's bounded queue marks it dropped and
    puts the None sentinel so the endpoint exits the read loop."""
    bus = SseBus(settings)
    sub = await bus.subscribe("CP_A")

    # Fill the queue past its maxsize without draining.
    for i in range(settings.sse_queue_max_size + 5):
        await bus._fan_out("CP_A", {"event": "tx_started", "data": {"i": i}})

    assert sub.dropped is True
    # Drain queued events, then expect the None sentinel.
    saw_sentinel = False
    for _ in range(settings.sse_queue_max_size + 2):
        msg = await asyncio.wait_for(sub.queue.get(), timeout=0.5)
        if msg is None:
            saw_sentinel = True
            break
    assert saw_sentinel


@pytest.mark.asyncio
async def test_stop_wakes_every_subscriber(settings: Settings) -> None:
    bus = SseBus(settings)
    sub_a = await bus.subscribe("CP_A")
    sub_b = await bus.subscribe("CP_B")

    await bus.stop()

    assert await asyncio.wait_for(sub_a.queue.get(), timeout=0.5) is None
    assert await asyncio.wait_for(sub_b.queue.get(), timeout=0.5) is None
