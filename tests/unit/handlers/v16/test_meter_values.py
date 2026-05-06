"""Unit tests for the MeterValues handler (E2-1)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.handlers.v16 import meter_values


def _sample(value: str, unit: str = "Wh") -> dict[str, Any]:
    return {"value": value, "unit": unit, "measurand": "Energy.Active.Import.Register"}


@pytest.mark.asyncio
async def test_returns_empty_response(fake_cp: Any) -> None:
    """OCPP MeterValuesResponse has no body."""
    fake_cp.event_producer = None
    result = await meter_values.handle(
        fake_cp,
        connector_id=1,
        meter_value=[
            {
                "timestamp": "2026-04-30T00:00:00Z",
                "sampled_value": [_sample("1234")],
            }
        ],
    )
    assert isinstance(result, call_result.MeterValues)


@pytest.mark.asyncio
async def test_publishes_to_kafka_when_producer_attached(fake_cp: Any) -> None:
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await meter_values.handle(
        fake_cp,
        connector_id=1,
        transaction_id=42,
        meter_value=[
            {
                "timestamp": "2026-04-30T00:00:00+00:00",
                "sampled_value": [_sample("1500"), _sample("1600")],
            }
        ],
    )

    fake_producer.publish.assert_awaited_once()
    call_kwargs = fake_producer.publish.await_args.kwargs
    assert call_kwargs["topic"] == fake_cp.settings.kafka_topic_cp_meter
    assert call_kwargs["key"] == "TEST_CP_001"

    # Decode the envelope and check the payload routed to cp_meter.
    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(call_kwargs["value"])
    assert envelope.cp_id == "TEST_CP_001"
    assert envelope.HasField("cp_meter")
    assert envelope.cp_meter.connector_id == 1
    assert envelope.cp_meter.transaction_id == 42
    assert len(envelope.cp_meter.sampled_values) == 2
    assert envelope.cp_meter.charger_reported_at == "2026-04-30T00:00:00+00:00"


@pytest.mark.asyncio
async def test_no_publish_when_producer_is_none(fake_cp: Any) -> None:
    """Kafka-less local stack: handler still returns successfully."""
    fake_cp.event_producer = None
    result = await meter_values.handle(
        fake_cp,
        connector_id=1,
        meter_value=[{"timestamp": "x", "sampled_value": [_sample("100")]}],
    )
    assert isinstance(result, call_result.MeterValues)


@pytest.mark.asyncio
async def test_no_publish_when_no_samples(fake_cp: Any) -> None:
    """Empty sampled_value list → don't publish an empty envelope."""
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await meter_values.handle(
        fake_cp,
        connector_id=1,
        meter_value=[{"timestamp": "x", "sampled_value": []}],
    )
    fake_producer.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_quarantines_absurd_values(fake_cp: Any) -> None:
    """101 MWh single sample → dropped, log + warn, no publish for it.

    Also exercises the unit-aware sanity check (kWh, MWh).
    """
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await meter_values.handle(
        fake_cp,
        connector_id=1,
        meter_value=[
            {
                "timestamp": "x",
                "sampled_value": [
                    _sample("101", unit="MWh"),  # quarantined: 101 MWh > 100 MWh cap
                    _sample("50", unit="kWh"),  # 50 kWh = 50_000 Wh — fine
                    _sample("1234"),  # plain Wh — fine
                ],
            }
        ],
    )

    fake_producer.publish.assert_awaited_once()
    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(fake_producer.publish.await_args.kwargs["value"])
    # Only 2 valid samples; the absurd one was dropped.
    assert len(envelope.cp_meter.sampled_values) == 2


@pytest.mark.asyncio
async def test_tolerates_malformed_entries(fake_cp: Any) -> None:
    """Non-dict garbage in meter_value or sampled_value: skip, don't crash."""
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    result = await meter_values.handle(
        fake_cp,
        connector_id=1,
        meter_value=[
            "junk",  # type: ignore[list-item]
            {
                "timestamp": "x",
                "sampled_value": ["junk", _sample("100")],  # type: ignore[list-item]
            },
        ],
    )
    assert isinstance(result, call_result.MeterValues)
    fake_producer.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_unparseable_value_is_quarantined(fake_cp: Any) -> None:
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await meter_values.handle(
        fake_cp,
        connector_id=1,
        meter_value=[
            {"timestamp": "x", "sampled_value": [{"value": "not-a-number", "unit": "Wh"}]}
        ],
    )
    # No samples got through → no publish.
    fake_producer.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_survives_kafka_publish_exception(fake_cp: Any) -> None:
    """A broker drop must not crash the handler — chargers retry, and a
    flaky broker would otherwise DoS the gateway. Aligns with E2-8 where
    the same guard is added to BootNotification, StatusNotification, and
    StartTransaction."""
    fake_producer = AsyncMock()
    fake_producer.publish = AsyncMock(side_effect=RuntimeError("broker is down"))
    fake_cp.event_producer = fake_producer

    result = await meter_values.handle(
        fake_cp,
        connector_id=1,
        meter_value=[{"timestamp": "x", "sampled_value": [_sample("100")]}],
    )

    assert isinstance(result, call_result.MeterValues)
    fake_producer.publish.assert_awaited_once()
