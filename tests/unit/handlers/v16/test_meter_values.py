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
async def test_quarantines_per_measurand_range_and_keeps_others(fake_cp: Any) -> None:
    """E5-4: a bad voltage report drops only the voltage; the energy
    sample in the same envelope is forwarded. The Prometheus counter
    increments with `measurand=Voltage, reason=out_of_range`."""
    from eveys_ocpp.metrics import registry as metrics_registry

    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    counter = metrics_registry.METER_VALUE_QUARANTINED_TOTAL.labels(
        measurand="Voltage", reason="out_of_range"
    )
    before = counter._value.get()

    await meter_values.handle(
        fake_cp,
        connector_id=1,
        meter_value=[
            {
                "timestamp": "x",
                "sampled_value": [
                    {"value": "2500", "measurand": "Voltage", "unit": "V"},  # bad
                    {"value": "30000", "measurand": "Energy.Active.Import.Register"},
                ],
            }
        ],
    )

    assert counter._value.get() == before + 1
    fake_producer.publish.assert_awaited_once()
    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(fake_producer.publish.await_args.kwargs["value"])
    # Only the energy sample survives.
    assert len(envelope.cp_meter.sampled_values) == 1


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


# ----- enum mapping (#135) --------------------------------------------------


@pytest.mark.parametrize(
    ("ocpp_field", "wire_value", "proto_field", "expected_proto_value"),
    [
        # Measurand
        ("measurand", "Voltage", "measurand", events_pb2.MEASURAND_VOLTAGE),
        ("measurand", "SoC", "measurand", events_pb2.MEASURAND_SOC),
        ("measurand", "Current.Import", "measurand", events_pb2.MEASURAND_CURRENT_IMPORT),
        ("measurand", "Power.Active.Import", "measurand", events_pb2.MEASURAND_POWER_ACTIVE_IMPORT),
        (
            "measurand",
            "Energy.Active.Import.Register",
            "measurand",
            events_pb2.MEASURAND_ENERGY_ACTIVE_IMPORT_REGISTER,
        ),
        # Phase — including the dashed variants that aren't a mechanical
        # uppercase/replace transform of the proto enum name.
        ("phase", "L1", "phase", events_pb2.PHASE_L1),
        ("phase", "L2", "phase", events_pb2.PHASE_L2),
        ("phase", "L3", "phase", events_pb2.PHASE_L3),
        ("phase", "L1-N", "phase", events_pb2.PHASE_L1_N),
        ("phase", "L2-L3", "phase", events_pb2.PHASE_L2_L3),
        # Unit — case-sensitive distinction `kW` vs `W` matters.
        ("unit", "V", "unit", events_pb2.UNIT_V),
        ("unit", "W", "unit", events_pb2.UNIT_W),
        ("unit", "kW", "unit", events_pb2.UNIT_KW),
        ("unit", "Percent", "unit", events_pb2.UNIT_PERCENT),
        # Context / Format / Location.
        ("context", "Sample.Periodic", "context", events_pb2.CONTEXT_SAMPLE_PERIODIC),
        ("context", "Transaction.End", "context", events_pb2.CONTEXT_TRANSACTION_END),
        ("format", "SignedData", "format", events_pb2.FORMAT_SIGNED_DATA),
        ("location", "Outlet", "location", events_pb2.LOCATION_OUTLET),
    ],
)
def test_to_proto_maps_ocpp_wire_string_to_enum(
    ocpp_field: str,
    wire_value: str,
    proto_field: str,
    expected_proto_value: int,
) -> None:
    """OCPP 1.6 §6.21 wire strings must round-trip into proto enum values
    (not the literal `*_UNSPECIFIED` they were stored as before #135)."""
    raw: dict[str, Any] = {"value": "1.0", ocpp_field: wire_value}
    sv = meter_values._to_proto_sampled_value(raw)
    assert getattr(sv, proto_field) == expected_proto_value


def test_to_proto_unknown_measurand_falls_back_to_unspecified() -> None:
    """Vendor-extension measurand strings keep the value but land on
    `MEASURAND_UNSPECIFIED` — no crash, no data loss."""
    sv = meter_values._to_proto_sampled_value(
        {"value": "42", "measurand": "Vendor.Custom.Reading", "unit": "V"}
    )
    assert sv.value == "42"
    assert sv.measurand == events_pb2.MEASURAND_UNSPECIFIED
    assert sv.unit == events_pb2.UNIT_V


def test_to_proto_absent_measurand_defaults_to_energy_active_import_register() -> None:
    """OCPP 1.6 §6.21.4: an absent measurand means
    `Energy.Active.Import.Register`. Without this default, consumers
    filtering by that measurand would silently miss bare-energy
    samples (the most common shape chargers emit)."""
    sv = meter_values._to_proto_sampled_value({"value": "1234", "unit": "Wh"})
    assert sv.measurand == events_pb2.MEASURAND_ENERGY_ACTIVE_IMPORT_REGISTER


@pytest.mark.asyncio
async def test_published_envelope_carries_proto_enum_values(fake_cp: Any) -> None:
    """End-to-end inside the handler: a charger sample with measurand /
    phase / unit lands on Kafka with the proto enum *values* set, not
    `_UNSPECIFIED`. This is the assertion that would have caught #135
    on the unit tier — the originally-shipped handler passed every
    other test in this file because none of them looked at enum fields."""
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await meter_values.handle(
        fake_cp,
        connector_id=1,
        transaction_id=99,
        meter_value=[
            {
                "timestamp": "2026-04-30T00:00:00+00:00",
                "sampled_value": [
                    {"value": "230.4", "measurand": "Voltage", "unit": "V", "phase": "L1"},
                ],
            }
        ],
    )

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(fake_producer.publish.await_args.kwargs["value"])
    [sv] = envelope.cp_meter.sampled_values
    assert sv.value == "230.4"
    assert sv.measurand == events_pb2.MEASURAND_VOLTAGE
    assert sv.phase == events_pb2.PHASE_L1
    assert sv.unit == events_pb2.UNIT_V


@pytest.mark.asyncio
async def test_pending_cp_raises_security_error(fake_cp: Any) -> None:
    """A pending device must be refused with a CALLERROR and never
    reach the Kafka publish path — MeterValues bypasses Postgres, but
    the gate still stops the event emit."""
    from unittest.mock import MagicMock

    from ocpp.exceptions import SecurityError

    fake_cp.is_pending = True
    fake_cp.session_factory = MagicMock(
        side_effect=AssertionError("session_factory must not be used while pending")
    )
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    with pytest.raises(SecurityError):
        await meter_values.handle(
            fake_cp,
            connector_id=1,
            meter_value=[{"timestamp": "2026-07-02T16:00:00Z", "sampled_value": [_sample("1234")]}],
        )

    fake_producer.publish.assert_not_awaited()
