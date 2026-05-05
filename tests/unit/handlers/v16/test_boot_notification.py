"""Unit tests for the BootNotification handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.handlers.v16 import boot_notification


@pytest.mark.asyncio
async def test_returns_accepted_with_configured_interval(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    result = await boot_notification.handle(
        fake_cp,
        charge_point_vendor="ACME",
        charge_point_model="X1",
        firmware_version="1.0.0",
        charge_point_serial_number="SN001",
    )

    assert isinstance(result, call_result.BootNotification)
    assert result.status == "Accepted"
    assert result.interval == fake_cp.settings.heartbeat_interval_seconds
    upsert.assert_awaited_once()


@pytest.mark.asyncio
async def test_persists_charger_metadata(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    await boot_notification.handle(
        fake_cp,
        charge_point_vendor="ACME",
        charge_point_model="X1",
        firmware_version="1.0.0",
        charge_point_serial_number="SN001",
    )

    call = upsert.await_args
    assert call is not None
    kwargs = call.kwargs
    assert kwargs["cp_id"] == "TEST_CP_001"
    assert kwargs["vendor"] == "ACME"
    assert kwargs["model"] == "X1"
    assert kwargs["firmware_version"] == "1.0.0"
    assert kwargs["serial_number"] == "SN001"


@pytest.mark.asyncio
async def test_handles_missing_optional_fields(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.status == "Accepted"


# ---- E2-8 Kafka emit -------------------------------------------------------


@pytest.mark.asyncio
async def test_publishes_to_cp_boot_topic_when_producer_attached(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await boot_notification.handle(
        fake_cp,
        charge_point_vendor="ACME",
        charge_point_model="X1",
        firmware_version="1.0.0",
        charge_point_serial_number="SN001",
    )

    fake_producer.publish.assert_awaited_once()
    call_kwargs = fake_producer.publish.await_args.kwargs
    assert call_kwargs["topic"] == fake_cp.settings.kafka_topic_cp_boot
    assert call_kwargs["key"] == "TEST_CP_001"

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(call_kwargs["value"])
    assert envelope.cp_id == "TEST_CP_001"
    assert envelope.HasField("cp_boot")
    assert envelope.cp_boot.vendor == "ACME"
    assert envelope.cp_boot.model == "X1"
    assert envelope.cp_boot.firmware_version == "1.0.0"
    assert envelope.cp_boot.serial_number == "SN001"
    assert envelope.cp_boot.status == events_pb2.CP_BOOT_STATUS_ACCEPTED


@pytest.mark.asyncio
async def test_no_publish_when_producer_is_none(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Kafka-less local stack: handler still returns Accepted."""
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())
    fake_cp.event_producer = None

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.status == "Accepted"


@pytest.mark.asyncio
async def test_handler_survives_kafka_publish_exception(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A broker drop must not crash the OCPP handler — chargers retry
    aggressively and a flaky broker would otherwise DoS the gateway."""
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())
    fake_producer = AsyncMock()
    fake_producer.publish = AsyncMock(side_effect=RuntimeError("broker is down"))
    fake_cp.event_producer = fake_producer

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert isinstance(result, call_result.BootNotification)
    assert result.status == "Accepted"
    fake_producer.publish.assert_awaited_once()


# ---- E2-11 idempotency -----------------------------------------------------


@pytest.mark.asyncio
async def test_replay_skips_db_and_kafka(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Cache hit → return Accepted without DB write OR Kafka emit.

    Critical: the platform never sees a duplicate `cp.boot` event for
    a retried request. Catching the replay before the publish is the
    whole point of E2-11.
    """
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(return_value=True)  # replay
    fake_cp.idempotency = fake_idem
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    result = await boot_notification.handle(
        fake_cp,
        message_id="MSG-RETRY-1",
        charge_point_vendor="ACME",
    )

    assert result.status == "Accepted"
    upsert.assert_not_awaited()
    fake_producer.publish.assert_not_awaited()
    fake_idem.check_and_record.assert_awaited_once_with(
        cp_id="TEST_CP_001", message_id="MSG-RETRY-1"
    )


@pytest.mark.asyncio
async def test_first_sighting_runs_handler(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(return_value=False)  # not a replay
    fake_cp.idempotency = fake_idem

    result = await boot_notification.handle(
        fake_cp,
        message_id="MSG-FIRST",
        charge_point_vendor="ACME",
    )

    assert result.status == "Accepted"
    upsert.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_message_id_falls_through(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing message_id → can't dedup; run the handler normally.

    Defensive — the OCPP library always supplies a message_id, but the
    handler should still work if a test or future caller doesn't.
    """
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    fake_idem = AsyncMock()
    fake_cp.idempotency = fake_idem

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.status == "Accepted"
    upsert.assert_awaited_once()
    fake_idem.check_and_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_idempotency_cache_falls_through(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cache attribute None (unit-test setup, Redis-less local stack) →
    handler still runs."""
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)
    fake_cp.idempotency = None

    result = await boot_notification.handle(fake_cp, message_id="MSG-X", charge_point_vendor="ACME")

    assert result.status == "Accepted"
    upsert.assert_awaited_once()


@pytest.mark.asyncio
async def test_cache_outage_falls_through(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """If the cache raises (Redis down), don't wedge the handler.

    Better a rare double-write than a stuck charger when the cache
    misbehaves. Documented in ADR-0017.
    """
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(side_effect=RuntimeError("redis down"))
    fake_cp.idempotency = fake_idem

    result = await boot_notification.handle(fake_cp, message_id="MSG-Y", charge_point_vendor="ACME")

    assert result.status == "Accepted"
    upsert.assert_awaited_once()
