"""Unit tests for the BootNotification handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

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
    # The OCPP subprotocol negotiated on the WS upgrade is captured
    # here too so the Console can show "OCPP 1.6 / 2.0.1" on the
    # detail page without guessing. fake_cp's connection reports
    # 1.6 (the only subprotocol the gateway accepts today); when
    # 2.0.1 lands the same field flips per-row.
    assert kwargs["ocpp_version"] == "ocpp1.6"


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


# ---- E3-5: backend `/charge-points/register` wiring ------------------------


def _register_result(
    status: str = "Accepted",
    interval: int = 60,
    cp_id: str = "TEST_CP_001",
) -> Any:
    from eveys_ocpp.platform import ChargePointRegisterResult

    return ChargePointRegisterResult(
        cp_id=cp_id,
        registration_status=status,
        heartbeat_interval_seconds=interval,
        request_id="req-abc",
        command_id=4421,
    )


@pytest.mark.asyncio
async def test_calls_backend_register_when_client_wired(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.register_charge_point = AsyncMock(return_value=_register_result())

    await boot_notification.handle(
        fake_cp,
        message_id="MSG-Z",
        charge_point_vendor="ACME",
        charge_point_model="X1",
        firmware_version="1.0.0",
        charge_point_serial_number="SN001",
    )

    fake_cp.backend_client.register_charge_point.assert_awaited_once()
    kwargs = fake_cp.backend_client.register_charge_point.await_args.kwargs
    assert kwargs["cp_id"] == "TEST_CP_001"
    assert kwargs["vendor"] == "ACME"
    assert kwargs["model"] == "X1"
    assert kwargs["firmware_version"] == "1.0.0"
    assert kwargs["serial_number"] == "SN001"
    # Idempotency-Key shape mirrors the contract.
    assert kwargs["idempotency_key"] == "ocpp-boot-TEST_CP_001-MSG-Z"
    # boot_at is an ISO 8601 string sourced from `received_at`.
    assert "T" in kwargs["boot_at"]


@pytest.mark.asyncio
async def test_forwards_pending_status_from_backend(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())
    producer = AsyncMock()
    producer.publish = AsyncMock()
    fake_cp.event_producer = producer
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.register_charge_point = AsyncMock(
        return_value=_register_result(status="Pending", interval=10)
    )

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.status == "Pending"
    assert result.interval == 10
    # Pending must NOT emit cp.boot — downstream materializes only on Accepted.
    producer.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_forwards_rejected_status_from_backend(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())
    producer = AsyncMock()
    producer.publish = AsyncMock()
    fake_cp.event_producer = producer
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.register_charge_point = AsyncMock(
        return_value=_register_result(status="Rejected")
    )

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.status == "Rejected"
    producer.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_uses_backend_heartbeat_interval(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Backend value (60) wins over the gateway's default (300)."""
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.register_charge_point = AsyncMock(
        return_value=_register_result(interval=42)
    )

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.interval == 42
    assert result.interval != fake_cp.settings.heartbeat_interval_seconds


@pytest.mark.asyncio
async def test_unrecognised_backend_status_maps_to_pending(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Forward-compat: an unknown registration_status from the backend
    maps to Pending — the safe default per the OCPP spec."""
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.register_charge_point = AsyncMock(
        return_value=_register_result(status="SomethingNew")
    )

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.status == "Pending"


@pytest.mark.asyncio
async def test_backend_unavailable_accept_offline_fallback(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.platform import BackendUnavailableError

    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())
    producer = AsyncMock()
    producer.publish = AsyncMock()
    fake_cp.event_producer = producer
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.register_charge_point = AsyncMock(
        side_effect=BackendUnavailableError("backend timeout", error_code="TIMEOUT"),
    )
    # Default policy is accept_offline.
    assert fake_cp.settings.backend_register_fallback == "accept_offline"

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.status == "Accepted"
    assert result.interval == fake_cp.settings.heartbeat_interval_seconds
    # Accepted fallback DOES emit cp.boot — the local row was upserted
    # and the charger is operational against cached Authorize results.
    producer.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_backend_unavailable_reject_fallback(
    fake_cp: Any,
    monkeypatch: pytest.MonkeyPatch,
    settings_factory: Any,
) -> None:
    from eveys_ocpp.platform import BackendUnavailableError

    fake_cp.settings = settings_factory(backend_register_fallback="reject")
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())
    producer = AsyncMock()
    producer.publish = AsyncMock()
    fake_cp.event_producer = producer
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.register_charge_point = AsyncMock(
        side_effect=BackendUnavailableError("network", error_code="NETWORK"),
    )

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.status == "Rejected"
    producer.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_backend_business_error_returns_rejected(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.platform import BackendBusinessError

    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())
    producer = AsyncMock()
    producer.publish = AsyncMock()
    fake_cp.event_producer = producer
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.register_charge_point = AsyncMock(
        side_effect=BackendBusinessError("unknown_cp_id", error_code="UNKNOWN_CP_ID"),
    )

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.status == "Rejected"
    producer.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_replay_does_not_call_backend(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Replay-cache hit must short-circuit before the backend round-trip
    — preserves E2-11 idempotency semantics."""
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)
    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(return_value=True)  # replay
    fake_cp.idempotency = fake_idem
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.register_charge_point = AsyncMock()

    result = await boot_notification.handle(
        fake_cp, message_id="MSG-REPLAY", charge_point_vendor="ACME"
    )

    assert result.status == "Accepted"
    fake_cp.backend_client.register_charge_point.assert_not_awaited()
    upsert.assert_not_awaited()


# ---------------------------------------------------------------------------
# MeterValueSampleInterval push (eveys-mobility/OCPP#238)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_meter_value_sample_interval_pushed_after_accepted_boot(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After BootNotification.Accepted, the handler schedules a
    deferred ChangeConfiguration push. Sleep is patched to 0 so the
    task runs synchronously inside the test."""
    from eveys_ocpp.handlers.v16 import boot_notification

    upsert = AsyncMock(return_value=None)
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)
    # Patch the inter-step sleep so the deferred task fires immediately.
    monkeypatch.setattr(boot_notification, "_POST_BOOT_PUSH_DELAY_SECONDS", 0)
    monkeypatch.setattr(boot_notification, "_POST_BOOT_INTER_CALL_GAP_SECONDS", 0)
    fake_cp.call = AsyncMock(return_value=MagicMock(status="Accepted"))
    # Default is 15 — fake_cp.settings ships with it. If a test needs
    # a different value, swap the whole `cp.settings` for a new
    # `Settings(meter_value_sample_interval_seconds=…)` instance
    # (Settings is frozen).

    await boot_notification.handle(fake_cp, charge_point_vendor="ACME")
    # Let the deferred task run. Read from __dict__ so a MagicMock
    # auto-attr can't paper over a missing assignment.
    task = fake_cp.__dict__.get("_meter_value_push_task")
    assert task is not None, "the deferred task should be attached to cp"
    await task

    # Post-boot push sends a burst of ChangeConfiguration calls (the
    # full OCPP config matrix, see `_post_boot_keys`). The first one is
    # still MeterValueSampleInterval=15 — keep that assertion as the
    # backwards-compatible smoke; verify the rest by gathering the
    # `(key, value)` pairs from every awaited call.
    assert fake_cp.call.await_count >= 1
    first_request = fake_cp.call.await_args_list[0].args[0]
    assert type(first_request).__name__ == "ChangeConfiguration"
    assert first_request.key == "MeterValueSampleInterval"
    assert first_request.value == "15"
    pushed_keys = {c.args[0].key for c in fake_cp.call.await_args_list}
    assert "HeartbeatInterval" in pushed_keys
    # Measurand-list keys are NOT pushed by the post-boot burst —
    # they're charger-type-specific and BootNotification doesn't carry
    # a reliable AC/DC signal. Operators send them per-CP via the
    # ChangeConfiguration command endpoint.
    assert "MeterValuesSampledData" not in pushed_keys
    assert "StopTxnAlignedData" not in pushed_keys


@pytest.mark.asyncio
async def test_meter_value_sample_interval_not_pushed_on_rejected_boot(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No push if the boot was Rejected — a charger we just turned
    away shouldn't receive any commands."""
    from eveys_ocpp.handlers.v16 import boot_notification

    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock(return_value=None))
    monkeypatch.setattr(boot_notification, "_POST_BOOT_PUSH_DELAY_SECONDS", 0)
    monkeypatch.setattr(boot_notification, "_POST_BOOT_INTER_CALL_GAP_SECONDS", 0)
    # Backend rejects → handler maps to RegistrationStatus.rejected.
    fake_cp.backend_client = MagicMock()
    fake_cp.backend_client.register_charge_point = AsyncMock(
        side_effect=boot_notification.BackendBusinessError(
            error_code="cp_disabled", message="charger is disabled"
        )
    )
    fake_cp.call = AsyncMock()

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.status == "Rejected"
    # MagicMock auto-creates attributes on access — check the real
    # __dict__ instead. The handler attaches the task to `cp` only
    # on Accepted; a Rejected boot must NOT have touched it.
    assert "_meter_value_push_task" not in fake_cp.__dict__
    fake_cp.call.assert_not_awaited()


@pytest.mark.asyncio
async def test_meter_value_sample_interval_push_failure_does_not_propagate(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A charger that disconnects (or refuses the key) must not crash
    the background task or leak the exception."""
    from eveys_ocpp.handlers.v16 import boot_notification

    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock(return_value=None))
    monkeypatch.setattr(boot_notification, "_POST_BOOT_PUSH_DELAY_SECONDS", 0)
    monkeypatch.setattr(boot_notification, "_POST_BOOT_INTER_CALL_GAP_SECONDS", 0)
    fake_cp.call = AsyncMock(side_effect=RuntimeError("WS closed"))

    await boot_notification.handle(fake_cp, charge_point_vendor="ACME")
    task = fake_cp.__dict__.get("_meter_value_push_task")
    assert task is not None
    # The task should finish without raising — log + drop is the contract.
    await task
    assert task.done()
    assert task.exception() is None


# ---- Pending-authorization gate ------------------------------------------


@pytest.mark.asyncio
async def test_pending_boot_records_metadata_and_skips_postgres(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When `is_pending=True`, BootNotification merges metadata into the
    Redis pending row and returns Accepted — no Postgres upsert, no
    `cp.boot` Kafka emit, no post-boot ChangeConfiguration push."""
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    fake_cp.is_pending = True
    fake_cp.pending_store = AsyncMock()
    fake_cp.pending_store.record_boot = AsyncMock(return_value={"cp_id": "TEST_CP_001"})
    fake_cp.event_producer = AsyncMock()
    fake_cp.event_producer.publish = AsyncMock()

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
    fake_cp.pending_store.record_boot.assert_awaited_once()
    kwargs = fake_cp.pending_store.record_boot.await_args.kwargs
    assert kwargs["cp_id"] == "TEST_CP_001"
    assert kwargs["vendor"] == "ACME"
    assert kwargs["model"] == "X1"
    assert kwargs["firmware"] == "1.0.0"
    assert kwargs["serial_number"] == "SN001"
    # No Postgres write, no Kafka emit, no post-boot push task attached.
    upsert.assert_not_awaited()
    fake_cp.event_producer.publish.assert_not_awaited()
    assert "_meter_value_push_task" not in fake_cp.__dict__


@pytest.mark.asyncio
async def test_pending_boot_closes_ws_when_row_expired(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the Redis pending row TTL expired between the WS upgrade and
    this Boot, `record_boot` returns None. The handler must close the
    WS with 1008 and raise SecurityError so the CALLERROR reaches the
    charger before the socket dies."""
    from ocpp.exceptions import SecurityError

    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    fake_cp.is_pending = True
    fake_cp.pending_store = AsyncMock()
    fake_cp.pending_store.record_boot = AsyncMock(return_value=None)
    fake_cp._connection = MagicMock()
    fake_cp._connection.close = AsyncMock()

    with pytest.raises(SecurityError):
        await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    fake_cp._connection.close.assert_awaited_once_with(1008, "authorization pending expired")
    upsert.assert_not_awaited()
