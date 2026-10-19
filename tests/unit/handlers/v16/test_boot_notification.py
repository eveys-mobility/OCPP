"""Unit tests for the BootNotification handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from ocpp.v16 import call as call_
from ocpp.v16 import call_result

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.handlers.v16 import boot_notification

_PLANNED = "boot_notification.change_configuration.planned"


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

    # Post-boot push reads the wanted keys first; this mock's reply has
    # no readable configuration, so every key is pushed as before (the
    # full OCPP config matrix, see `_post_boot_keys`). The first push is
    # still MeterValueSampleInterval=15 — keep that assertion as the
    # backwards-compatible smoke; verify the rest by gathering the
    # `(key, value)` pairs from every ChangeConfiguration.
    requests = [c.args[0] for c in fake_cp.call.await_args_list]
    assert type(requests[0]).__name__ == "GetConfiguration"
    changes = [r for r in requests if type(r).__name__ == "ChangeConfiguration"]
    assert len(changes) == len(requests) - 1
    first_request = changes[0]
    assert first_request.key == "MeterValueSampleInterval"
    assert first_request.value == "15"
    pushed = {r.key: r.value for r in changes}
    assert "HeartbeatInterval" in pushed
    # Measurand-list keys are NOT pushed by the post-boot burst —
    # they're charger-type-specific and BootNotification doesn't carry
    # a reliable AC/DC signal. Operators send them per-CP via the
    # ChangeConfiguration command endpoint.
    assert "MeterValuesSampledData" not in pushed
    assert "StopTxnAlignedData" not in pushed
    # ISO 15118 PnC keys are pushed as strings on the wire; bools are
    # lowercased per the OCA/ISO 15118 convention that chargers accept.
    assert pushed["ISO15118PnCEnabled"] == "false"
    assert pushed["PlugandChargeMode"] == "0"
    assert pushed["ContractValidationOffline"] == "true"
    # Unplugging at the car ends the transaction on the charger, which
    # then delivers StopTransaction from its own queue.
    assert pushed["StopTransactionOnEVSideDisconnect"] == "true"
    # An idTag the CSMS answers Invalid stops the charge, and an offline
    # charger does not start for a tag it cannot check.
    assert pushed["StopTransactionOnInvalidId"] == "true"
    assert pushed["AllowOfflineTxForUnknownId"] == "false"


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


# ---------------------------------------------------------------------------
# Post-boot push kill switch + per-key trim
# ---------------------------------------------------------------------------
#
# The push replays on EVERY reconnect, so for a charger that is already
# flapping it is a burst of sequential CALLs every couple of minutes.
# These knobs make "is the push itself what this firmware can't survive?"
# a runtime experiment instead of a deploy.


@pytest.mark.asyncio
async def test_post_boot_push_disabled_sends_nothing(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The master switch suppresses the whole burst.

    The charger keeps whatever configuration it already had; the boot
    itself is still Accepted.
    """
    from eveys_ocpp.settings import Settings

    upsert = AsyncMock(return_value=None)
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)
    monkeypatch.setattr(boot_notification, "_POST_BOOT_PUSH_DELAY_SECONDS", 0)
    monkeypatch.setattr(boot_notification, "_POST_BOOT_INTER_CALL_GAP_SECONDS", 0)
    fake_cp.call = AsyncMock(return_value=MagicMock(status="Accepted"))
    fake_cp.settings = Settings(ocpp_cfg_post_boot_push_enabled=False)

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    task = fake_cp.__dict__.get("_meter_value_push_task")
    assert task is not None
    await task

    assert fake_cp.call.await_count == 0, "kill switch must suppress every key"
    assert result.status == "Accepted", "the boot itself is unaffected"


@pytest.mark.asyncio
async def test_post_boot_push_skips_listed_keys(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Named keys are dropped; everything else still converges.

    The motivating case is a firmware that answers `NotSupported` to
    the ISO 15118 keys — they cost a round trip on every reconnect and
    change nothing.
    """
    from eveys_ocpp.settings import Settings

    upsert = AsyncMock(return_value=None)
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)
    monkeypatch.setattr(boot_notification, "_POST_BOOT_PUSH_DELAY_SECONDS", 0)
    monkeypatch.setattr(boot_notification, "_POST_BOOT_INTER_CALL_GAP_SECONDS", 0)
    fake_cp.call = AsyncMock(return_value=MagicMock(status="Accepted"))
    fake_cp.settings = Settings(
        # Mixed case on purpose — matching is case-insensitive.
        ocpp_cfg_post_boot_push_skip_keys=(
            "ISO15118PnCEnabled, plugandchargemode ,ContractValidationOffline"
        )
    )

    await boot_notification.handle(fake_cp, charge_point_vendor="ACME")
    await fake_cp.__dict__["_meter_value_push_task"]

    pushed = {
        c.args[0].key
        for c in fake_cp.call.await_args_list
        if type(c.args[0]).__name__ == "ChangeConfiguration"
    }
    assert "ISO15118PnCEnabled" not in pushed
    assert "PlugandChargeMode" not in pushed
    assert "ContractValidationOffline" not in pushed
    # The rest of the matrix is untouched.
    assert "MeterValueSampleInterval" in pushed
    assert "HeartbeatInterval" in pushed


@pytest.mark.asyncio
async def test_post_boot_push_ignores_unknown_skip_keys(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A typo in the skip list must not silently drop a real key."""
    from eveys_ocpp.settings import Settings

    upsert = AsyncMock(return_value=None)
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)
    monkeypatch.setattr(boot_notification, "_POST_BOOT_PUSH_DELAY_SECONDS", 0)
    monkeypatch.setattr(boot_notification, "_POST_BOOT_INTER_CALL_GAP_SECONDS", 0)
    fake_cp.call = AsyncMock(return_value=MagicMock(status="Accepted"))
    fake_cp.settings = Settings(ocpp_cfg_post_boot_push_skip_keys="NotARealKey,,  ")

    await boot_notification.handle(fake_cp, charge_point_vendor="ACME")
    await fake_cp.__dict__["_meter_value_push_task"]

    pushed = {
        c.args[0].key
        for c in fake_cp.call.await_args_list
        if type(c.args[0]).__name__ == "ChangeConfiguration"
    }
    assert "MeterValueSampleInterval" in pushed
    assert "HeartbeatInterval" in pushed
    assert "ISO15118PnCEnabled" in pushed


# ---------------------------------------------------------------------------
# Post-boot push reads the charger's values first
# ---------------------------------------------------------------------------
#
# Some chargers boot hundreds of times a day. Writing every key on every
# boot cost thousands of ChangeConfigurations for nothing; one
# GetConfiguration for the wanted keys is enough when nothing changed.


def _wanted(fake_cp: Any) -> dict[str, str]:
    return dict(boot_notification._post_boot_keys(fake_cp))


def _charger(
    values: dict[str, str] | None = None,
    *,
    readonly: tuple[str, ...] = (),
    unknown: tuple[str, ...] = (),
    get_error: BaseException | None = None,
    get_reply: Any = "configuration",
) -> AsyncMock:
    """Fake `cp.call`: answers GetConfiguration from `values` and accepts
    every ChangeConfiguration."""

    async def call(request: Any) -> Any:
        if isinstance(request, call_.GetConfiguration):
            if get_error is not None:
                raise get_error
            if get_reply != "configuration":
                return get_reply
            return call_result.GetConfiguration(
                configuration_key=[
                    {"key": key, "readonly": key in readonly, "value": value}
                    for key, value in (values or {}).items()
                ],
                unknown_key=list(unknown),
            )
        return call_result.ChangeConfiguration(status="Accepted")

    return AsyncMock(side_effect=call)


async def _boot_and_push(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock(return_value=None))
    monkeypatch.setattr(boot_notification, "_POST_BOOT_PUSH_DELAY_SECONDS", 0)
    monkeypatch.setattr(boot_notification, "_POST_BOOT_INTER_CALL_GAP_SECONDS", 0)
    await boot_notification.handle(fake_cp, charge_point_vendor="ACME")
    await fake_cp.__dict__["_meter_value_push_task"]
    return [c.args[0] for c in fake_cp.call.await_args_list]


def _changes(requests: list[Any]) -> dict[str, str]:
    return {r.key: r.value for r in requests if isinstance(r, call_.ChangeConfiguration)}


@pytest.mark.asyncio
async def test_post_boot_push_reads_the_wanted_keys_and_writes_nothing_when_they_match(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    wanted = _wanted(fake_cp)
    # Chargers report booleans and numbers in their own spelling.
    reported = {key: f" {value.upper()} " for key, value in wanted.items()}
    fake_cp.call = _charger(reported)
    log = MagicMock()
    monkeypatch.setattr(boot_notification, "log", log)

    requests = await _boot_and_push(fake_cp, monkeypatch)

    [read] = requests
    assert isinstance(read, call_.GetConfiguration)
    assert read.key == list(wanted)
    planned = [c for c in log.info.call_args_list if c.args == (_PLANNED,)]
    assert [c.kwargs["read"] for c in planned] == [len(wanted)]
    assert planned[0].kwargs["differing"] == 0


@pytest.mark.asyncio
async def test_post_boot_push_writes_only_the_differing_key(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    wanted = _wanted(fake_cp)
    fake_cp.call = _charger({**wanted, "HeartbeatInterval": "3600"})

    requests = await _boot_and_push(fake_cp, monkeypatch)

    assert _changes(requests) == {"HeartbeatInterval": wanted["HeartbeatInterval"]}


@pytest.mark.asyncio
async def test_post_boot_push_skips_unknown_and_readonly_keys(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    wanted = _wanted(fake_cp)
    values = {key: "different" for key in wanted if key != "PlugandChargeMode"}
    fake_cp.call = _charger(values, readonly=("HeartbeatInterval",), unknown=("PlugandChargeMode",))
    log = MagicMock()
    monkeypatch.setattr(boot_notification, "log", log)

    requests = await _boot_and_push(fake_cp, monkeypatch)

    pushed = _changes(requests)
    assert "PlugandChargeMode" not in pushed
    assert "HeartbeatInterval" not in pushed
    assert set(pushed) == set(wanted) - {"PlugandChargeMode", "HeartbeatInterval"}
    [planned] = [c for c in log.info.call_args_list if c.args == (_PLANNED,)]
    assert planned.kwargs["skipped_unknown"] == 1
    assert planned.kwargs["skipped_readonly"] == 1


@pytest.mark.asyncio
async def test_post_boot_push_writes_a_key_the_reply_left_out(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A charger with a low GetConfigurationMaxKeys may answer only part
    of the list; whatever is not reported is written as before."""
    wanted = _wanted(fake_cp)
    reported = {key: value for key, value in wanted.items() if key != "ConnectionTimeOut"}
    fake_cp.call = _charger(reported)

    requests = await _boot_and_push(fake_cp, monkeypatch)

    assert _changes(requests) == {"ConnectionTimeOut": wanted["ConnectionTimeOut"]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "charger",
    [
        {"get_error": TimeoutError("no reply")},
        {"get_error": RuntimeError("WS closed")},
        # The library returns None for a CALLERROR (call(..., suppress=True)).
        {"get_reply": None},
    ],
)
async def test_post_boot_push_writes_every_key_when_the_read_fails(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch, charger: dict[str, Any]
) -> None:
    wanted = _wanted(fake_cp)
    fake_cp.call = _charger(**charger)

    requests = await _boot_and_push(fake_cp, monkeypatch)

    assert isinstance(requests[0], call_.GetConfiguration)
    assert _changes(requests) == wanted
    assert [r.key for r in requests[1:]] == list(wanted)


@pytest.mark.asyncio
async def test_post_boot_push_skip_keys_are_neither_read_nor_written(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.settings import Settings

    fake_cp.settings = Settings(ocpp_cfg_post_boot_push_skip_keys="StopTransactionOnInvalidId")
    fake_cp.call = _charger({})

    requests = await _boot_and_push(fake_cp, monkeypatch)

    assert "StopTransactionOnInvalidId" not in requests[0].key
    assert "StopTransactionOnInvalidId" not in _changes(requests)
    assert "AllowOfflineTxForUnknownId" in _changes(requests)


@pytest.mark.asyncio
async def test_post_boot_push_disabled_does_not_read_either(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.settings import Settings

    fake_cp.settings = Settings(ocpp_cfg_post_boot_push_enabled=False)
    fake_cp.call = _charger({})

    assert await _boot_and_push(fake_cp, monkeypatch) == []


def test_invalid_id_keys_follow_env_and_runtime_overrides(fake_cp: Any) -> None:
    from eveys_ocpp import runtime_overrides
    from eveys_ocpp.settings import Settings

    assert _wanted(fake_cp)["StopTransactionOnInvalidId"] == "true"
    assert _wanted(fake_cp)["AllowOfflineTxForUnknownId"] == "false"

    fake_cp.settings = Settings(
        ocpp_cfg_stop_transaction_on_invalid_id=False,
        ocpp_cfg_allow_offline_tx_for_unknown_id=True,
    )
    assert _wanted(fake_cp)["StopTransactionOnInvalidId"] == "false"
    assert _wanted(fake_cp)["AllowOfflineTxForUnknownId"] == "true"

    try:
        runtime_overrides.set_override("ocpp_cfg_stop_transaction_on_invalid_id", "true")
        runtime_overrides.set_override("ocpp_cfg_allow_offline_tx_for_unknown_id", False)
        assert _wanted(fake_cp)["StopTransactionOnInvalidId"] == "true"
        assert _wanted(fake_cp)["AllowOfflineTxForUnknownId"] == "false"
    finally:
        runtime_overrides.clear_override("ocpp_cfg_stop_transaction_on_invalid_id")
        runtime_overrides.clear_override("ocpp_cfg_allow_offline_tx_for_unknown_id")
