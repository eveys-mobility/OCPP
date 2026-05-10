"""Unit tests for the StopTransaction handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16.enums import AuthorizationStatus

from eveys_ocpp.handlers.v16 import stop_transaction


@pytest.mark.asyncio
async def test_applies_stop(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    stop = AsyncMock(return_value=0)
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)

    result = await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
        reason="Local",
        id_tag="VALID_RFID_001",
    )

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    stop.assert_awaited_once()
    kwargs = stop.await_args.kwargs
    assert kwargs["transaction_id"] == 999
    assert kwargs["meter_stop_wh"] == 12345
    # Idempotency key built from (cp_id, transaction_id, meter_stop)
    assert kwargs["idempotency_key"] == "TEST_CP_001:999:12345"


@pytest.mark.asyncio
async def test_replay_is_noop(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Same triple twice → second call sees `applied=False` from the repo."""
    stop = AsyncMock(return_value=None)
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)

    result = await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
        reason="Local",
    )

    # Still returns Accepted to the charger — the charger doesn't know or
    # care that we treated it as a replay; it just needs an OK reply.
    assert result.id_tag_info.status == AuthorizationStatus.accepted


@pytest.mark.asyncio
async def test_reject_invalid_id_tag(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(stop_transaction, "stop_transaction", AsyncMock(return_value=0))

    result = await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
        reason="Local",
        id_tag="INVALID_TAG",
    )

    assert result.id_tag_info.status == AuthorizationStatus.invalid


# ---- E2-11 idempotency -----------------------------------------------------


@pytest.mark.asyncio
async def test_replay_skips_db_write(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Cache hit → return Accepted without invoking the DB-layer stop."""
    stop = AsyncMock(return_value=0)
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)

    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(return_value=True)  # replay
    fake_cp.idempotency = fake_idem

    result = await stop_transaction.handle(
        fake_cp,
        message_id="MSG-RETRY-1",
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
    )

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    stop.assert_not_awaited()
    fake_idem.check_and_record.assert_awaited_once_with(
        cp_id="TEST_CP_001", message_id="MSG-RETRY-1"
    )


@pytest.mark.asyncio
async def test_first_sighting_runs_handler(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    stop = AsyncMock(return_value=0)
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)

    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(return_value=False)  # not a replay
    fake_cp.idempotency = fake_idem

    result = await stop_transaction.handle(
        fake_cp,
        message_id="MSG-FIRST",
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
    )

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_cache_outage_falls_through_to_db_dedup(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Redis outage → fall through to the DB-layer dedup. Defense in depth."""
    stop = AsyncMock(return_value=0)
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)

    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(side_effect=RuntimeError("redis down"))
    fake_cp.idempotency = fake_idem

    result = await stop_transaction.handle(
        fake_cp,
        message_id="MSG-Y",
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
    )

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_idempotency_cache_falls_through(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """idempotency=None → DB-layer dedup is the only line of defense."""
    stop = AsyncMock(return_value=0)
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)
    fake_cp.idempotency = None

    result = await stop_transaction.handle(
        fake_cp,
        message_id="MSG-Z",
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
    )

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    stop.assert_awaited_once()


# ---- E3-6: backend `/sessions/close` wiring -------------------------------


def _close_result(status: str = "Accepted", transaction_id: int = 999) -> Any:
    from eveys_ocpp.platform import IdTagInfo as PlatformIdTagInfo
    from eveys_ocpp.platform import SessionCloseResult

    return SessionCloseResult(
        transaction_id=transaction_id,
        id_tag_info=PlatformIdTagInfo(status=status, parent_id_tag=None, expiry_date=None),
        request_id="req-close-1",
        command_id=8842,
    )


@pytest.mark.asyncio
async def test_calls_backend_close_session_with_kwargs_and_idempotency_key(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """First-time stop with backend wired: full kwargs flow through and
    the Idempotency-Key matches the contract."""
    monkeypatch.setattr(stop_transaction, "stop_transaction", AsyncMock(return_value=0))
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.close_session = AsyncMock(return_value=_close_result())

    await stop_transaction.handle(
        fake_cp,
        message_id="MSG-CLOSE-1",
        transaction_id=999,
        meter_stop=4523500,
        timestamp="2026-05-05T15:14:30.012+00:00",
        reason="Local",
        id_tag="VALID_RFID",
    )

    fake_cp.backend_client.close_session.assert_awaited_once()
    kwargs = fake_cp.backend_client.close_session.await_args.kwargs
    assert kwargs["transaction_id"] == 999
    assert kwargs["cp_id"] == "TEST_CP_001"
    assert kwargs["id_tag"] == "VALID_RFID"
    assert kwargs["meter_stop_wh"] == 4523500
    assert kwargs["stopped_reported_at"] == "2026-05-05T15:14:30.012+00:00"
    assert kwargs["stop_reason"] == "Local"
    # Per docs/integration/01-backend-rest-contract.md.
    assert kwargs["idempotency_key"] == "ocpp-session-close-999-MSG-CLOSE-1"


@pytest.mark.asyncio
async def test_business_error_forwards_blocked_to_charger(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 4xx from the backend (e.g. billing fraud) maps to OCPP's
    enum via the same status map. The DB row stays — operations need
    a trace of the rejected stop."""
    from eveys_ocpp.platform import BackendBusinessError

    stop = AsyncMock(return_value=0)
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.close_session = AsyncMock(
        side_effect=BackendBusinessError("blocked by ops", error_code="SESSION_BLOCKED"),
    )

    result = await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
        id_tag="VALID_RFID",
    )

    # Default for an unmapped business error is Invalid; the row stays.
    assert result.id_tag_info.status == AuthorizationStatus.invalid
    stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_unavailable_keeps_state_and_accepts(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Backend network outage: the DB row remains stopped, the charger
    finalises cleanly, the reconciler heals later."""
    from eveys_ocpp.platform import BackendUnavailableError

    monkeypatch.setattr(stop_transaction, "stop_transaction", AsyncMock(return_value=0))
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.close_session = AsyncMock(
        side_effect=BackendUnavailableError("network", error_code="NETWORK"),
    )

    result = await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
        id_tag="VALID_RFID",
    )

    assert result.id_tag_info.status == AuthorizationStatus.accepted


@pytest.mark.asyncio
async def test_replay_cache_hit_skips_backend(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """E2-11 cache hit must short-circuit BEFORE the backend call —
    chargers retry aggressively and the first request already settled
    the session backend-side."""
    stop = AsyncMock()
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)
    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(return_value=True)  # replay
    fake_cp.idempotency = fake_idem
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.close_session = AsyncMock()

    result = await stop_transaction.handle(
        fake_cp,
        message_id="MSG-REPLAY",
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
        id_tag="VALID_RFID",
    )

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    stop.assert_not_awaited()
    fake_cp.backend_client.close_session.assert_not_awaited()


@pytest.mark.asyncio
async def test_db_dedup_applied_false_skips_backend(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the Postgres dedup catches a replay (cache TTL expired,
    cross-pod retry), don't double-bill the backend."""
    monkeypatch.setattr(stop_transaction, "stop_transaction", AsyncMock(return_value=None))
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.close_session = AsyncMock()

    result = await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
        id_tag="VALID_RFID",
    )

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    fake_cp.backend_client.close_session.assert_not_awaited()


@pytest.mark.asyncio
async def test_omitted_id_tag_passes_empty_string(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """OCPP allows omitting id_tag (e.g. EVDisconnected stop reason).
    The contract requires a string — pass empty so the backend can
    settle by transaction_id alone."""
    monkeypatch.setattr(stop_transaction, "stop_transaction", AsyncMock(return_value=0))
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.close_session = AsyncMock(return_value=_close_result())

    await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
        reason="EVDisconnected",
        id_tag=None,
    )

    kwargs = fake_cp.backend_client.close_session.await_args.kwargs
    assert kwargs["id_tag"] == ""
    assert kwargs["stop_reason"] == "EVDisconnected"


@pytest.mark.asyncio
async def test_forwards_backend_status_through_status_map(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Backend explicit `Blocked` (e.g. billing-fraud detected at close
    time) flows through to the OCPP response."""
    monkeypatch.setattr(stop_transaction, "stop_transaction", AsyncMock(return_value=0))
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.close_session = AsyncMock(return_value=_close_result(status="Blocked"))

    result = await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
        id_tag="VALID_RFID",
    )

    assert result.id_tag_info.status == AuthorizationStatus.blocked


# ----- SLO 4 counter discipline (#163) --------------------------------------


@pytest.mark.asyncio
async def test_received_counter_bumps_before_persist(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SLO 4 needs the denominator (`received`) to fire even when the
    DB write fails — that's exactly the case the SLO is designed to
    flag (received but not persisted == billing incident). Simulate a
    DB-layer exception and assert: `received` ticks up; `_total`
    (persisted) does not."""
    from eveys_ocpp.metrics import registry

    received_before = registry.STOP_TRANSACTIONS_RECEIVED_TOTAL._value.get()
    persisted_before = registry.STOP_TRANSACTIONS_TOTAL.labels(reason="Local")._value.get()

    monkeypatch.setattr(
        stop_transaction,
        "stop_transaction",
        AsyncMock(side_effect=RuntimeError("DB went away")),
    )

    with pytest.raises(RuntimeError):
        await stop_transaction.handle(
            fake_cp,
            transaction_id=999,
            meter_stop=12345,
            timestamp="2026-04-29T01:00:00+00:00",
            reason="Local",
            id_tag="VALID_RFID_001",
        )

    assert registry.STOP_TRANSACTIONS_RECEIVED_TOTAL._value.get() == received_before + 1
    assert registry.STOP_TRANSACTIONS_TOTAL.labels(reason="Local")._value.get() == persisted_before


@pytest.mark.asyncio
async def test_persisted_counter_bumps_only_on_successful_apply(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Happy path: both counters tick up. Replay path: only `received`
    bumps — the row was already in Postgres, so the persisted counter
    must NOT inflate (would skew SLO 4 numerator above 1.0 over time)."""
    from eveys_ocpp.metrics import registry

    received_before = registry.STOP_TRANSACTIONS_RECEIVED_TOTAL._value.get()
    persisted_before = registry.STOP_TRANSACTIONS_TOTAL.labels(reason="Other")._value.get()

    monkeypatch.setattr(stop_transaction, "stop_transaction", AsyncMock(return_value=0))
    await stop_transaction.handle(
        fake_cp,
        transaction_id=1,
        meter_stop=100,
        timestamp="2026-04-29T01:00:00+00:00",
        reason="Other",
    )

    assert registry.STOP_TRANSACTIONS_RECEIVED_TOTAL._value.get() == received_before + 1
    assert (
        registry.STOP_TRANSACTIONS_TOTAL.labels(reason="Other")._value.get() == persisted_before + 1
    )

    # Replay: same triple → applied=False, neither persisted nor received
    # double-counted (received bumps once per CALL; persisted not at all).
    monkeypatch.setattr(stop_transaction, "stop_transaction", AsyncMock(return_value=None))
    await stop_transaction.handle(
        fake_cp,
        transaction_id=1,
        meter_stop=100,
        timestamp="2026-04-29T01:00:00+00:00",
        reason="Other",
    )
    assert registry.STOP_TRANSACTIONS_RECEIVED_TOTAL._value.get() == received_before + 2
    assert (
        registry.STOP_TRANSACTIONS_TOTAL.labels(reason="Other")._value.get()
        == persisted_before + 1  # unchanged
    )


# ----- tx.stopped Kafka publish (#165) --------------------------------------


@pytest.mark.asyncio
async def test_publishes_tx_stopped_envelope_on_apply(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Successful StopTransaction publishes one TxStopped envelope to
    `tx.stopped`, with `consumed_wh` pre-computed from the
    `meter_start_wh` returned by the repo."""
    from eveys_ocpp._generated.events.v1 import events_pb2

    monkeypatch.setattr(stop_transaction, "stop_transaction", AsyncMock(return_value=4_500_000))
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=4_523_500,
        timestamp="2026-05-05T15:14:30.000+00:00",
        reason="Local",
        id_tag="VALID_RFID",
    )

    fake_producer.publish.assert_awaited_once()
    kwargs = fake_producer.publish.await_args.kwargs
    assert kwargs["topic"] == fake_cp.settings.kafka_topic_tx_stopped
    assert kwargs["key"] == "TEST_CP_001"

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(kwargs["value"])
    assert envelope.cp_id == "TEST_CP_001"
    assert envelope.WhichOneof("payload") == "tx_stopped"
    p = envelope.tx_stopped
    assert p.transaction_id == 999
    assert p.id_tag == "VALID_RFID"
    assert p.meter_stop_wh == 4_523_500
    assert p.consumed_wh == 23_500  # 4_523_500 - 4_500_000
    assert p.stop_reason == "Local"
    assert p.charger_reported_at == "2026-05-05T15:14:30.000+00:00"


@pytest.mark.asyncio
async def test_does_not_publish_tx_stopped_on_replay(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Replay (DB-layer dedup) returns before the publish — re-emitting
    would double-deliver to backends that subscribe."""
    monkeypatch.setattr(stop_transaction, "stop_transaction", AsyncMock(return_value=None))
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=4_523_500,
        timestamp="2026-05-05T15:14:30.000+00:00",
        reason="Local",
    )

    fake_producer.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_failure_does_not_crash_handler(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Best-effort publish: a broker drop must not break the OCPP
    response — the synchronous `/sessions/close` is the primary path,
    the webhook is the belt-and-braces."""
    monkeypatch.setattr(stop_transaction, "stop_transaction", AsyncMock(return_value=4_500_000))
    fake_producer = AsyncMock()
    fake_producer.publish = AsyncMock(side_effect=RuntimeError("kafka down"))
    fake_cp.event_producer = fake_producer

    result = await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=4_523_500,
        timestamp="2026-05-05T15:14:30.000+00:00",
        reason="Local",
        id_tag="VALID_RFID_001",
    )

    # Charger still gets a clean reply; broker failure logged but not raised.
    assert result.id_tag_info.status == AuthorizationStatus.accepted
