"""Unit tests for the StartTransaction handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16.enums import AuthorizationStatus

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.handlers.v16 import start_transaction


@pytest.mark.asyncio
async def test_assigns_transaction_id(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(start_transaction, "get_charge_point_pk", AsyncMock(return_value=42))
    monkeypatch.setattr(start_transaction, "insert_transaction", AsyncMock(return_value=999))

    result = await start_transaction.handle(
        fake_cp,
        connector_id=1,
        id_tag="VALID_RFID_001",
        meter_start=0,
        timestamp="2026-04-29T00:00:00+00:00",
    )

    assert result.transaction_id == 999
    assert result.id_tag_info.status == AuthorizationStatus.accepted


@pytest.mark.asyncio
async def test_rejects_invalid_tag_without_db_write(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    insert = AsyncMock()
    monkeypatch.setattr(start_transaction, "get_charge_point_pk", AsyncMock(return_value=42))
    monkeypatch.setattr(start_transaction, "insert_transaction", insert)

    result = await start_transaction.handle(
        fake_cp,
        connector_id=1,
        id_tag="INVALID_TAG",
        meter_start=0,
        timestamp="2026-04-29T00:00:00+00:00",
    )

    assert result.transaction_id == 0
    assert result.id_tag_info.status == AuthorizationStatus.invalid
    insert.assert_not_called()


@pytest.mark.asyncio
async def test_unknown_charger_is_rejected_concurrent_tx(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(start_transaction, "get_charge_point_pk", AsyncMock(return_value=None))
    insert = AsyncMock()
    monkeypatch.setattr(start_transaction, "insert_transaction", insert)

    result = await start_transaction.handle(
        fake_cp,
        connector_id=1,
        id_tag="VALID_RFID_001",
        meter_start=0,
        timestamp="2026-04-29T00:00:00+00:00",
    )

    assert result.transaction_id == 0
    assert result.id_tag_info.status == AuthorizationStatus.concurrent_tx
    insert.assert_not_called()


# ---- E2-8 Kafka emit -------------------------------------------------------


@pytest.mark.asyncio
async def test_publishes_to_tx_started_topic_when_producer_attached(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(start_transaction, "get_charge_point_pk", AsyncMock(return_value=42))
    monkeypatch.setattr(start_transaction, "insert_transaction", AsyncMock(return_value=999))
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await start_transaction.handle(
        fake_cp,
        connector_id=2,
        id_tag="VALID_RFID_001",
        meter_start=12345,
        timestamp="2026-04-29T00:00:00+00:00",
    )

    fake_producer.publish.assert_awaited_once()
    call_kwargs = fake_producer.publish.await_args.kwargs
    assert call_kwargs["topic"] == fake_cp.settings.kafka_topic_tx_started
    assert call_kwargs["key"] == "TEST_CP_001"

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(call_kwargs["value"])
    assert envelope.cp_id == "TEST_CP_001"
    assert envelope.HasField("tx_started")
    assert envelope.tx_started.transaction_id == 999
    assert envelope.tx_started.connector_id == 2
    assert envelope.tx_started.id_tag == "VALID_RFID_001"
    assert envelope.tx_started.meter_start_wh == 12345
    assert envelope.tx_started.charger_reported_at == "2026-04-29T00:00:00+00:00"


@pytest.mark.asyncio
async def test_no_publish_on_invalid_tag(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """INVALID id_tag returns early — no transaction recorded → no event."""
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await start_transaction.handle(
        fake_cp,
        connector_id=1,
        id_tag="INVALID_TAG",
        meter_start=0,
        timestamp="2026-04-29T00:00:00+00:00",
    )

    fake_producer.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_publish_on_unknown_charger(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Unknown charger → no transaction → no event."""
    monkeypatch.setattr(start_transaction, "get_charge_point_pk", AsyncMock(return_value=None))
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await start_transaction.handle(
        fake_cp,
        connector_id=1,
        id_tag="VALID_RFID_001",
        meter_start=0,
        timestamp="2026-04-29T00:00:00+00:00",
    )

    fake_producer.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_publish_when_producer_is_none(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(start_transaction, "get_charge_point_pk", AsyncMock(return_value=42))
    monkeypatch.setattr(start_transaction, "insert_transaction", AsyncMock(return_value=999))
    fake_cp.event_producer = None

    result = await start_transaction.handle(
        fake_cp,
        connector_id=1,
        id_tag="VALID_RFID_001",
        meter_start=0,
        timestamp="2026-04-29T00:00:00+00:00",
    )

    assert result.transaction_id == 999


@pytest.mark.asyncio
async def test_handler_survives_kafka_publish_exception(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Broker drop must not crash the OCPP handler. The transaction is
    already committed in Postgres; the OCPP `Accepted` reply must still
    flow even if the downstream tx.started emit fails."""
    monkeypatch.setattr(start_transaction, "get_charge_point_pk", AsyncMock(return_value=42))
    monkeypatch.setattr(start_transaction, "insert_transaction", AsyncMock(return_value=999))
    fake_producer = AsyncMock()
    fake_producer.publish = AsyncMock(side_effect=RuntimeError("broker is down"))
    fake_cp.event_producer = fake_producer

    result = await start_transaction.handle(
        fake_cp,
        connector_id=1,
        id_tag="VALID_RFID_001",
        meter_start=0,
        timestamp="2026-04-29T00:00:00+00:00",
    )

    assert result.transaction_id == 999
    assert result.id_tag_info.status == AuthorizationStatus.accepted
    fake_producer.publish.assert_awaited_once()


# ---- E3-5: backend `/sessions/open` wiring --------------------------------


def _session_open_result(status: str = "Accepted", transaction_id: int = 999) -> Any:
    from eveys_ocpp.platform import IdTagInfo as PlatformIdTagInfo
    from eveys_ocpp.platform import SessionOpenResult

    return SessionOpenResult(
        transaction_id=transaction_id,
        id_tag_info=PlatformIdTagInfo(status=status, parent_id_tag=None, expiry_date=None),
        request_id="req-xyz",
        command_id=8842,
    )


@pytest.mark.asyncio
async def test_calls_backend_open_session_with_assigned_transaction_id(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The DB-assigned transaction_id flows into the backend call and
    becomes the Idempotency-Key per the contract."""
    monkeypatch.setattr(start_transaction, "get_charge_point_pk", AsyncMock(return_value=42))
    monkeypatch.setattr(start_transaction, "insert_transaction", AsyncMock(return_value=999))
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.open_session = AsyncMock(return_value=_session_open_result())

    await start_transaction.handle(
        fake_cp,
        connector_id=1,
        id_tag="VALID_RFID_001",
        meter_start=4500000,
        timestamp="2026-04-29T00:00:00+00:00",
    )

    fake_cp.backend_client.open_session.assert_awaited_once()
    kwargs = fake_cp.backend_client.open_session.await_args.kwargs
    assert kwargs["transaction_id"] == 999
    assert kwargs["cp_id"] == "TEST_CP_001"
    assert kwargs["connector_id"] == 1
    assert kwargs["id_tag"] == "VALID_RFID_001"
    assert kwargs["meter_start_wh"] == 4500000
    assert kwargs["started_reported_at"] == "2026-04-29T00:00:00+00:00"
    # Per docs/integration/01-backend-rest-contract.md.
    assert kwargs["idempotency_key"] == "ocpp-session-open-999"


@pytest.mark.asyncio
async def test_business_error_returns_invalid_but_keeps_row_and_emits(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 4xx from the backend (e.g. quota exceeded between Authorize and
    StartTransaction) maps to OCPP `Invalid`, but the local row is the
    audit-of-record and Kafka still emits."""
    from eveys_ocpp.platform import BackendBusinessError

    monkeypatch.setattr(start_transaction, "get_charge_point_pk", AsyncMock(return_value=42))
    insert = AsyncMock(return_value=999)
    monkeypatch.setattr(start_transaction, "insert_transaction", insert)
    producer = AsyncMock()
    producer.publish = AsyncMock()
    fake_cp.event_producer = producer
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.open_session = AsyncMock(
        side_effect=BackendBusinessError("quota exceeded", error_code="QUOTA_EXCEEDED"),
    )

    result = await start_transaction.handle(
        fake_cp,
        connector_id=1,
        id_tag="VALID_RFID_001",
        meter_start=0,
        timestamp="2026-04-29T00:00:00+00:00",
    )

    assert result.transaction_id == 999  # row kept
    assert result.id_tag_info.status == AuthorizationStatus.invalid
    insert.assert_awaited_once()
    producer.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_keeps_row_and_accepts_when_backend_unavailable(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Backend network outage: the local row anchors reconciliation,
    the charger continues to deliver energy, the reconciler heals."""
    from eveys_ocpp.platform import BackendUnavailableError

    monkeypatch.setattr(start_transaction, "get_charge_point_pk", AsyncMock(return_value=42))
    insert = AsyncMock(return_value=999)
    monkeypatch.setattr(start_transaction, "insert_transaction", insert)
    producer = AsyncMock()
    producer.publish = AsyncMock()
    fake_cp.event_producer = producer
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.open_session = AsyncMock(
        side_effect=BackendUnavailableError("network", error_code="NETWORK"),
    )

    result = await start_transaction.handle(
        fake_cp,
        connector_id=1,
        id_tag="VALID_RFID_001",
        meter_start=0,
        timestamp="2026-04-29T00:00:00+00:00",
    )

    assert result.transaction_id == 999
    assert result.id_tag_info.status == AuthorizationStatus.accepted
    producer.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_invalid_id_tag_does_not_call_backend(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The `INVALID*` early-reject branch must short-circuit before the
    DB write AND before the backend call."""
    insert = AsyncMock()
    monkeypatch.setattr(start_transaction, "insert_transaction", insert)
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.open_session = AsyncMock()

    result = await start_transaction.handle(
        fake_cp,
        connector_id=1,
        id_tag="INVALID_RFID",
        meter_start=0,
        timestamp="2026-04-29T00:00:00+00:00",
    )

    assert result.id_tag_info.status == AuthorizationStatus.invalid
    fake_cp.backend_client.open_session.assert_not_awaited()
    insert.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_cp_raises_security_error(fake_cp: Any) -> None:
    """A pending device must be refused with a CALLERROR and never
    touch Postgres or the backend."""
    from unittest.mock import MagicMock

    from ocpp.exceptions import SecurityError

    fake_cp.is_pending = True
    fake_cp.session_factory = MagicMock(
        side_effect=AssertionError("session_factory must not be used while pending")
    )
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.open_session = AsyncMock(
        side_effect=AssertionError("backend must not be called while pending")
    )

    with pytest.raises(SecurityError):
        await start_transaction.handle(
            fake_cp,
            connector_id=1,
            id_tag="RFID_X",
            meter_start=0,
            timestamp="2026-04-29T00:00:00+00:00",
        )

    fake_cp.backend_client.open_session.assert_not_awaited()
