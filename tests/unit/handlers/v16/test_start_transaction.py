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
