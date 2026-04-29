"""Unit tests for the StartTransaction handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16.enums import AuthorizationStatus

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
