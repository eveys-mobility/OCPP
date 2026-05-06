"""Tests for the transactions read endpoints."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest


def _tx_row(transaction_id: int = 999, *, stopped: bool = False) -> dict[str, Any]:
    return {
        "id": transaction_id,
        "transaction_id": transaction_id,
        "charge_point_id": 1,
        "connector_id": 1,
        "id_tag": "RFID_001",
        "meter_start_wh": 1_000_000,
        "meter_stop_wh": 1_500_000 if stopped else None,
        "consumed_wh": 500_000 if stopped else None,
        "started_reported_at": datetime(2026, 5, 5, 14, 0, tzinfo=UTC),
        "started_received_at": datetime(2026, 5, 5, 14, 0, 1, tzinfo=UTC),
        "stopped_reported_at": datetime(2026, 5, 5, 15, 0, tzinfo=UTC) if stopped else None,
        "stopped_received_at": datetime(2026, 5, 5, 15, 0, 1, tzinfo=UTC) if stopped else None,
        "stop_reason": "Local" if stopped else None,
    }


@pytest.mark.asyncio
async def test_get_by_id_returns_404_when_unknown(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(tx_module, "get_transaction_by_id", AsyncMock(return_value=None))

    response = await client.get("/api/v1/transactions/12345")

    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_TRANSACTION_ID"


@pytest.mark.asyncio
async def test_get_by_id_returns_full_shape(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(
        tx_module,
        "get_transaction_by_id",
        AsyncMock(return_value=_tx_row(stopped=True)),
    )

    response = await client.get("/api/v1/transactions/999")

    body = response.json()
    assert body["transaction_id"] == 999
    assert body["consumed_wh"] == 500_000
    assert isinstance(body["started_reported_at"], str)
    assert body["stop_reason"] == "Local"


@pytest.mark.asyncio
async def test_list_by_cp_returns_404_for_unknown_cp(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`list_transactions_by_cp` returns None to signal unknown cp_id."""
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(tx_module, "list_transactions_by_cp", AsyncMock(return_value=None))

    response = await client.get("/api/v1/charge-points/UNKNOWN/transactions")

    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_list_by_cp_paginates_and_emits_cursor(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    rows = [_tx_row(transaction_id=i) for i in range(1, 4)]  # 3 rows
    monkeypatch.setattr(tx_module, "list_transactions_by_cp", AsyncMock(return_value=rows))

    response = await client.get("/api/v1/charge-points/CP_001/transactions?limit=2")

    body = response.json()
    assert len(body["transactions"]) == 2
    assert body["next_cursor"]
    # The route fills cp_id (the repo dict doesn't carry it).
    assert body["transactions"][0]["cp_id"] == "CP_001"


@pytest.mark.asyncio
async def test_list_by_cp_passes_filters_through(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    spy = AsyncMock(return_value=[])
    monkeypatch.setattr(tx_module, "list_transactions_by_cp", spy)

    response = await client.get(
        "/api/v1/charge-points/CP_001/transactions"
        "?id_tag=RFID_X&open=true&from=2026-05-01T00:00:00%2B00:00&to=2026-05-31T00:00:00%2B00:00"
    )

    assert response.status_code == 200
    kwargs = spy.await_args.kwargs
    assert kwargs["id_tag"] == "RFID_X"
    assert kwargs["open_only"] is True
    assert kwargs["started_from"] is not None
    assert kwargs["started_to"] is not None


@pytest.mark.asyncio
async def test_list_by_cp_rejects_unparseable_time(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/charge-points/CP_001/transactions?from=not-iso-8601")

    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"
