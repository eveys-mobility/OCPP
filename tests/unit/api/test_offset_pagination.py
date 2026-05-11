"""Tests for the standard offset pagination shape on list endpoints (#198).

The cursor-pagination behaviour stays exercised in each endpoint's
existing test module; this file is dedicated to the new `page` /
`page_size` / `total` / `total_pages` / `has_next` / `has_prev`
contract and the mutual-exclusion guard.

Pagination helpers (`pagination_block`, `offset_for_page`) are
unit-tested at the bottom; route-level behaviour at the top.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest


def _cp_row(cp_id: str = "CP_001", *, internal_id: int = 1) -> dict[str, Any]:
    return {
        "id": internal_id,
        "cp_id": cp_id,
        "vendor": "ACME",
        "model": "X1",
        "firmware_version": "1.0.0",
        "serial_number": f"SN-{internal_id}",
        "last_boot_at": datetime(2026, 5, 5, 14, 0, tzinfo=UTC),
        "last_heartbeat_at": datetime(2026, 5, 5, 15, 0, tzinfo=UTC),
        "last_status": "Available",
        "last_diagnostics_status": None,
        "last_firmware_status": "Installed",
    }


def _tx_row(
    *, internal_id: int = 1, transaction_id: int = 10001, cp_id: str = "CP_001"
) -> dict[str, Any]:
    return {
        "id": internal_id,
        "transaction_id": transaction_id,
        "cp_id": cp_id,
        "connector_id": 1,
        "id_tag": "USER_RFID",
        "meter_start_wh": 1000,
        "meter_stop_wh": 5000,
        "consumed_wh": 4000,
        "stop_reason": "Local",
        "started_reported_at": datetime(2026, 5, 5, 14, 0, tzinfo=UTC),
        "started_received_at": datetime(2026, 5, 5, 14, 0, tzinfo=UTC),
        "stopped_reported_at": datetime(2026, 5, 5, 15, 0, tzinfo=UTC),
        "stopped_received_at": datetime(2026, 5, 5, 15, 0, tzinfo=UTC),
    }


# ---- /charge-points offset pagination --------------------------------------


@pytest.mark.asyncio
async def test_charge_points_offset_returns_pagination_block(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import charge_points as cp_module

    monkeypatch.setattr(
        cp_module,
        "list_charge_points",
        AsyncMock(return_value=[_cp_row(internal_id=i) for i in range(1, 11)]),
    )
    monkeypatch.setattr(cp_module, "count_charge_points", AsyncMock(return_value=42))

    response = await client.get("/api/v1/charge-points?page=2&page_size=10")
    assert response.status_code == 200
    body = response.json()
    assert "next_cursor" not in body
    assert body["pagination"] == {
        "page": 2,
        "page_size": 10,
        "total": 42,
        "total_pages": 5,
        "has_next": True,
        "has_prev": True,
    }


@pytest.mark.asyncio
async def test_charge_points_offset_first_page_empty(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import charge_points as cp_module

    monkeypatch.setattr(cp_module, "list_charge_points", AsyncMock(return_value=[]))
    monkeypatch.setattr(cp_module, "count_charge_points", AsyncMock(return_value=0))

    response = await client.get("/api/v1/charge-points?page=1&page_size=10")
    body = response.json()
    assert body["pagination"] == {
        "page": 1,
        "page_size": 10,
        "total": 0,
        "total_pages": 0,
        "has_next": False,
        "has_prev": False,
    }


@pytest.mark.asyncio
async def test_charge_points_offset_last_page_has_no_next(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import charge_points as cp_module

    monkeypatch.setattr(
        cp_module,
        "list_charge_points",
        AsyncMock(return_value=[_cp_row(internal_id=i) for i in range(1, 3)]),
    )
    monkeypatch.setattr(cp_module, "count_charge_points", AsyncMock(return_value=22))

    response = await client.get("/api/v1/charge-points?page=3&page_size=10")
    body = response.json()
    assert body["pagination"]["has_next"] is False
    assert body["pagination"]["has_prev"] is True
    assert body["pagination"]["total_pages"] == 3


@pytest.mark.asyncio
async def test_charge_points_cursor_path_unchanged(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Backwards compat: no `page` query param → original cursor shape."""
    from eveys_ocpp.api import charge_points as cp_module

    monkeypatch.setattr(cp_module, "list_charge_points", AsyncMock(return_value=[]))

    response = await client.get("/api/v1/charge-points")
    body = response.json()
    assert "pagination" not in body
    assert body["next_cursor"] is None


@pytest.mark.asyncio
async def test_charge_points_rejects_mixed_pagination(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/charge-points?page=1&cursor=eyJpZCI6MX0=")
    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


# ---- /charge-points/{cp_id}/transactions offset pagination -----------------


@pytest.mark.asyncio
async def test_transactions_by_cp_offset_pagination(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(
        tx_module,
        "list_transactions_by_cp",
        AsyncMock(return_value=[_tx_row(internal_id=i) for i in range(1, 6)]),
    )
    monkeypatch.setattr(tx_module, "count_transactions_by_cp", AsyncMock(return_value=17))

    response = await client.get("/api/v1/charge-points/CP_001/transactions?page=2&page_size=5")
    assert response.status_code == 200
    body = response.json()
    assert "next_cursor" not in body
    assert body["pagination"] == {
        "page": 2,
        "page_size": 5,
        "total": 17,
        "total_pages": 4,
        "has_next": True,
        "has_prev": True,
    }


@pytest.mark.asyncio
async def test_transactions_by_cp_offset_unknown_cp_id(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(tx_module, "list_transactions_by_cp", AsyncMock(return_value=None))
    monkeypatch.setattr(tx_module, "count_transactions_by_cp", AsyncMock(return_value=None))

    response = await client.get("/api/v1/charge-points/UNKNOWN/transactions?page=1&page_size=10")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_transactions_by_cp_rejects_mixed_pagination(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get(
        "/api/v1/charge-points/CP_001/transactions?page=1&cursor=eyJpZCI6MX0="
    )
    assert response.status_code == 400


# ---- /transactions (global) offset pagination ------------------------------


@pytest.mark.asyncio
async def test_transactions_global_offset_pagination(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(
        tx_module,
        "list_transactions",
        AsyncMock(return_value=[_tx_row(internal_id=i) for i in range(1, 11)]),
    )
    monkeypatch.setattr(tx_module, "count_transactions", AsyncMock(return_value=100))

    response = await client.get("/api/v1/transactions?page=5&page_size=10")
    body = response.json()
    assert body["pagination"] == {
        "page": 5,
        "page_size": 10,
        "total": 100,
        "total_pages": 10,
        "has_next": True,
        "has_prev": True,
    }


@pytest.mark.asyncio
async def test_transactions_global_rejects_mixed_pagination(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/transactions?page=1&cursor=eyJpZCI6MX0=")
    assert response.status_code == 400


# ---- pagination helper unit tests ------------------------------------------


def test_pagination_block_empty() -> None:
    from eveys_ocpp.api._pagination import pagination_block

    assert pagination_block(page=1, page_size=10, total=0) == {
        "page": 1,
        "page_size": 10,
        "total": 0,
        "total_pages": 0,
        "has_next": False,
        "has_prev": False,
    }


def test_pagination_block_middle_page() -> None:
    from eveys_ocpp.api._pagination import pagination_block

    block = pagination_block(page=3, page_size=10, total=100)
    assert block["total_pages"] == 10
    assert block["has_prev"] is True
    assert block["has_next"] is True


def test_pagination_block_ceiling_division() -> None:
    """101 rows / 10 per page → 11 total pages (last one has 1 row)."""
    from eveys_ocpp.api._pagination import pagination_block

    block = pagination_block(page=11, page_size=10, total=101)
    assert block["total_pages"] == 11
    assert block["has_next"] is False
    assert block["has_prev"] is True


def test_offset_for_page_basic() -> None:
    from eveys_ocpp.api._pagination import offset_for_page

    assert offset_for_page(1, 10) == 0
    assert offset_for_page(2, 10) == 10
    assert offset_for_page(5, 25) == 100
