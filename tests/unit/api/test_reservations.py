"""Tests for the reservations read endpoint."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest


def _row(rid: int = 8842, *, status: str = "Active") -> dict[str, Any]:
    return {
        "id": rid,
        "reservation_id": rid,
        "connector_id": 1,
        "id_tag": "RFID_FAMILY",
        "parent_id_tag": None,
        "expiry_date": datetime(2026, 5, 6, 16, 0, tzinfo=UTC),
        "status": status,
        "created_at": datetime(2026, 5, 6, 14, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 5, 6, 14, 0, tzinfo=UTC),
    }


@pytest.mark.asyncio
async def test_list_returns_404_for_unknown_cp(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import reservations as r_module

    monkeypatch.setattr(r_module, "list_reservations_by_cp", AsyncMock(return_value=None))

    response = await client.get("/api/v1/charge-points/UNKNOWN/reservations")

    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_list_returns_full_shape(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import reservations as r_module

    monkeypatch.setattr(
        r_module,
        "list_reservations_by_cp",
        AsyncMock(return_value=[_row()]),
    )

    response = await client.get("/api/v1/charge-points/CP_001/reservations")

    body = response.json()
    assert response.status_code == 200
    res = body["reservations"][0]
    assert res["reservation_id"] == 8842
    assert res["status"] == "Active"
    assert res["cp_id"] == "CP_001"
    assert isinstance(res["expiry_date"], str)
    assert body["next_cursor"] is None


@pytest.mark.asyncio
async def test_list_paginates(client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    from eveys_ocpp.api import reservations as r_module

    rows = [_row(rid=i) for i in range(1, 4)]  # 3 rows
    monkeypatch.setattr(r_module, "list_reservations_by_cp", AsyncMock(return_value=rows))

    response = await client.get("/api/v1/charge-points/CP_001/reservations?limit=2")

    body = response.json()
    assert len(body["reservations"]) == 2
    assert body["next_cursor"]


@pytest.mark.asyncio
async def test_list_passes_filters_through(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import reservations as r_module

    spy = AsyncMock(return_value=[])
    monkeypatch.setattr(r_module, "list_reservations_by_cp", spy)

    response = await client.get(
        "/api/v1/charge-points/CP_001/reservations?status=Active&active=true&id_tag=RFID_X"
    )

    assert response.status_code == 200
    kwargs = spy.await_args.kwargs
    assert kwargs["status"] == "Active"
    assert kwargs["active"] is True
    assert kwargs["id_tag"] == "RFID_X"


@pytest.mark.asyncio
async def test_list_rejects_malformed_cursor(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/charge-points/CP_001/reservations?cursor=not-base64-!!!")

    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"
