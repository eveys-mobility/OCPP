"""Tests for the charging-profiles read endpoint."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest


def _row(profile_id: int = 42, *, status: str = "Active") -> dict[str, Any]:
    return {
        "id": profile_id,
        "charging_profile_id": profile_id,
        "connector_id": 1,
        "stack_level": 0,
        "purpose": "TxDefaultProfile",
        "kind": "Recurring",
        "recurrency_kind": "Daily",
        "valid_from": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        "valid_to": datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        "transaction_id": None,
        "charging_rate_unit": "W",
        "min_charging_rate": None,
        "schedule_duration": 3600,
        "start_schedule": None,
        "status": status,
        "schedule_periods": [
            {"start_period": 0, "limit": 11000.0, "number_phases": 3},
        ],
        "created_at": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
    }


@pytest.mark.asyncio
async def test_list_returns_404_for_unknown_cp(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import charging_profiles as p_module

    monkeypatch.setattr(p_module, "list_charging_profiles_by_cp", AsyncMock(return_value=None))

    response = await client.get("/api/v1/charge-points/UNKNOWN/charging-profiles")

    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_list_returns_full_shape_with_periods(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import charging_profiles as p_module

    monkeypatch.setattr(
        p_module,
        "list_charging_profiles_by_cp",
        AsyncMock(return_value=[_row()]),
    )

    response = await client.get("/api/v1/charge-points/CP_001/charging-profiles")

    body = response.json()
    assert response.status_code == 200
    p = body["charging_profiles"][0]
    assert p["charging_profile_id"] == 42
    assert p["purpose"] == "TxDefaultProfile"
    assert p["cp_id"] == "CP_001"
    assert len(p["schedule_periods"]) == 1
    assert p["schedule_periods"][0]["limit"] == 11000.0
    assert isinstance(p["valid_from"], str)


@pytest.mark.asyncio
async def test_list_paginates(client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    from eveys_ocpp.api import charging_profiles as p_module

    rows = [_row(profile_id=i) for i in range(1, 4)]
    monkeypatch.setattr(p_module, "list_charging_profiles_by_cp", AsyncMock(return_value=rows))

    response = await client.get("/api/v1/charge-points/CP_001/charging-profiles?limit=2")

    body = response.json()
    assert len(body["charging_profiles"]) == 2
    assert body["next_cursor"]


@pytest.mark.asyncio
async def test_list_passes_filters_through(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import charging_profiles as p_module

    spy = AsyncMock(return_value=[])
    monkeypatch.setattr(p_module, "list_charging_profiles_by_cp", spy)

    response = await client.get(
        "/api/v1/charge-points/CP_001/charging-profiles"
        "?purpose=TxDefaultProfile&stack_level=2&connector_id=1"
    )

    assert response.status_code == 200
    kwargs = spy.await_args.kwargs
    assert kwargs["purpose"] == "TxDefaultProfile"
    assert kwargs["stack_level"] == 2
    assert kwargs["connector_id"] == 1


@pytest.mark.asyncio
async def test_list_rejects_malformed_cursor(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/charge-points/CP_001/charging-profiles?cursor=not-base64-!!!"
    )

    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"
