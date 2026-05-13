"""Tests for the expanded filter surface on list endpoints (#200).

Verifies that every new filter query param is forwarded to the
repository call. Per-endpoint happy paths + a representative
mixed-filter test per resource.
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


def _tx_row(*, internal_id: int = 1, cp_id: str = "CP_001") -> dict[str, Any]:
    return {
        "id": internal_id,
        "transaction_id": 10000 + internal_id,
        "cp_id": cp_id,
        "connector_id": 1,
        "id_tag": "USER",
        "meter_start_wh": 1000,
        "meter_stop_wh": 5000,
        "consumed_wh": 4000,
        "stop_reason": "Local",
        "started_reported_at": datetime(2026, 5, 5, 14, 0, tzinfo=UTC),
        "started_received_at": datetime(2026, 5, 5, 14, 0, tzinfo=UTC),
        "stopped_reported_at": datetime(2026, 5, 5, 15, 0, tzinfo=UTC),
        "stopped_received_at": datetime(2026, 5, 5, 15, 0, tzinfo=UTC),
    }


# ---- /charge-points filters ------------------------------------------------


@pytest.mark.asyncio
async def test_charge_points_filters_forwarded_to_repo(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every new filter query-param must show up in the repo call kwargs."""
    from eveys_ocpp.api import charge_points as cp_module

    list_mock = AsyncMock(return_value=[])
    monkeypatch.setattr(cp_module, "list_charge_points", list_mock)

    response = await client.get(
        "/api/v1/charge-points"
        "?model=X1"
        "&firmware_version=1.0.0"
        "&ocpp_version=ocpp1.6"
        "&last_status=Charging"
        "&last_firmware_status=Installed"
        "&last_diagnostics_status=Uploaded"
        "&last_log_status=Uploaded"
        "&last_boot_after=2026-05-01T00:00:00Z"
        "&last_boot_before=2026-05-31T00:00:00Z"
        "&last_heartbeat_after=2026-05-05T00:00:00Z"
        "&last_heartbeat_before=2026-05-10T00:00:00Z"
        "&created_after=2026-01-01T00:00:00Z"
        "&created_before=2026-12-31T00:00:00Z"
        "&cp_id_prefix=CP_ACME_"
        "&cp_id_contains=617b"
    )
    assert response.status_code == 200, response.json()

    kwargs = list_mock.await_args.kwargs
    assert kwargs["model"] == "X1"
    assert kwargs["firmware_version"] == "1.0.0"
    assert kwargs["ocpp_version"] == "ocpp1.6"
    assert kwargs["last_status"] == "Charging"
    assert kwargs["last_firmware_status"] == "Installed"
    assert kwargs["last_diagnostics_status"] == "Uploaded"
    assert kwargs["last_log_status"] == "Uploaded"
    assert kwargs["last_boot_after"] == datetime(2026, 5, 1, tzinfo=UTC)
    assert kwargs["last_boot_before"] == datetime(2026, 5, 31, tzinfo=UTC)
    assert kwargs["last_heartbeat_after"] == datetime(2026, 5, 5, tzinfo=UTC)
    assert kwargs["last_heartbeat_before"] == datetime(2026, 5, 10, tzinfo=UTC)
    assert kwargs["created_after"] == datetime(2026, 1, 1, tzinfo=UTC)
    assert kwargs["created_before"] == datetime(2026, 12, 31, tzinfo=UTC)
    assert kwargs["cp_id_prefix"] == "CP_ACME_"
    assert kwargs["cp_id_contains"] == "617b"


@pytest.mark.asyncio
async def test_charge_points_filters_in_offset_mode_reach_count(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """In offset mode, the same filters must reach `count_charge_points`."""
    from eveys_ocpp.api import charge_points as cp_module

    monkeypatch.setattr(cp_module, "list_charge_points", AsyncMock(return_value=[_cp_row()]))
    count_mock = AsyncMock(return_value=1)
    monkeypatch.setattr(cp_module, "count_charge_points", count_mock)

    response = await client.get(
        "/api/v1/charge-points?page=1&page_size=10"
        "&model=X1&last_status=Charging&ocpp_version=ocpp1.6"
    )
    assert response.status_code == 200
    kwargs = count_mock.await_args.kwargs
    assert kwargs["model"] == "X1"
    assert kwargs["last_status"] == "Charging"
    assert kwargs["ocpp_version"] == "ocpp1.6"


@pytest.mark.asyncio
async def test_charge_points_invalid_timestamp_400(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/charge-points?last_boot_after=not-a-date")
    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


# ---- /transactions filters -------------------------------------------------


@pytest.mark.asyncio
async def test_transactions_global_filters_forwarded(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    list_mock = AsyncMock(return_value=[])
    monkeypatch.setattr(tx_module, "list_transactions", list_mock)

    response = await client.get(
        "/api/v1/transactions"
        "?connector_id=2"
        "&stop_reason=EmergencyStop"
        "&stopped_after=2026-05-01T00:00:00Z"
        "&stopped_before=2026-05-31T00:00:00Z"
        "&min_consumed_wh=1000"
        "&max_consumed_wh=50000"
    )
    assert response.status_code == 200
    kwargs = list_mock.await_args.kwargs
    assert kwargs["connector_id"] == 2
    assert kwargs["stop_reason"] == "EmergencyStop"
    assert kwargs["stopped_from"] == datetime(2026, 5, 1, tzinfo=UTC)
    assert kwargs["stopped_to"] == datetime(2026, 5, 31, tzinfo=UTC)
    assert kwargs["min_consumed_wh"] == 1000
    assert kwargs["max_consumed_wh"] == 50000


@pytest.mark.asyncio
async def test_transactions_global_filters_reach_count_in_offset_mode(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(tx_module, "list_transactions", AsyncMock(return_value=[_tx_row()]))
    count_mock = AsyncMock(return_value=1)
    monkeypatch.setattr(tx_module, "count_transactions", count_mock)

    response = await client.get(
        "/api/v1/transactions?page=1&page_size=10&connector_id=2&stop_reason=Local"
    )
    assert response.status_code == 200
    kwargs = count_mock.await_args.kwargs
    assert kwargs["connector_id"] == 2
    assert kwargs["stop_reason"] == "Local"


@pytest.mark.asyncio
async def test_transactions_by_cp_filters_forwarded(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    list_mock = AsyncMock(return_value=[])
    monkeypatch.setattr(tx_module, "list_transactions_by_cp", list_mock)

    response = await client.get(
        "/api/v1/charge-points/CP_X/transactions"
        "?connector_id=1"
        "&stop_reason=Remote"
        "&stopped_after=2026-05-01T00:00:00Z"
        "&min_consumed_wh=500"
    )
    assert response.status_code == 200
    kwargs = list_mock.await_args.kwargs
    assert kwargs["connector_id"] == 1
    assert kwargs["stop_reason"] == "Remote"
    assert kwargs["stopped_from"] == datetime(2026, 5, 1, tzinfo=UTC)
    assert kwargs["min_consumed_wh"] == 500


@pytest.mark.asyncio
async def test_transactions_negative_min_consumed_4xx(
    client: httpx.AsyncClient,
) -> None:
    """Energy can't be negative; FastAPI's `ge=0` enforces this.

    FastAPI returns 422 for query-param validation errors; our
    handler may map it to 400. Accept either 4xx code."""
    response = await client.get("/api/v1/transactions?min_consumed_wh=-100")
    assert response.status_code in (400, 422)


@pytest.mark.asyncio
async def test_transactions_invalid_stopped_after_400(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/transactions?stopped_after=not-a-date")
    assert response.status_code == 400
