"""Tests for the charge-points read endpoints."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

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


@pytest.mark.asyncio
async def test_list_returns_empty_when_no_chargers(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import charge_points as cp_module

    monkeypatch.setattr(cp_module, "list_charge_points", AsyncMock(return_value=[]))

    response = await client.get("/api/v1/charge-points")

    assert response.status_code == 200
    body = response.json()
    assert body["charge_points"] == []
    assert body["next_cursor"] is None
    assert body["request_id"]


@pytest.mark.asyncio
async def test_list_paginates_with_cursor(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Repo returns limit+1 rows → response has limit + a cursor."""
    from eveys_ocpp.api import charge_points as cp_module

    rows = [_cp_row(cp_id=f"CP_{i:03d}", internal_id=i) for i in range(1, 4)]  # 3 rows
    monkeypatch.setattr(cp_module, "list_charge_points", AsyncMock(return_value=rows))

    response = await client.get("/api/v1/charge-points?limit=2")

    body = response.json()
    assert len(body["charge_points"]) == 2
    assert body["next_cursor"]


@pytest.mark.asyncio
async def test_list_filters_by_online_post_postgres(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_registry: MagicMock,
) -> None:
    """The `online` filter runs against the Redis registry, not Postgres,
    so it filters AFTER paging — the spec accepts the resulting page may
    be shorter than `limit`."""
    from eveys_ocpp.api import charge_points as cp_module

    rows = [_cp_row(cp_id="CP_ONLINE", internal_id=1), _cp_row(cp_id="CP_OFFLINE", internal_id=2)]
    monkeypatch.setattr(cp_module, "list_charge_points", AsyncMock(return_value=rows))

    # Registry says CP_ONLINE has a pod, CP_OFFLINE has none.
    async def _get_pod(cp_id: str) -> str | None:
        return "pod-1" if cp_id == "CP_ONLINE" else None

    fake_registry.get_pod = AsyncMock(side_effect=_get_pod)

    response = await client.get("/api/v1/charge-points?online=true")

    body = response.json()
    assert len(body["charge_points"]) == 1
    assert body["charge_points"][0]["cp_id"] == "CP_ONLINE"
    assert body["charge_points"][0]["online"] is True
    assert body["charge_points"][0]["pod_id"] == "pod-1"


@pytest.mark.asyncio
async def test_list_passes_vendor_filter(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import charge_points as cp_module

    spy = AsyncMock(return_value=[])
    monkeypatch.setattr(cp_module, "list_charge_points", spy)

    await client.get("/api/v1/charge-points?vendor=ACME")

    assert spy.await_args.kwargs["vendor"] == "ACME"


@pytest.mark.asyncio
async def test_list_rejects_malformed_cursor(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/charge-points?cursor=not-base64-!!!")

    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


@pytest.mark.asyncio
async def test_detail_returns_404_for_unknown(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import charge_points as cp_module

    monkeypatch.setattr(cp_module, "get_charge_point_detail", AsyncMock(return_value=None))

    response = await client.get("/api/v1/charge-points/UNKNOWN_CP")

    assert response.status_code == 404
    body = response.json()
    assert body["error_code"] == "UNKNOWN_CP_ID"
    assert "request_id" in body


@pytest.mark.asyncio
async def test_detail_returns_full_shape_with_inlined_collections(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_registry: MagicMock,
) -> None:
    from eveys_ocpp.api import charge_points as cp_module

    detail = _cp_row(cp_id="CP_001", internal_id=1)
    detail["active_reservations"] = [
        {
            "reservation_id": 8842,
            "connector_id": 1,
            "id_tag": "RFID_FAMILY",
            "expiry_date": datetime(2026, 5, 5, 16, 0, tzinfo=UTC),
            "status": "Active",
        }
    ]
    detail["active_charging_profiles"] = [
        {
            "charging_profile_id": 42,
            "connector_id": 1,
            "stack_level": 0,
            "purpose": "TxDefaultProfile",
            "kind": "Recurring",
        }
    ]

    monkeypatch.setattr(cp_module, "get_charge_point_detail", AsyncMock(return_value=detail))
    fake_registry.get_pod = AsyncMock(return_value="pod-7b3fc9d")

    response = await client.get("/api/v1/charge-points/CP_001")

    body = response.json()
    assert body["cp_id"] == "CP_001"
    assert body["online"] is True
    assert body["pod_id"] == "pod-7b3fc9d"
    assert len(body["active_reservations"]) == 1
    assert body["active_reservations"][0]["reservation_id"] == 8842
    # expiry_date serialized as ISO-8601 string.
    assert isinstance(body["active_reservations"][0]["expiry_date"], str)
    assert len(body["active_charging_profiles"]) == 1
    assert body["active_charging_profiles"][0]["purpose"] == "TxDefaultProfile"
