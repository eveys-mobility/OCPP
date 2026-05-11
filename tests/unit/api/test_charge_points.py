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
async def test_list_online_true_pushes_cp_ids_in_filter_from_registry(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_registry: MagicMock,
) -> None:
    """`online=true` resolves to `cp_ids_in=<online_ids>` so the SQL
    count and page math respect the filter (no post-page trimming)."""
    from eveys_ocpp.api import charge_points as cp_module

    fake_registry.list_online_ids = AsyncMock(return_value=["CP_ONLINE_A", "CP_ONLINE_B"])
    fake_registry.get_pod = AsyncMock(side_effect=lambda cp_id: "pod-1")

    list_spy = AsyncMock(return_value=[_cp_row(cp_id="CP_ONLINE_A", internal_id=1)])
    monkeypatch.setattr(cp_module, "list_charge_points", list_spy)

    response = await client.get("/api/v1/charge-points?online=true")
    assert response.status_code == 200

    kwargs = list_spy.await_args.kwargs
    assert kwargs["cp_ids_in"] == ["CP_ONLINE_A", "CP_ONLINE_B"]
    assert kwargs["cp_ids_not_in"] is None


@pytest.mark.asyncio
async def test_list_online_false_pushes_cp_ids_not_in_filter(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_registry: MagicMock,
) -> None:
    """`online=false` resolves to `cp_ids_not_in=<online_ids>`."""
    from eveys_ocpp.api import charge_points as cp_module

    fake_registry.list_online_ids = AsyncMock(return_value=["CP_ONLINE_X"])
    fake_registry.get_pod = AsyncMock(return_value=None)

    list_spy = AsyncMock(return_value=[_cp_row(cp_id="CP_OFFLINE", internal_id=2)])
    monkeypatch.setattr(cp_module, "list_charge_points", list_spy)

    response = await client.get("/api/v1/charge-points?online=false")
    assert response.status_code == 200

    kwargs = list_spy.await_args.kwargs
    assert kwargs["cp_ids_in"] is None
    assert kwargs["cp_ids_not_in"] == ["CP_ONLINE_X"]


@pytest.mark.asyncio
async def test_list_online_true_in_page_mode_passes_filter_to_count(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_registry: MagicMock,
) -> None:
    """In page-mode the same `cp_ids_in` filter must reach
    `count_charge_points` — otherwise total reports the unfiltered
    count and the page footer lies to the operator."""
    from eveys_ocpp.api import charge_points as cp_module

    fake_registry.list_online_ids = AsyncMock(return_value=["CP_ONLINE_A"])
    fake_registry.get_pod = AsyncMock(return_value="pod-1")

    monkeypatch.setattr(
        cp_module,
        "list_charge_points",
        AsyncMock(return_value=[_cp_row(cp_id="CP_ONLINE_A", internal_id=1)]),
    )
    count_spy = AsyncMock(return_value=1)
    monkeypatch.setattr(cp_module, "count_charge_points", count_spy)

    response = await client.get("/api/v1/charge-points?page=1&page_size=10&online=true")
    assert response.status_code == 200

    kwargs = count_spy.await_args.kwargs
    assert kwargs["cp_ids_in"] == ["CP_ONLINE_A"]


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


# ---- per-connector status enrichment ---------------------------------


@pytest.mark.asyncio
async def test_list_includes_per_connector_status_from_clickhouse(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    """Each charger in the list response carries a `connectors` array
    populated from the latest StatusNotification per connector. Multi-
    connector chargers no longer collapse onto a single `last_status`
    slot — managers see every connector's true state."""
    from eveys_ocpp.api import charge_points as cp_module

    rows = [_cp_row(cp_id="CP_MULTI", internal_id=1)]
    monkeypatch.setattr(cp_module, "list_charge_points", AsyncMock(return_value=rows))
    fake_ch_client.fetch_latest_connector_statuses = AsyncMock(
        return_value={
            "CP_MULTI": [
                {
                    "connector_id": 1,
                    "status": "Charging",
                    "error_code": "NoError",
                    "last_changed_at": datetime(2026, 5, 5, 14, 30, tzinfo=UTC),
                },
                {
                    "connector_id": 2,
                    "status": "Available",
                    "error_code": "NoError",
                    "last_changed_at": datetime(2026, 5, 5, 14, 25, tzinfo=UTC),
                },
            ]
        }
    )

    response = await client.get("/api/v1/charge-points")

    body = response.json()
    cp = body["charge_points"][0]
    assert cp["cp_id"] == "CP_MULTI"
    connectors = cp["connectors"]
    assert len(connectors) == 2
    assert {c["connector_id"]: c["status"] for c in connectors} == {
        1: "Charging",
        2: "Available",
    }
    # last_changed_at serialized as ISO-8601.
    assert all(isinstance(c["last_changed_at"], str) for c in connectors)


@pytest.mark.asyncio
async def test_list_returns_empty_connectors_when_clickhouse_silent(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ClickHouse has no per-connector data for a charger (e.g.
    the charger has booted but never sent a StatusNotification), the
    response still includes a `connectors` field — empty list, never
    missing — so consumers can rely on the shape."""
    from eveys_ocpp.api import charge_points as cp_module

    rows = [_cp_row(cp_id="CP_SILENT", internal_id=1)]
    monkeypatch.setattr(cp_module, "list_charge_points", AsyncMock(return_value=rows))

    response = await client.get("/api/v1/charge-points")

    body = response.json()
    assert body["charge_points"][0]["connectors"] == []


@pytest.mark.asyncio
async def test_list_survives_clickhouse_failure(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    """A flaky ClickHouse must not 500 the /charge-points list. The
    metadata is the load-bearing data; the per-connector breakdown is
    enrichment — degrade gracefully to []."""
    from eveys_ocpp.api import charge_points as cp_module

    rows = [_cp_row(cp_id="CP_OK", internal_id=1)]
    monkeypatch.setattr(cp_module, "list_charge_points", AsyncMock(return_value=rows))
    fake_ch_client.fetch_latest_connector_statuses = AsyncMock(
        side_effect=RuntimeError("clickhouse exploded"),
    )

    response = await client.get("/api/v1/charge-points")

    assert response.status_code == 200
    body = response.json()
    assert body["charge_points"][0]["connectors"] == []


@pytest.mark.asyncio
async def test_detail_includes_per_connector_status(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_registry: MagicMock,
    fake_ch_client: MagicMock,
) -> None:
    from eveys_ocpp.api import charge_points as cp_module

    detail = _cp_row(cp_id="CP_001", internal_id=1)
    detail["active_reservations"] = []
    detail["active_charging_profiles"] = []
    monkeypatch.setattr(cp_module, "get_charge_point_detail", AsyncMock(return_value=detail))
    fake_registry.get_pod = AsyncMock(return_value="pod-1")
    fake_ch_client.fetch_latest_connector_statuses = AsyncMock(
        return_value={
            "CP_001": [
                {
                    "connector_id": 1,
                    "status": "Faulted",
                    "error_code": "GroundFailure",
                    "last_changed_at": datetime(2026, 5, 5, 14, 30, tzinfo=UTC),
                },
            ]
        }
    )

    response = await client.get("/api/v1/charge-points/CP_001")

    body = response.json()
    assert body["connectors"] == [
        {
            "connector_id": 1,
            "status": "Faulted",
            "error_code": "GroundFailure",
            "last_changed_at": "2026-05-05T14:30:00+00:00",
        }
    ]
