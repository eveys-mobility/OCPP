"""Tests for the offline-history endpoint."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest


def _row(event_id: str = "evt-1", offline_seconds: int = 60) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "occurred_at": datetime(2026, 5, 11, 12, 1, tzinfo=UTC),
        "cp_id": "CP_001",
        "went_offline_at": datetime(2026, 5, 11, 12, 0, tzinfo=UTC),
        "came_online_at": datetime(2026, 5, 11, 12, 1, tzinfo=UTC),
        "offline_seconds": offline_seconds,
        "prior_pod_id": "pod-1",
        "prior_reason": "clean",
    }


@pytest.mark.asyncio
async def test_unknown_cp_404(client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=None))

    response = await client.get("/api/v1/charge-points/UNKNOWN/offline-history")
    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_returns_full_shape(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    fake_ch_client.fetch_offline_history = AsyncMock(return_value=([_row()], 1))

    response = await client.get("/api/v1/charge-points/CP_001/offline-history")
    body = response.json()
    assert response.status_code == 200
    assert body["cp_id"] == "CP_001"
    window = body["offline_windows"][0]
    assert window["event_id"] == "evt-1"
    assert window["offline_seconds"] == 60
    assert window["went_offline_at"] == "2026-05-11T12:00:00+00:00"
    assert window["came_online_at"] == "2026-05-11T12:01:00+00:00"
    assert window["prior_reason"] == "clean"


@pytest.mark.asyncio
async def test_page_mode_returns_pagination_block(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    fake_ch_client.fetch_offline_history = AsyncMock(
        return_value=([_row(event_id=f"evt-{i}") for i in range(2)], 5),
    )

    response = await client.get("/api/v1/charge-points/CP_001/offline-history?page=1&page_size=2")
    body = response.json()
    assert response.status_code == 200
    assert body["pagination"] == {
        "page": 1,
        "page_size": 2,
        "total": 5,
        "total_pages": 3,
        "has_next": True,
        "has_prev": False,
    }
    # Page mode does NOT include `next_cursor` — the two modes are
    # mutually exclusive on a single response (same rule as
    # /charge-points).
    assert "next_cursor" not in body or body["next_cursor"] is None


@pytest.mark.asyncio
async def test_cursor_mode_emits_next_cursor(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    """Repo returns limit+1 rows → response trims to limit and emits a
    cursor pointing at the next offset."""
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    fake_ch_client.fetch_offline_history = AsyncMock(
        return_value=([_row(event_id=f"evt-{i}") for i in range(3)], 100),
    )

    response = await client.get("/api/v1/charge-points/CP_001/offline-history?limit=2")
    body = response.json()
    assert len(body["offline_windows"]) == 2
    assert body["next_cursor"]
    decoded = json.loads(base64.urlsafe_b64decode(body["next_cursor"]))
    assert decoded == {"offset": 2}


@pytest.mark.asyncio
async def test_since_until_passed_through_to_clickhouse(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    fetch = AsyncMock(return_value=([], 0))
    fake_ch_client.fetch_offline_history = fetch

    response = await client.get(
        "/api/v1/charge-points/CP_001/offline-history"
        "?since=2026-05-01T00:00:00%2B00:00&until=2026-05-11T00:00:00%2B00:00"
    )
    assert response.status_code == 200
    kwargs = fetch.await_args.kwargs
    assert kwargs["since"] == datetime(2026, 5, 1, tzinfo=UTC)
    assert kwargs["until"] == datetime(2026, 5, 11, tzinfo=UTC)


@pytest.mark.asyncio
async def test_inverted_window_400(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    response = await client.get(
        "/api/v1/charge-points/CP_001/offline-history"
        "?since=2026-05-11T00:00:00%2B00:00&until=2026-05-01T00:00:00%2B00:00"
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


@pytest.mark.asyncio
async def test_mixed_pagination_400(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    response = await client.get("/api/v1/charge-points/CP_001/offline-history?cursor=abc&page=1")
    assert response.status_code == 400
