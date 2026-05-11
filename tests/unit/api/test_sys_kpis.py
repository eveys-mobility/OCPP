"""Tests for `GET /api/v1/sys/kpis` — Console-dashboard rollup."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest


@pytest.mark.asyncio
async def test_returns_the_expected_shape(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import sys_kpis as kpis_module

    monkeypatch.setattr(kpis_module, "count_transactions", AsyncMock(return_value=0))
    monkeypatch.setattr(kpis_module, "count_charge_points", AsyncMock(return_value=0))

    response = await client.get("/api/v1/sys/kpis")
    assert response.status_code == 200, response.text
    body = response.json()
    for key in (
        "online_count",
        "total_count",
        "active_tx_count",
        "tx_today_count",
        "faulted_count",
        "energy_24h_wh",
        "request_id",
    ):
        assert key in body, f"missing: {key}"
    # Energy isn't backed by a rollup yet — must be null, not 0.
    assert body["energy_24h_wh"] is None


@pytest.mark.asyncio
async def test_online_count_is_null_when_registry_count_fails(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_registry: Any,
) -> None:
    """A Redis blip on count_online surfaces as null, not 0."""
    from eveys_ocpp.api import sys_kpis as kpis_module

    fake_registry.count_online = AsyncMock(side_effect=ConnectionError("redis down"))
    monkeypatch.setattr(kpis_module, "count_transactions", AsyncMock(return_value=0))
    monkeypatch.setattr(kpis_module, "count_charge_points", AsyncMock(return_value=0))

    response = await client.get("/api/v1/sys/kpis")
    body = response.json()
    assert body["online_count"] is None


@pytest.mark.asyncio
async def test_online_count_flows_from_registry(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_registry: Any,
) -> None:
    from eveys_ocpp.api import sys_kpis as kpis_module

    fake_registry.count_online = AsyncMock(return_value=42)
    monkeypatch.setattr(kpis_module, "count_transactions", AsyncMock(return_value=0))
    monkeypatch.setattr(kpis_module, "count_charge_points", AsyncMock(return_value=0))

    response = await client.get("/api/v1/sys/kpis")
    body = response.json()
    assert body["online_count"] == 42


@pytest.mark.asyncio
async def test_forwards_today_filter_to_count_transactions(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The `tx_today_count` call must pass a UTC-midnight `started_from`."""
    from eveys_ocpp.api import sys_kpis as kpis_module

    captured_kwargs: list[dict[str, Any]] = []

    async def fake_count_tx(_session: Any, **kwargs: Any) -> int:
        captured_kwargs.append(kwargs)
        return 0

    monkeypatch.setattr(kpis_module, "count_transactions", AsyncMock(side_effect=fake_count_tx))
    monkeypatch.setattr(kpis_module, "count_charge_points", AsyncMock(return_value=0))

    response = await client.get("/api/v1/sys/kpis")
    assert response.status_code == 200

    assert any(call.get("active") is True for call in captured_kwargs)
    started_from_calls = [c for c in captured_kwargs if "started_from" in c]
    assert started_from_calls, "expected a count_transactions call with started_from"
    midnight = started_from_calls[0]["started_from"]
    assert midnight.hour == 0
    assert midnight.minute == 0
    assert midnight.second == 0
    assert midnight.microsecond == 0


@pytest.mark.asyncio
async def test_faulted_count_filters_by_last_status(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import sys_kpis as kpis_module

    captured: list[dict[str, Any]] = []

    async def fake_count_cp(_session: Any, **kwargs: Any) -> int:
        captured.append(kwargs)
        # The faulted call passes last_status="Faulted"; return 3 for it
        # and 0 for the total-count call so the test can tell them apart.
        return 3 if kwargs.get("last_status") == "Faulted" else 0

    monkeypatch.setattr(kpis_module, "count_charge_points", AsyncMock(side_effect=fake_count_cp))
    monkeypatch.setattr(kpis_module, "count_transactions", AsyncMock(return_value=0))

    response = await client.get("/api/v1/sys/kpis")
    body = response.json()
    assert body["faulted_count"] == 3
    assert body["total_count"] == 0
    assert any(c.get("last_status") == "Faulted" for c in captured)
