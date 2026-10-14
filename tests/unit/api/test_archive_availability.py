"""Archive outages must not masquerade as empty history or erase PG state."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from eveys_ocpp.api import timeseries, transactions

from .test_transactions import _tx_row

_WINDOW = "?from=2026-10-13T00:00:00Z&to=2026-10-13T01:00:00Z"
_ARCHIVE_ROUTES = [
    ("/charge-points/CP_001/meter-values" + _WINDOW, "fetch_meter_values"),
    ("/charge-points/CP_001/status-history" + _WINDOW, "fetch_status_history"),
    ("/status-history" + _WINDOW, "fetch_fleet_status_history"),
    ("/charge-points/CP_001/frames" + _WINDOW, "fetch_frames_by_cp"),
    ("/charge-points/CP_001/uptime" + _WINDOW, "fetch_uptime_for_cp"),
    ("/charge-points/CP_001/offline-history", "fetch_offline_history"),
    ("/charge-points/CP_001/offline-history?page=1", "fetch_offline_history"),
    ("/transactions/4823/frames", "fetch_frames_by_transaction"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("path,method", _ARCHIVE_ROUTES)
@pytest.mark.parametrize("configured", [False, True])
async def test_archive_unavailable_is_sanitized_503(
    client: httpx.AsyncClient,
    fake_ch_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    method: str,
    configured: bool,
) -> None:
    client._transport.raise_app_exceptions = False
    monkeypatch.setattr(timeseries, "get_charge_point_pk", AsyncMock(return_value=1))
    monkeypatch.setattr(
        transactions, "get_transaction_by_id", AsyncMock(return_value=_tx_row(4823))
    )
    if configured:
        setattr(fake_ch_client, method, AsyncMock(side_effect=OSError("PRIVATE_TEST_DATA")))
    else:
        client._transport.app.state.ch_client = None

    response = await client.get("/api/v1" + path)

    assert response.status_code == 503
    assert response.json()["error_code"] == "ARCHIVE_UNAVAILABLE"
    assert "PRIVATE_TEST_DATA" not in response.text
    assert "frames" not in response.json()
    assert response.json()["request_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("stopped", [False, True])
async def test_telemetry_outage_preserves_physical_transaction(
    client: httpx.AsyncClient,
    fake_ch_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    stopped: bool,
) -> None:
    client._transport.raise_app_exceptions = False
    monkeypatch.setattr(
        transactions,
        "get_transaction_by_id",
        AsyncMock(return_value=_tx_row(4823, stopped=stopped)),
    )
    fake_ch_client.fetch_transaction_telemetry = AsyncMock(side_effect=OSError("PRIVATE_TEST_DATA"))

    response = await client.get("/api/v1/transactions/4823")

    assert response.status_code == 200
    body = response.json()
    assert body["transaction_id"] == 4823
    assert body["cp_id"] == "CP_001" and body["connector_id"] == 1
    assert body["open"] is not stopped
    assert body["stop_reason"] == ("Local" if stopped else None)
    assert body["telemetry"] is None
    assert body["telemetry_state"] == "unavailable"
    assert body["telemetry_error"] == "ARCHIVE_UNAVAILABLE"
    assert "PRIVATE_TEST_DATA" not in response.text


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", [False, True])
async def test_empty_success_differs_from_unconfigured_telemetry(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    configured: bool,
) -> None:
    monkeypatch.setattr(
        transactions, "get_transaction_by_id", AsyncMock(return_value=_tx_row(4823))
    )
    if not configured:
        client._transport.app.state.ch_client = None

    response = await client.get("/api/v1/transactions/4823")

    assert response.status_code == 200
    body = response.json()
    assert body["telemetry_state"] == ("available" if configured else "unconfigured")
    assert body["telemetry_error"] == (None if configured else "ARCHIVE_UNCONFIGURED")
    if configured:
        assert body["telemetry"] == {
            "soc": {"start": None, "last": None, "delta": None},
            "phases": {},
        }
    else:
        assert body["telemetry"] is None
