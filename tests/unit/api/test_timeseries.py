"""Tests for the timeseries (meter-values + status-history) endpoints (E3-7d)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest


def _meter_row(connector_id: int = 1) -> dict[str, Any]:
    """Mock the *storage* shape — proto enum names (`MEASURAND_*`,
    `PHASE_*`, …) — not the OCPP wire form. The route layer (#136)
    translates these to the wire form on the way out."""
    return {
        "event_id": "evt-0001",
        "occurred_at": datetime(2026, 5, 6, 14, 0, tzinfo=UTC),
        "cp_id": "CP_001",
        "connector_id": connector_id,
        "transaction_id": 12345,
        "charger_reported_at": "2026-05-06T13:59:59Z",
        "value": "1500",
        "context": "CONTEXT_SAMPLE_PERIODIC",
        "format": "FORMAT_RAW",
        "measurand": "MEASURAND_ENERGY_ACTIVE_IMPORT_REGISTER",
        "phase": "",
        "location": "LOCATION_OUTLET",
        "unit": "UNIT_WH",
    }


def _status_row(connector_id: int = 1, status: str = "Available") -> dict[str, Any]:
    return {
        "event_id": "evt-0002",
        "occurred_at": datetime(2026, 5, 6, 14, 0, tzinfo=UTC),
        "cp_id": "CP_001",
        "connector_id": connector_id,
        "status": status,
        "error_code": "",
        "info": "",
        "vendor_id": "",
        "vendor_error_code": "",
        "charger_reported_at": "2026-05-06T13:59:59Z",
    }


# ---- meter-values ----------------------------------------------------------


@pytest.mark.asyncio
async def test_meter_values_unknown_cp_404(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=None))

    response = await client.get(
        "/api/v1/charge-points/UNKNOWN/meter-values"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
    )

    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_meter_values_returns_full_shape(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    fake_ch_client.fetch_meter_values = AsyncMock(return_value=[_meter_row()])

    response = await client.get(
        "/api/v1/charge-points/CP_001/meter-values"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
    )

    body = response.json()
    assert response.status_code == 200
    sample = body["meter_values"][0]
    assert sample["cp_id"] == "CP_001"
    assert sample["sample"]["measurand"] == "Energy.Active.Import.Register"
    assert sample["sample"]["unit"] == "Wh"
    # Empty proto strings should normalise to None on the wire.
    assert sample["sample"]["phase"] is None


@pytest.mark.asyncio
async def test_meter_values_passes_filters_through(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    spy = AsyncMock(return_value=[])
    fake_ch_client.fetch_meter_values = spy

    response = await client.get(
        "/api/v1/charge-points/CP_001/meter-values"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T01:00:00%2B00:00"
        "&connector_id=2&measurand=Energy.Active.Import.Register&limit=50"
    )

    assert response.status_code == 200
    kwargs = spy.await_args.kwargs
    assert kwargs["connector_id"] == 2
    # Route translates the OCPP wire form on input → proto enum name
    # (storage form) before issuing the SQL.
    assert kwargs["measurand"] == "MEASURAND_ENERGY_ACTIVE_IMPORT_REGISTER"
    assert kwargs["limit"] == 50


@pytest.mark.asyncio
async def test_meter_values_response_translates_proto_enum_names_to_ocpp_wire_form(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    """Storage layer returns proto enum names (`MEASURAND_*`, `PHASE_*`,
    `UNIT_*`); the API surfaces the OCPP wire form (`Voltage`, `L1`,
    `V`). Without this, every API consumer would have to learn two
    naming systems for the same enum dimension."""
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    row = _meter_row()
    row.update(
        {
            "measurand": "MEASURAND_VOLTAGE",
            "phase": "PHASE_L1",
            "unit": "UNIT_V",
            "context": "CONTEXT_SAMPLE_PERIODIC",
            "format": "FORMAT_RAW",
            "location": "LOCATION_OUTLET",
        }
    )
    fake_ch_client.fetch_meter_values = AsyncMock(return_value=[row])

    response = await client.get(
        "/api/v1/charge-points/CP_001/meter-values"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
    )

    sample = response.json()["meter_values"][0]["sample"]
    assert sample["measurand"] == "Voltage"
    assert sample["phase"] == "L1"
    assert sample["unit"] == "V"
    assert sample["context"] == "Sample.Periodic"
    assert sample["format"] == "Raw"
    assert sample["location"] == "Outlet"


@pytest.mark.asyncio
async def test_meter_values_response_normalises_unspecified_to_null(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    """`*_UNSPECIFIED` is an internal sentinel — it MUST NOT leak to
    API clients. Same goes for vendor-extension strings the ingestor
    stored verbatim from the wire (no proto mapping). Both surface as
    `null` so consumers can safely treat absent / unknown identically."""
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    row = _meter_row()
    row.update(
        {
            "measurand": "MEASURAND_UNSPECIFIED",  # internal sentinel
            "phase": "Vendor.Custom.Phase",  # vendor extension
            "unit": "",  # absent
        }
    )
    fake_ch_client.fetch_meter_values = AsyncMock(return_value=[row])

    response = await client.get(
        "/api/v1/charge-points/CP_001/meter-values"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
    )

    sample = response.json()["meter_values"][0]["sample"]
    assert sample["measurand"] is None
    assert sample["phase"] is None
    assert sample["unit"] is None


@pytest.mark.asyncio
async def test_meter_values_unknown_filter_short_circuits_to_empty(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    """`?measurand=Vendor.Custom` (unknown) returns an empty page WITHOUT
    issuing a CH query. The alternative — passing the unknown string
    through to SQL — would silently match every `_UNSPECIFIED` row and
    mislead the caller into thinking those rows match their filter."""
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    spy = AsyncMock(return_value=[_meter_row()])
    fake_ch_client.fetch_meter_values = spy

    response = await client.get(
        "/api/v1/charge-points/CP_001/meter-values"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
        "&measurand=Vendor.Custom.Reading"
    )

    assert response.status_code == 200
    assert response.json()["meter_values"] == []
    spy.assert_not_awaited()


@pytest.mark.asyncio
async def test_meter_values_window_too_large(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/charge-points/CP_001/meter-values"
        "?from=2026-04-01T00:00:00%2B00:00&to=2026-05-06T00:00:00%2B00:00"
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "WINDOW_TOO_LARGE"


@pytest.mark.asyncio
async def test_meter_values_inverted_window_400(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/charge-points/CP_001/meter-values"
        "?from=2026-05-06T23:00:00%2B00:00&to=2026-05-06T01:00:00%2B00:00"
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


@pytest.mark.asyncio
async def test_meter_values_unparseable_time_400(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/charge-points/CP_001/meter-values?from=not-iso&to=2026-05-06T00:00:00%2B00:00"
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


@pytest.mark.asyncio
async def test_meter_values_missing_window_422(client: httpx.AsyncClient) -> None:
    """Missing required `from`/`to` query params triggers FastAPI's
    validation handler, which our handler turns into 400 BAD_REQUEST."""
    response = await client.get("/api/v1/charge-points/CP_001/meter-values")
    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


# ---- status-history --------------------------------------------------------


@pytest.mark.asyncio
async def test_status_history_unknown_cp_404(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=None))

    response = await client.get(
        "/api/v1/charge-points/UNKNOWN/status-history"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
    )

    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_status_history_returns_full_shape(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    fake_ch_client.fetch_status_history = AsyncMock(return_value=[_status_row(status="Charging")])

    response = await client.get(
        "/api/v1/charge-points/CP_001/status-history"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
    )

    body = response.json()
    assert response.status_code == 200
    row = body["status_history"][0]
    assert row["status"] == "Charging"
    assert row["cp_id"] == "CP_001"
    assert row["error_code"] is None  # empty string → None


@pytest.mark.asyncio
async def test_status_history_passes_filters_through(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    spy = AsyncMock(return_value=[])
    fake_ch_client.fetch_status_history = spy

    response = await client.get(
        "/api/v1/charge-points/CP_001/status-history"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
        "&connector_id=2&limit=10"
    )

    assert response.status_code == 200
    kwargs = spy.await_args.kwargs
    assert kwargs["connector_id"] == 2
    assert kwargs["limit"] == 10


@pytest.mark.asyncio
async def test_status_history_window_too_large(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/charge-points/CP_001/status-history"
        "?from=2026-04-01T00:00:00%2B00:00&to=2026-05-06T00:00:00%2B00:00"
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "WINDOW_TOO_LARGE"


# ---- fleet status-history --------------------------------------------------


@pytest.mark.asyncio
async def test_fleet_status_returns_events_from_multiple_cps(
    client: httpx.AsyncClient,
    fake_ch_client: MagicMock,
) -> None:
    row_a = _status_row(status="Faulted") | {"cp_id": "CP_A"}
    row_b = _status_row(status="Faulted") | {"cp_id": "CP_B"}
    fake_ch_client.fetch_fleet_status_history = AsyncMock(return_value=[row_a, row_b])

    response = await client.get(
        "/api/v1/status-history"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
        "&status=Faulted"
    )

    assert response.status_code == 200
    body = response.json()
    assert {row["cp_id"] for row in body["events"]} == {"CP_A", "CP_B"}
    assert all(row["status"] == "Faulted" for row in body["events"])


@pytest.mark.asyncio
async def test_fleet_status_passes_status_and_cp_filters_through(
    client: httpx.AsyncClient,
    fake_ch_client: MagicMock,
) -> None:
    spy = AsyncMock(return_value=[])
    fake_ch_client.fetch_fleet_status_history = spy

    response = await client.get(
        "/api/v1/status-history"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
        "&status=Faulted&status=Unavailable"
        "&cp_id=CP_001&cp_id=CP_002"
        "&limit=50"
    )

    assert response.status_code == 200
    kwargs = spy.await_args.kwargs
    assert kwargs["statuses"] == ["Faulted", "Unavailable"]
    assert kwargs["cp_ids"] == ["CP_001", "CP_002"]
    assert kwargs["limit"] == 50


@pytest.mark.asyncio
async def test_fleet_status_no_filters_returns_all(
    client: httpx.AsyncClient,
    fake_ch_client: MagicMock,
) -> None:
    """No `status` / `cp_id` means no SQL filter — the client method
    receives None for both and the route doesn't 400 on missing
    filters (only `from`/`to` are required)."""
    spy = AsyncMock(return_value=[_status_row()])
    fake_ch_client.fetch_fleet_status_history = spy

    response = await client.get(
        "/api/v1/status-history?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
    )

    assert response.status_code == 200
    kwargs = spy.await_args.kwargs
    assert kwargs["statuses"] is None
    assert kwargs["cp_ids"] is None


@pytest.mark.asyncio
async def test_fleet_status_window_too_large(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/status-history?from=2026-04-01T00:00:00%2B00:00&to=2026-05-06T00:00:00%2B00:00"
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "WINDOW_TOO_LARGE"


@pytest.mark.asyncio
async def test_fleet_status_bad_window_orientation(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/status-history?from=2026-05-06T23:59:59%2B00:00&to=2026-05-06T00:00:00%2B00:00"
    )
    assert response.status_code == 400


# ---- frames-by-cp ----------------------------------------------------------


def _frame_row(
    cp_id: str = "CP_001",
    direction: str = "inbound",
    action: str = "MeterValues",
    transaction_id: int | None = 12345,
) -> dict[str, Any]:
    return {
        "event_id": "evt-0003",
        "occurred_at": datetime(2026, 5, 6, 14, 0, tzinfo=UTC),
        "cp_id": cp_id,
        "direction": direction,
        "action": action,
        "message_type": 2,
        "message_id": "call-abc",
        "ocpp_version": "ocpp1.6",
        "transaction_id": transaction_id,
        "raw_payload": '[2,"call-abc","MeterValues",{"transactionId":12345}]',
    }


@pytest.mark.asyncio
async def test_frames_by_cp_returns_full_shape(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    fake_ch_client.fetch_frames_by_cp = AsyncMock(return_value=[_frame_row()])

    response = await client.get(
        "/api/v1/charge-points/CP_001/frames"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["cp_id"] == "CP_001"
    row = body["frames"][0]
    assert row["direction"] == "inbound"
    assert row["action"] == "MeterValues"
    assert row["transaction_id"] == 12345
    assert row["raw_payload"].startswith("[2,")


@pytest.mark.asyncio
async def test_frames_by_cp_passes_optional_filters_through(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    spy = AsyncMock(return_value=[])
    fake_ch_client.fetch_frames_by_cp = spy

    await client.get(
        "/api/v1/charge-points/CP_001/frames"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
        "&direction=outbound&action=BootNotification&limit=50"
    )

    kwargs = spy.await_args.kwargs
    assert kwargs["direction"] == "outbound"
    assert kwargs["action"] == "BootNotification"
    assert kwargs["limit"] == 50


@pytest.mark.asyncio
async def test_frames_by_cp_window_too_large(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/charge-points/CP_001/frames"
        "?from=2026-04-01T00:00:00%2B00:00&to=2026-05-06T00:00:00%2B00:00"
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "WINDOW_TOO_LARGE"


@pytest.mark.asyncio
async def test_frames_by_cp_unknown_cp_404(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=None))

    response = await client.get(
        "/api/v1/charge-points/UNKNOWN/frames"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
    )
    assert response.status_code == 404


# ---- uptime ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_uptime_returns_100_percent_when_no_outages(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    """No offline intervals → uptime_pct = 100, offline = 0,
    intervals = []."""
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    fake_ch_client.fetch_uptime_for_cp = AsyncMock(return_value=(0, []))

    response = await client.get(
        "/api/v1/charge-points/CP_001/uptime"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-06T23:59:59%2B00:00"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["uptime_pct"] == 100.0
    assert body["offline_seconds_total"] == 0
    assert body["intervals"] == []
    # Window block confirms what was queried.
    assert body["window"]["seconds"] == 86399  # 23h59m59s
    assert body["cp_id"] == "CP_001"


@pytest.mark.asyncio
async def test_uptime_subtracts_offline_seconds_from_window(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    """One 1-hour outage inside a 24-hour window → uptime ≈ 95.83%.
    Intervals carry through with clipped timestamps."""
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    fake_ch_client.fetch_uptime_for_cp = AsyncMock(
        return_value=(
            3600,
            [
                {
                    "went_offline_at": datetime(2026, 5, 6, 10, 0, tzinfo=UTC),
                    "came_online_at": datetime(2026, 5, 6, 11, 0, tzinfo=UTC),
                    "offline_seconds": 3600,
                    "prior_reason": "clean",
                }
            ],
        )
    )

    response = await client.get(
        "/api/v1/charge-points/CP_001/uptime"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-07T00:00:00%2B00:00"
    )

    body = response.json()
    assert body["offline_seconds_total"] == 3600
    assert body["online_seconds_total"] == 86400 - 3600
    # 95.8333…, rounded to 4 decimals.
    assert abs(body["uptime_pct"] - 95.8333) < 0.001
    assert body["intervals"][0]["offline_seconds"] == 3600
    assert body["intervals"][0]["prior_reason"] == "clean"


@pytest.mark.asyncio
async def test_uptime_clamps_total_to_window(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    """If the aggregator returns more offline seconds than the
    window (shouldn't happen — the clip math guards it — but
    floating-point edges, row boundaries) the route clamps to
    avoid negative uptime%."""
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=1))
    # 999_999 > one-day window of 86400. Route should clamp to 86400.
    fake_ch_client.fetch_uptime_for_cp = AsyncMock(return_value=(999_999, []))

    response = await client.get(
        "/api/v1/charge-points/CP_001/uptime"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-07T00:00:00%2B00:00"
    )
    body = response.json()
    assert body["offline_seconds_total"] == 86400
    assert body["uptime_pct"] == 0.0


@pytest.mark.asyncio
async def test_uptime_window_too_large_400(client: httpx.AsyncClient) -> None:
    """Cap is 90 days for uptime (vs 7 for streams). 91 days → 400."""
    response = await client.get(
        "/api/v1/charge-points/CP_001/uptime"
        "?from=2026-01-01T00:00:00%2B00:00&to=2026-04-02T00:00:00%2B00:00"
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "WINDOW_TOO_LARGE"


@pytest.mark.asyncio
async def test_uptime_inverted_window_400(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/charge-points/CP_001/uptime"
        "?from=2026-05-07T00:00:00%2B00:00&to=2026-05-06T00:00:00%2B00:00"
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_uptime_unknown_cp_404(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import timeseries as ts_module

    monkeypatch.setattr(ts_module, "get_charge_point_pk", AsyncMock(return_value=None))

    response = await client.get(
        "/api/v1/charge-points/UNKNOWN/uptime"
        "?from=2026-05-06T00:00:00%2B00:00&to=2026-05-07T00:00:00%2B00:00"
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"
