"""Tests for the transactions read endpoints."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from eveys_ocpp.api._app import make_app
from eveys_ocpp.settings import Settings


def _tx_row(
    transaction_id: int = 999,
    *,
    stopped: bool = False,
    cp_id: str = "CP_001",
) -> dict[str, Any]:
    return {
        "id": transaction_id,
        "transaction_id": transaction_id,
        "charge_point_id": 1,
        "cp_id": cp_id,
        "connector_id": 1,
        "id_tag": "RFID_001",
        "meter_start_wh": 1_000_000,
        "meter_stop_wh": 1_500_000 if stopped else None,
        "consumed_wh": 500_000 if stopped else None,
        "started_reported_at": datetime(2026, 5, 5, 14, 0, tzinfo=UTC),
        "started_received_at": datetime(2026, 5, 5, 14, 0, 1, tzinfo=UTC),
        "stopped_reported_at": datetime(2026, 5, 5, 15, 0, tzinfo=UTC) if stopped else None,
        "stopped_received_at": datetime(2026, 5, 5, 15, 0, 1, tzinfo=UTC) if stopped else None,
        "stop_reason": "Local" if stopped else None,
    }


@pytest.mark.asyncio
async def test_get_by_id_returns_404_when_unknown(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(tx_module, "get_transaction_by_id", AsyncMock(return_value=None))

    response = await client.get("/api/v1/transactions/12345")

    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_TRANSACTION_ID"


@pytest.mark.asyncio
async def test_get_by_id_returns_full_shape(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(
        tx_module,
        "get_transaction_by_id",
        AsyncMock(return_value=_tx_row(stopped=True)),
    )

    response = await client.get("/api/v1/transactions/999")

    body = response.json()
    assert body["transaction_id"] == 999
    assert body["consumed_wh"] == 500_000
    assert isinstance(body["started_reported_at"], str)
    assert body["stop_reason"] == "Local"


@pytest.mark.asyncio
async def test_list_by_cp_returns_404_for_unknown_cp(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`list_transactions_by_cp` returns None to signal unknown cp_id."""
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(tx_module, "list_transactions_by_cp", AsyncMock(return_value=None))

    response = await client.get("/api/v1/charge-points/UNKNOWN/transactions")

    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_list_by_cp_paginates_and_emits_cursor(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    rows = [_tx_row(transaction_id=i) for i in range(1, 4)]  # 3 rows
    monkeypatch.setattr(tx_module, "list_transactions_by_cp", AsyncMock(return_value=rows))

    response = await client.get("/api/v1/charge-points/CP_001/transactions?limit=2")

    body = response.json()
    assert len(body["transactions"]) == 2
    assert body["next_cursor"]
    # The route fills cp_id (the repo dict doesn't carry it).
    assert body["transactions"][0]["cp_id"] == "CP_001"


@pytest.mark.asyncio
async def test_list_by_cp_passes_filters_through(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    spy = AsyncMock(return_value=[])
    monkeypatch.setattr(tx_module, "list_transactions_by_cp", spy)

    response = await client.get(
        "/api/v1/charge-points/CP_001/transactions"
        "?id_tag=RFID_X&open=true&from=2026-05-01T00:00:00%2B00:00&to=2026-05-31T00:00:00%2B00:00"
    )

    assert response.status_code == 200
    kwargs = spy.await_args.kwargs
    assert kwargs["id_tag"] == "RFID_X"
    assert kwargs["open_only"] is True
    assert kwargs["started_from"] is not None
    assert kwargs["started_to"] is not None


@pytest.mark.asyncio
async def test_list_by_cp_rejects_unparseable_time(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/charge-points/CP_001/transactions?from=not-iso-8601")

    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


# ----- GET /api/v1/transactions (global list) -------------------------------


def _tx_row_with_cp(transaction_id: int, cp_id: str, *, stopped: bool = False) -> dict[str, Any]:
    row = _tx_row(transaction_id, stopped=stopped)
    row["cp_id"] = cp_id
    return row


@pytest.mark.asyncio
async def test_list_global_empty_returns_empty_array(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(tx_module, "list_transactions", AsyncMock(return_value=[]))

    response = await client.get("/api/v1/transactions")

    assert response.status_code == 200
    body = response.json()
    assert body["transactions"] == []
    assert body["next_cursor"] is None


@pytest.mark.asyncio
async def test_list_global_returns_cp_id_per_row(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    rows = [
        _tx_row_with_cp(1, "CP_BERLIN_001"),
        _tx_row_with_cp(2, "CP_BERLIN_002"),
    ]
    monkeypatch.setattr(tx_module, "list_transactions", AsyncMock(return_value=rows))

    response = await client.get("/api/v1/transactions")

    body = response.json()
    assert [t["cp_id"] for t in body["transactions"]] == ["CP_BERLIN_001", "CP_BERLIN_002"]


@pytest.mark.asyncio
async def test_list_global_paginates_and_emits_cursor(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    rows = [_tx_row_with_cp(i, "CP_X") for i in range(1, 4)]  # 3 rows
    monkeypatch.setattr(tx_module, "list_transactions", AsyncMock(return_value=rows))

    response = await client.get("/api/v1/transactions?limit=2")

    body = response.json()
    assert len(body["transactions"]) == 2
    assert body["next_cursor"]


@pytest.mark.asyncio
async def test_list_global_passes_filters_through(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    spy = AsyncMock(return_value=[])
    monkeypatch.setattr(tx_module, "list_transactions", spy)

    response = await client.get(
        "/api/v1/transactions"
        "?cp_id=CP_X&id_tag=RFID_X&active=true"
        "&from=2026-05-01T00:00:00%2B00:00&to=2026-05-31T00:00:00%2B00:00"
    )

    assert response.status_code == 200
    kwargs = spy.await_args.kwargs
    assert kwargs["cp_id"] == "CP_X"
    assert kwargs["id_tag"] == "RFID_X"
    assert kwargs["active"] is True
    assert kwargs["started_from"] is not None
    assert kwargs["started_to"] is not None


@pytest.mark.asyncio
async def test_list_global_rejects_unparseable_time(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/transactions?from=not-iso-8601")

    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


@pytest.mark.asyncio
async def test_list_global_does_not_collide_with_detail_route(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sanity: /transactions and /transactions/{id} are distinct routes."""
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(
        tx_module, "get_transaction_by_id", AsyncMock(return_value=_tx_row(transaction_id=42))
    )
    detail = await client.get("/api/v1/transactions/42")
    assert detail.status_code == 200
    assert detail.json()["transaction_id"] == 42

    monkeypatch.setattr(tx_module, "list_transactions", AsyncMock(return_value=[]))
    listing = await client.get("/api/v1/transactions")
    assert listing.status_code == 200
    assert listing.json()["transactions"] == []


# ----- telemetry on detail endpoint -----------------------------------------


@pytest.mark.asyncio
async def test_detail_includes_telemetry_block(
    client: httpx.AsyncClient,
    fake_ch_client: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Detail response stitches the ClickHouse telemetry snapshot under
    `telemetry`, scoped by (cp_id, transaction_id)."""
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(
        tx_module,
        "get_transaction_by_id",
        AsyncMock(return_value=_tx_row(transaction_id=999, stopped=True, cp_id="CP_BERLIN_001")),
    )
    fake_ch_client.fetch_transaction_telemetry = AsyncMock(
        return_value={
            "soc": {
                "start_pct": 38.0,
                "last_pct": 81.0,
                "last_at": "2026-05-05T15:00:00+00:00",
            },
            "phases": {
                "L1": {
                    "voltage_v": 231.4,
                    "current_a": 14.8,
                    "power_w": 3417.3,
                    "last_at": "2026-05-05T15:00:00+00:00",
                },
            },
        }
    )

    response = await client.get("/api/v1/transactions/999")

    assert response.status_code == 200
    body = response.json()
    assert body["telemetry"]["soc"]["start_pct"] == 38.0
    assert body["telemetry"]["soc"]["last_pct"] == 81.0
    assert body["telemetry"]["phases"]["L1"]["voltage_v"] == 231.4
    assert body["telemetry"]["phases"]["L1"]["current_a"] == 14.8
    assert body["telemetry"]["phases"]["L1"]["power_w"] == 3417.3

    fake_ch_client.fetch_transaction_telemetry.assert_awaited_once_with(
        cp_id="CP_BERLIN_001", transaction_id=999
    )


@pytest.mark.asyncio
async def test_detail_telemetry_null_when_no_ch_client(
    settings: Settings,
    fake_session_factory: Any,
    fake_registry: Any,
    fake_command_service: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compose-smoke / unit-test gateways may run without ClickHouse —
    surface `telemetry: null` instead of 500ing on a detail call."""
    app = make_app(
        session_factory=fake_session_factory,
        settings=settings,
        registry=fake_registry,
        redis=None,
        command_service=fake_command_service,
        ch_client=None,
    )

    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(
        tx_module, "get_transaction_by_id", AsyncMock(return_value=_tx_row(transaction_id=42))
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gw",
        headers={"Authorization": "Bearer test-token-foundation"},
    ) as ac:
        response = await ac.get("/api/v1/transactions/42")

    assert response.status_code == 200
    body = response.json()
    assert body["transaction_id"] == 42
    assert body["telemetry"] is None


@pytest.mark.asyncio
async def test_list_responses_have_no_telemetry_field(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Telemetry is detail-only — list responses must stay Postgres-only
    so cursor pages don't fan out N+1 ClickHouse queries."""
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(
        tx_module,
        "list_transactions",
        AsyncMock(return_value=[_tx_row_with_cp(1, "CP_X"), _tx_row_with_cp(2, "CP_Y")]),
    )

    response = await client.get("/api/v1/transactions")

    body = response.json()
    for row in body["transactions"]:
        assert "telemetry" not in row
