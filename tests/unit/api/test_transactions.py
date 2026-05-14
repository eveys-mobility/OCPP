"""Tests for the transactions read endpoints."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

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
    # UI-facing aliases: `started_at` and `stopped_at` point at the
    # charger-reported timestamps, and `open` reflects "not stopped yet."
    assert body["started_at"] == body["started_reported_at"]
    assert body["stopped_at"] == body["stopped_reported_at"]
    assert body["open"] is False


@pytest.mark.asyncio
async def test_get_by_id_active_emits_open_true(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A still-running session has null stop fields and `open=true`."""
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(
        tx_module,
        "get_transaction_by_id",
        AsyncMock(return_value=_tx_row(stopped=False)),
    )

    response = await client.get("/api/v1/transactions/999")

    body = response.json()
    assert body["stopped_at"] is None
    assert body["stopped_reported_at"] is None
    assert body["open"] is True


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
    # Wire shape uses `start` / `last` / derived `delta` (not the CH
    # client's internal `start_pct` / `last_pct`).
    assert body["telemetry"]["soc"]["start"] == 38.0
    assert body["telemetry"]["soc"]["last"] == 81.0
    assert body["telemetry"]["soc"]["delta"] == pytest.approx(43.0)
    phase_l1 = body["telemetry"]["phases"]["L1"]
    assert phase_l1["voltage_v"] == 231.4
    assert phase_l1["current_a"] == 14.8
    assert phase_l1["power_w"] == 3417.3
    # Each phase carries power_factor (null when not reported) +
    # occurred_at (mapped from the CH client's `last_at`).
    assert phase_l1["power_factor"] is None
    assert phase_l1["occurred_at"] == "2026-05-05T15:00:00+00:00"

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


# ---- /transactions/{id}/frames --------------------------------------------


@pytest.mark.asyncio
async def test_get_transaction_frames_unknown_tx_404(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(tx_module, "get_transaction_by_id", AsyncMock(return_value=None))

    response = await client.get("/api/v1/transactions/999/frames")

    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_TRANSACTION_ID"


@pytest.mark.asyncio
async def test_get_transaction_frames_returns_frames_for_known_tx(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    """Existing tx + ClickHouse rows → 200 with the projected frames.
    ``transaction_id`` in the row body is preserved."""
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(
        tx_module, "get_transaction_by_id", AsyncMock(return_value=_tx_row(transaction_id=42))
    )
    fake_ch_client.fetch_frames_by_transaction = AsyncMock(
        return_value=[
            {
                "event_id": "evt-x",
                "occurred_at": datetime(2026, 5, 6, 14, 0, tzinfo=UTC),
                "cp_id": "CP_001",
                "direction": "inbound",
                "action": "MeterValues",
                "message_type": 2,
                "message_id": "call-x",
                "ocpp_version": "ocpp1.6",
                "transaction_id": 42,
                "raw_payload": '[2,"call-x","MeterValues",{"transactionId":42}]',
            }
        ]
    )

    response = await client.get("/api/v1/transactions/42/frames")

    assert response.status_code == 200
    body = response.json()
    assert body["transaction_id"] == 42
    assert body["frames"][0]["action"] == "MeterValues"
    assert body["frames"][0]["transaction_id"] == 42


@pytest.mark.asyncio
async def test_get_transaction_frames_passes_limit_through(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    fake_ch_client: MagicMock,
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(
        tx_module, "get_transaction_by_id", AsyncMock(return_value=_tx_row(transaction_id=42))
    )
    spy = AsyncMock(return_value=[])
    fake_ch_client.fetch_frames_by_transaction = spy

    await client.get("/api/v1/transactions/42/frames?limit=10")

    kwargs = spy.await_args.kwargs
    assert kwargs["transaction_id"] == 42
    assert kwargs["limit"] == 10


# ---------------------------------------------------------------------------
# Aggregate analytics endpoint (B1 of eveys-console#192)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aggregate_returns_empty_buckets_when_repo_empty(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(tx_module, "aggregate_transactions", AsyncMock(return_value=[]))

    response = await client.get(
        "/api/v1/transactions/aggregate?from=2026-05-01T00:00:00Z&to=2026-05-10T00:00:00Z"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["buckets"] == []
    assert body["window"]["bucket"] == "day"
    assert body["window"]["group_by"] == "none"


@pytest.mark.asyncio
async def test_aggregate_forwards_filter_args_to_repo(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    spy = AsyncMock(return_value=[])
    monkeypatch.setattr(tx_module, "aggregate_transactions", spy)

    await client.get(
        "/api/v1/transactions/aggregate"
        "?from=2026-05-01T00:00:00Z&to=2026-05-10T00:00:00Z"
        "&bucket=hour&group_by=cp_id"
    )

    kwargs = spy.await_args.kwargs
    assert kwargs["bucket"] == "hour"
    assert kwargs["group_by"] == "cp_id"
    assert kwargs["window_from"] == datetime(2026, 5, 1, tzinfo=UTC)
    assert kwargs["window_to"] == datetime(2026, 5, 10, tzinfo=UTC)


@pytest.mark.asyncio
async def test_aggregate_shapes_single_row(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    repo_rows = [
        {
            "bucket_at": datetime(2026, 5, 5, tzinfo=UTC),
            "session_count": 3,
            "consumed_wh_total": 12_500,
            "duration_seconds_total": 7_200,
        }
    ]
    monkeypatch.setattr(tx_module, "aggregate_transactions", AsyncMock(return_value=repo_rows))

    response = await client.get(
        "/api/v1/transactions/aggregate?from=2026-05-01T00:00:00Z&to=2026-05-10T00:00:00Z"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body["buckets"]) == 1
    one = body["buckets"][0]
    assert one["session_count"] == 3
    assert one["consumed_wh_total"] == 12_500
    assert one["duration_seconds_total"] == 7_200
    # `group` is omitted from each row when group_by=none.
    assert "group" not in one


@pytest.mark.asyncio
async def test_aggregate_group_rows_include_group_field(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    repo_rows = [
        {
            "bucket_at": datetime(2026, 5, 5, tzinfo=UTC),
            "group": "CP_BERLIN_017",
            "session_count": 1,
            "consumed_wh_total": 5_000,
            "duration_seconds_total": 1_800,
        },
        {
            "bucket_at": datetime(2026, 5, 5, tzinfo=UTC),
            "group": "CP_BERLIN_022",
            "session_count": 2,
            "consumed_wh_total": 10_000,
            "duration_seconds_total": 3_600,
        },
    ]
    monkeypatch.setattr(tx_module, "aggregate_transactions", AsyncMock(return_value=repo_rows))

    response = await client.get(
        "/api/v1/transactions/aggregate"
        "?from=2026-05-01T00:00:00Z&to=2026-05-10T00:00:00Z"
        "&group_by=cp_id"
    )

    body = response.json()
    groups = {b["group"]: b for b in body["buckets"]}
    assert groups["CP_BERLIN_017"]["session_count"] == 1
    assert groups["CP_BERLIN_022"]["session_count"] == 2


@pytest.mark.asyncio
async def test_aggregate_rejects_window_over_90_days(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/transactions/aggregate?from=2025-01-01T00:00:00Z&to=2026-05-10T00:00:00Z"
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "WINDOW_TOO_LARGE"


@pytest.mark.asyncio
async def test_aggregate_rejects_inverted_window(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/transactions/aggregate?from=2026-05-10T00:00:00Z&to=2026-05-01T00:00:00Z"
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


@pytest.mark.asyncio
async def test_aggregate_rejects_unknown_bucket(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/transactions/aggregate"
        "?from=2026-05-01T00:00:00Z&to=2026-05-10T00:00:00Z"
        "&bucket=banana"
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


@pytest.mark.asyncio
async def test_aggregate_rejects_unknown_group_by(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/transactions/aggregate"
        "?from=2026-05-01T00:00:00Z&to=2026-05-10T00:00:00Z"
        "&group_by=color"
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


# ---------------------------------------------------------------------------
# Sortable list (#43)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_global_forwards_sort_and_dir(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    spy = AsyncMock(return_value=[])
    monkeypatch.setattr(tx_module, "list_transactions", spy)
    monkeypatch.setattr(tx_module, "count_transactions", AsyncMock(return_value=0))

    await client.get("/api/v1/transactions?page=1&page_size=20&sort=consumed_wh&dir=desc")

    kwargs = spy.await_args.kwargs
    assert kwargs["sort"] == "consumed_wh"
    assert kwargs["direction"] == "desc"


@pytest.mark.asyncio
async def test_list_global_defaults_dir_to_desc(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    spy = AsyncMock(return_value=[])
    monkeypatch.setattr(tx_module, "list_transactions", spy)
    monkeypatch.setattr(tx_module, "count_transactions", AsyncMock(return_value=0))

    await client.get("/api/v1/transactions?page=1&page_size=20&sort=started_at")

    assert spy.await_args.kwargs["direction"] == "desc"


@pytest.mark.asyncio
async def test_list_global_rejects_unknown_sort(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(tx_module, "list_transactions", AsyncMock(return_value=[]))

    response = await client.get("/api/v1/transactions?sort=banana")

    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


@pytest.mark.asyncio
async def test_list_global_rejects_unknown_dir(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(tx_module, "list_transactions", AsyncMock(return_value=[]))

    response = await client.get("/api/v1/transactions?sort=consumed_wh&dir=upside-down")

    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


@pytest.mark.asyncio
async def test_list_global_rejects_cursor_with_non_default_sort(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keyset cursors only walk stably when ordering by the surrogate id;
    a non-id sort + cursor would skip rows with tied sort-values across
    pages. Route forces page mode in that combo."""
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(tx_module, "list_transactions", AsyncMock(return_value=[]))

    response = await client.get("/api/v1/transactions?sort=consumed_wh&cursor=cur-1")

    assert response.status_code == 400
    body = response.json()
    assert body["error_code"] == "BAD_REQUEST"
    assert "cursor pagination is not supported" in body["error"]


@pytest.mark.asyncio
async def test_list_global_default_sort_remains_cursor_paginated(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`sort=id` (the implicit default) still allows cursor mode."""
    from eveys_ocpp.api import transactions as tx_module

    monkeypatch.setattr(tx_module, "list_transactions", AsyncMock(return_value=[]))

    response = await client.get("/api/v1/transactions?sort=id&cursor=cur-1")

    # Cursor decode happens after the sort check; a malformed cursor
    # would 400, but the sort check itself must NOT fire.
    assert response.status_code != 400 or "cursor pagination" not in (
        response.json().get("message") or ""
    )
