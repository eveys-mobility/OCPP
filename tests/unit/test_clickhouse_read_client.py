"""Read-client unit tests — placeholder shape sanity (issue #92).

The route-layer tests mock the client wholesale, so they never see
the SQL. That left a real bug in production: `asynch` substitutes
parameters via `query.format(**escape_params(params))` — i.e. Python
`str.format` `{name}` placeholders, NOT the DB-API `%(name)s`
paramstyle. The two read methods used `%(name)s` and got 500s on
every real call (ClickHouse parses the literal `%`).

These tests stub the cursor at the boundary and assert the SQL we
*hand* to the server is well-formed under `asynch`'s substitution
rules — catches the regression at unit-test speed without booting
ClickHouse.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from asynch.proto.utils.escape import escape_params

from eveys_ocpp.clickhouse.read_client import ClickHouseReadClient


def _client_with_fake_cursor() -> tuple[ClickHouseReadClient, MagicMock]:
    """Build a client whose `Connection.cursor()` yields an
    AsyncMock cursor. Tests inspect `cursor.execute.await_args` to
    see the SQL + params actually sent."""

    class _FakeSettings:
        clickhouse_host = "ch"
        clickhouse_port = 9000
        clickhouse_db = "events"

    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[])
    cursor.description = []
    cursor.__aenter__ = AsyncMock(return_value=cursor)
    cursor.__aexit__ = AsyncMock(return_value=None)

    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)

    client = ClickHouseReadClient(_FakeSettings())  # type: ignore[arg-type]
    client._conn = conn  # type: ignore[attr-defined]
    return client, cursor


def _substitute(sql: str, params: dict[str, Any]) -> str:
    """Apply `asynch`'s exact substitution path — fails loud if the
    SQL has the wrong placeholder shape."""
    return sql.format(**escape_params(params))


@pytest.mark.asyncio
async def test_fetch_meter_values_uses_asynch_placeholder_shape() -> None:
    client, cursor = _client_with_fake_cursor()

    await client.fetch_meter_values(
        cp_id="CP_42",
        started_from=datetime(2026, 5, 1, tzinfo=UTC),
        started_to=datetime(2026, 5, 8, tzinfo=UTC),
        connector_id=1,
        measurand="Energy.Active.Import.Register",
        limit=1000,
    )

    sql, params = cursor.execute.await_args.args
    # The SQL must round-trip through asynch's `str.format` substitution
    # cleanly. If a `%(name)s` slipped back in, this raises KeyError /
    # IndexError on the format call — which is exactly the bug.
    rendered = _substitute(sql, params)
    assert "'CP_42'" in rendered
    assert "'2026-05-01 00:00:00'" in rendered
    assert "connector_id = 1" in rendered
    assert "measurand = 'Energy.Active.Import.Register'" in rendered
    assert "LIMIT 1000" in rendered
    assert "%(" not in rendered  # no DB-API placeholders should leak


@pytest.mark.asyncio
async def test_fetch_meter_values_skips_optional_filters_when_none() -> None:
    client, cursor = _client_with_fake_cursor()

    await client.fetch_meter_values(
        cp_id="CP_42",
        started_from=datetime(2026, 5, 1, tzinfo=UTC),
        started_to=datetime(2026, 5, 8, tzinfo=UTC),
        connector_id=None,
        measurand=None,
        limit=500,
    )

    sql, params = cursor.execute.await_args.args
    rendered = _substitute(sql, params)
    # `connector_id` and `measurand` are selected columns, so they
    # show up in the SELECT list. What we care about is that no
    # optional WHERE filter was added.
    assert "AND connector_id =" not in rendered
    assert "AND measurand =" not in rendered
    assert "LIMIT 500" in rendered


@pytest.mark.asyncio
async def test_fetch_status_history_uses_asynch_placeholder_shape() -> None:
    client, cursor = _client_with_fake_cursor()

    await client.fetch_status_history(
        cp_id="CP_42",
        started_from=datetime(2026, 5, 1, tzinfo=UTC),
        started_to=datetime(2026, 5, 8, tzinfo=UTC),
        connector_id=2,
        limit=1000,
    )

    sql, params = cursor.execute.await_args.args
    rendered = _substitute(sql, params)
    assert "'CP_42'" in rendered
    assert "connector_id = 2" in rendered
    assert "LIMIT 1000" in rendered
    assert "%(" not in rendered


@pytest.mark.asyncio
async def test_fetch_latest_connector_statuses_uses_asynch_placeholder_shape() -> None:
    """The third read method already used `{name}` correctly — pin it
    so a future refactor doesn't regress it the way the other two
    drifted."""
    client, cursor = _client_with_fake_cursor()
    cursor.fetchall = AsyncMock(return_value=[])
    cursor.description = []

    await client.fetch_latest_connector_statuses(cp_ids=["CP_A", "CP_B"])

    sql, params = cursor.execute.await_args.args
    rendered = _substitute(sql, params)
    assert "'CP_A'" in rendered
    assert "'CP_B'" in rendered
    assert "%(" not in rendered


@pytest.mark.asyncio
async def test_fetch_fleet_status_history_no_optional_filters() -> None:
    """With status + cp_ids both None the SQL has neither IN clause —
    a fleet-wide unfiltered scan over the time window."""
    client, cursor = _client_with_fake_cursor()

    await client.fetch_fleet_status_history(
        started_from=datetime(2026, 5, 1, tzinfo=UTC),
        started_to=datetime(2026, 5, 8, tzinfo=UTC),
        statuses=None,
        cp_ids=None,
        limit=500,
    )

    sql, params = cursor.execute.await_args.args
    rendered = _substitute(sql, params)
    assert "AND status IN" not in rendered
    assert "AND cp_id IN" not in rendered
    assert "%(" not in rendered


@pytest.mark.asyncio
async def test_fetch_fleet_status_history_status_in_clause() -> None:
    """`statuses=["Faulted", "Unavailable"]` expands into an `IN`
    clause with bound parameters — no string concatenation of user
    input into SQL."""
    client, cursor = _client_with_fake_cursor()

    await client.fetch_fleet_status_history(
        started_from=datetime(2026, 5, 1, tzinfo=UTC),
        started_to=datetime(2026, 5, 8, tzinfo=UTC),
        statuses=["Faulted", "Unavailable"],
        cp_ids=None,
        limit=1000,
    )

    sql, params = cursor.execute.await_args.args
    rendered = _substitute(sql, params)
    assert "'Faulted'" in rendered
    assert "'Unavailable'" in rendered
    assert "AND status IN" in rendered
    assert "AND cp_id IN" not in rendered
    # asynch's placeholder shape, not DB-API.
    assert "%(" not in rendered


@pytest.mark.asyncio
async def test_fetch_fleet_status_history_cp_ids_in_clause() -> None:
    client, cursor = _client_with_fake_cursor()

    await client.fetch_fleet_status_history(
        started_from=datetime(2026, 5, 1, tzinfo=UTC),
        started_to=datetime(2026, 5, 8, tzinfo=UTC),
        statuses=None,
        cp_ids=["CP_A", "CP_B"],
        limit=500,
    )

    sql, params = cursor.execute.await_args.args
    rendered = _substitute(sql, params)
    assert "'CP_A'" in rendered
    assert "'CP_B'" in rendered
    assert "AND cp_id IN" in rendered
    assert "%(" not in rendered


@pytest.mark.asyncio
async def test_fetch_frames_by_cp_uses_asynch_placeholder_shape() -> None:
    """Per-cp frame audit lookup. cp_id + time window required;
    direction + action are optional filters that expand into extra
    AND clauses with bound parameters."""
    client, cursor = _client_with_fake_cursor()

    await client.fetch_frames_by_cp(
        cp_id="CP_42",
        started_from=datetime(2026, 5, 1, tzinfo=UTC),
        started_to=datetime(2026, 5, 8, tzinfo=UTC),
        direction="inbound",
        action="MeterValues",
        limit=500,
    )

    sql, params = cursor.execute.await_args.args
    rendered = _substitute(sql, params)
    assert "'CP_42'" in rendered
    assert "direction = 'inbound'" in rendered
    assert "action = 'MeterValues'" in rendered
    assert "LIMIT 500" in rendered
    # asynch placeholder shape (`{name}`), not DB-API (`%(name)s`).
    assert "%(" not in rendered


@pytest.mark.asyncio
async def test_fetch_frames_by_cp_skips_optional_filters_when_none() -> None:
    """Without direction or action, the SQL has neither extra AND
    clause — the route handler must not synthesise empty-string
    filters that would silently match every row."""
    client, cursor = _client_with_fake_cursor()

    await client.fetch_frames_by_cp(
        cp_id="CP_42",
        started_from=datetime(2026, 5, 1, tzinfo=UTC),
        started_to=datetime(2026, 5, 8, tzinfo=UTC),
        direction=None,
        action=None,
        limit=200,
    )

    sql, params = cursor.execute.await_args.args
    rendered = _substitute(sql, params)
    assert "AND direction =" not in rendered
    assert "AND action =" not in rendered


@pytest.mark.asyncio
async def test_fetch_frames_by_transaction_uses_asynch_placeholder_shape() -> None:
    """No time window required — transactions are bounded already.
    The skip-index on transaction_id prunes parts so the scan stays
    cheap even across long retention windows."""
    client, cursor = _client_with_fake_cursor()

    await client.fetch_frames_by_transaction(transaction_id=12345, limit=1000)

    sql, params = cursor.execute.await_args.args
    rendered = _substitute(sql, params)
    assert "transaction_id = 12345" in rendered
    assert "LIMIT 1000" in rendered
    assert "%(" not in rendered


@pytest.mark.asyncio
async def test_fetch_uptime_for_cp_uses_asynch_placeholder_shape() -> None:
    """Uptime aggregation. cp_id + window required; the SQL clips
    interval edges to the window bounds inline so summing clipped
    seconds matches the route's response total exactly."""
    client, cursor = _client_with_fake_cursor()
    cursor.fetchall = AsyncMock(return_value=[])
    cursor.description = [
        ("clipped_offline_at",),
        ("clipped_online_at",),
        ("clipped_seconds",),
        ("original_offline_seconds",),
        ("prior_reason",),
    ]

    await client.fetch_uptime_for_cp(
        cp_id="CP_42",
        window_from=datetime(2026, 4, 1, tzinfo=UTC),
        window_to=datetime(2026, 5, 1, tzinfo=UTC),
    )

    sql, params = cursor.execute.await_args.args
    rendered = _substitute(sql, params)
    assert "'CP_42'" in rendered
    assert "greatest(went_offline_at" in rendered
    assert "least(came_online_at" in rendered
    assert "dateDiff" in rendered
    # asynch placeholder shape, not DB-API.
    assert "%(" not in rendered


@pytest.mark.asyncio
async def test_fetch_uptime_for_cp_sums_clipped_seconds() -> None:
    """Two rows the consumer would see — totals + intervals list
    line up. Zero-second rows (interval entirely outside window)
    are dropped."""
    client, cursor = _client_with_fake_cursor()
    cursor.description = [
        ("clipped_offline_at",),
        ("clipped_online_at",),
        ("clipped_seconds",),
        ("original_offline_seconds",),
        ("prior_reason",),
    ]
    cursor.fetchall = AsyncMock(
        return_value=[
            (
                datetime(2026, 4, 5, 10, 0, tzinfo=UTC),
                datetime(2026, 4, 5, 10, 30, tzinfo=UTC),
                1800,
                1800,
                "clean",
            ),
            (
                datetime(2026, 4, 10, 12, 0, tzinfo=UTC),
                datetime(2026, 4, 10, 12, 5, tzinfo=UTC),
                300,
                300,
                "error",
            ),
            # Degenerate zero-second row (edge case) — skipped.
            (
                datetime(2026, 4, 15, 0, 0, tzinfo=UTC),
                datetime(2026, 4, 15, 0, 0, tzinfo=UTC),
                0,
                0,
                "",
            ),
        ]
    )

    total, intervals = await client.fetch_uptime_for_cp(
        cp_id="CP_42",
        window_from=datetime(2026, 4, 1, tzinfo=UTC),
        window_to=datetime(2026, 5, 1, tzinfo=UTC),
    )

    assert total == 2100
    assert len(intervals) == 2
    assert intervals[0]["offline_seconds"] == 1800
    assert intervals[1]["offline_seconds"] == 300
    assert intervals[0]["prior_reason"] == "clean"
    # Empty-string prior_reason becomes None in the projected row.
    # (No interval with empty reason survives here — the zero-second
    # one was dropped — so this assertion would belong in the route
    # test where the response shape is asserted end-to-end.)
