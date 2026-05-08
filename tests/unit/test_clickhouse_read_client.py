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
