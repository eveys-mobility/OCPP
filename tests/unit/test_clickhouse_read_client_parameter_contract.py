"""Actual asynch cursor/query path without a socket or ClickHouse server.

The installed driver exercises its own substitution contract (brace-based
in 0.3.1, percent-based in 0.4.0). Only native send/receive are replaced.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from asynch import Connection
from asynch.proto.utils.escape import escape_params

from eveys_ocpp.clickhouse.read_client import ClickHouseReadClient


def _native_client(monkeypatch: pytest.MonkeyPatch):
    connection = Connection(host="unused", database="events")
    protocol = connection._connection
    queries: list[str] = []
    arguments: list[Any] = []

    async def send_query(query: str, **_kwargs: Any) -> None:
        queries.append(query)

    async def execute(query: str, args: Any = None, **kwargs: Any):
        arguments.append(args)
        return await protocol.process_ordinary_query(
            query, params=args, with_column_types=kwargs["with_column_types"],
        )

    monkeypatch.setattr(protocol, "send_query", send_query)
    monkeypatch.setattr(protocol, "send_external_tables", AsyncMock())
    monkeypatch.setattr(protocol, "receive_result", AsyncMock(
        return_value=([("230.5",)], [("value", "String")]),
    ))
    # Bypass only the network-connecting ExecuteContext. Keep the real
    # Cursor and native process_ordinary_query/substitute_params methods.
    monkeypatch.setattr(protocol, "execute", execute)
    client = ClickHouseReadClient(SimpleNamespace(
        clickhouse_host="unused", clickhouse_port=9000, clickhouse_db="events",
    ))
    client._conn = connection
    return client, queries, arguments


@pytest.mark.asyncio
async def test_native_driver_receives_one_safely_rendered_query_without_reformatting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, queries, arguments = _native_client(monkeypatch)
    value = "CP_%(value)s_{missing}'\\\nOR 1=1 --"
    columns, rows = await client._fetch(
        "SELECT {value} AS value LIMIT {limit}", {"value": value, "limit": 1},
    )

    assert columns == ["value"]
    assert rows == [("230.5",)]
    assert queries == [f"SELECT {escape_params({'value': value})['value']} AS value LIMIT 1"]
    assert arguments == [None], "driver versions must not reinterpret template or literal data"


@pytest.mark.asyncio
async def test_native_meter_query_resolves_all_templates_and_preserves_escaped_cp_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, queries, arguments = _native_client(monkeypatch)
    cp_id = "CP_%(cp_id)s_{cp_id}'\\"
    result = await client.fetch_meter_values(
        cp_id=cp_id, started_from=datetime(2026, 10, 14, tzinfo=UTC),
        started_to=datetime(2026, 10, 15, tzinfo=UTC), connector_id=1,
        measurand="MEASURAND_VOLTAGE", limit=10,
    )

    assert result == [{"value": "230.5"}]
    assert f"cp_id = {escape_params({'cp_id': cp_id})['cp_id']}" in queries[0]
    assert "occurred_at >= '2026-10-14 00:00:00'" in queries[0]
    assert "occurred_at <= '2026-10-15 00:00:00'" in queries[0]
    assert "AND connector_id = 1" in queries[0]
    assert "AND measurand = 'MEASURAND_VOLTAGE'" in queries[0]
    assert "LIMIT 10" in queries[0]
    assert arguments == [None]
