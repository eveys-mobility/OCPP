"""Native connection recovery/serialization without opening a socket."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from asynch.errors import ProgrammingError

from eveys_ocpp.clickhouse.read_client import ClickHouseReadClient


class _Connection:
    def __init__(self, *, failure: Exception | None = None, hold: asyncio.Event | None = None):
        self.failure = failure
        self.hold = hold
        self.started = asyncio.Event()
        self.executing = False
        self.overlapped = False
        self.closed = False

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        self.closed = True

    def cursor(self) -> _Cursor:
        return _Cursor(self)


class _Cursor:
    description = (("cp_id",), ("connector_id",), ("status",))

    def __init__(self, connection: _Connection):
        self.connection = connection

    async def __aenter__(self) -> _Cursor:
        return self

    async def __aexit__(self, *_: Any) -> None:
        pass

    async def execute(self, _sql: str, _params: dict[str, Any]) -> None:
        if self.connection.executing:
            self.connection.overlapped = True
            raise ProgrammingError("some records have not been fetched")
        self.connection.executing = True
        self.connection.started.set()
        if self.connection.failure is not None:
            raise self.connection.failure
        if self.connection.hold is not None:
            await self.connection.hold.wait()

    async def fetchall(self) -> list[tuple[Any, ...]]:
        self.connection.executing = False
        return [("CP_TEST", 1, "Charging")]


def _client(connection: _Connection) -> ClickHouseReadClient:
    client = ClickHouseReadClient(
        SimpleNamespace(clickhouse_host="unused", clickhouse_port=9000, clickhouse_db="events")
    )
    client._conn = connection
    return client


@pytest.mark.asyncio
async def test_concurrent_reads_wait_until_previous_result_is_fully_fetched() -> None:
    release = asyncio.Event()
    connection = _Connection(hold=release)
    client = _client(connection)

    first = asyncio.create_task(client.fetch_latest_connector_statuses(cp_ids=["CP_TEST"]))
    await connection.started.wait()
    second = asyncio.create_task(client.fetch_latest_connector_statuses(cp_ids=["CP_TEST"]))
    await asyncio.sleep(0)
    release.set()
    results = await asyncio.gather(first, second, return_exceptions=True)

    assert not connection.overlapped
    assert results == [
        {"CP_TEST": [{"connector_id": 1, "status": "Charging"}]},
        {"CP_TEST": [{"connector_id": 1, "status": "Charging"}]},
    ]


@pytest.mark.asyncio
async def test_stuck_query_discards_connection_and_retries_read_on_a_new_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broken = _Connection(failure=ProgrammingError("some records have not been fetched"))
    replacement = _Connection()
    client = _client(broken)
    monkeypatch.setattr("eveys_ocpp.clickhouse.read_client.Connection", lambda **_: replacement)

    result = await client.fetch_latest_connector_statuses(cp_ids=["CP_TEST"])

    assert result == {"CP_TEST": [{"connector_id": 1, "status": "Charging"}]}
    assert broken.closed
    assert client._conn is replacement


@pytest.mark.asyncio
async def test_cancelled_read_does_not_leave_busy_connection_for_the_next_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    interrupted = _Connection(hold=asyncio.Event())
    replacement = _Connection()
    client = _client(interrupted)
    monkeypatch.setattr("eveys_ocpp.clickhouse.read_client.Connection", lambda **_: replacement)
    task = asyncio.create_task(client.fetch_latest_connector_statuses(cp_ids=["CP_TEST"]))
    await interrupted.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    result = await client.fetch_latest_connector_statuses(cp_ids=["CP_TEST"])

    assert interrupted.closed
    assert result == {"CP_TEST": [{"connector_id": 1, "status": "Charging"}]}


@pytest.mark.asyncio
async def test_permanent_read_failure_retries_once_and_keeps_no_busy_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broken = _Connection(failure=OSError("offline"))
    replacement = _Connection(failure=OSError("still offline"))
    client = _client(broken)
    monkeypatch.setattr("eveys_ocpp.clickhouse.read_client.Connection", lambda **_: replacement)

    with pytest.raises(OSError, match="still offline"):
        await client.fetch_latest_connector_statuses(cp_ids=["CP_TEST"])

    assert broken.closed and replacement.closed
    assert client._conn is None


@pytest.mark.asyncio
async def test_gateway_read_client_start_does_not_connect_to_an_unavailable_archive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connections: list[_Connection] = []

    def connection(**_: Any) -> _Connection:
        connections.append(_Connection(failure=OSError("archive unavailable")))
        raise OSError("archive unavailable")

    monkeypatch.setattr("eveys_ocpp.clickhouse.read_client.Connection", connection)
    client = _client(_Connection())
    client._conn = None

    try:
        await client.start()
    except OSError:
        pytest.fail("gateway startup attempted an archive connection")

    assert connections == []


@pytest.mark.asyncio
async def test_lazy_first_query_failure_can_recover_without_restarting_the_gateway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OfflineConnection(_Connection):
        async def connect(self) -> None:
            raise OSError("archive offline")

    first, second, recovered = OfflineConnection(), OfflineConnection(), _Connection()
    connections = iter([first, second, recovered])
    monkeypatch.setattr(
        "eveys_ocpp.clickhouse.read_client.Connection", lambda **_: next(connections)
    )
    client = _client(_Connection())
    client._conn = None
    try:
        await client.start()
    except OSError:
        pytest.fail("gateway startup attempted an archive connection")

    with pytest.raises(OSError, match="archive offline"):
        await client.fetch_latest_connector_statuses(cp_ids=["CP_TEST"])
    assert first.closed and second.closed

    result = await client.fetch_latest_connector_statuses(cp_ids=["CP_TEST"])

    assert result == {"CP_TEST": [{"connector_id": 1, "status": "Charging"}]}
