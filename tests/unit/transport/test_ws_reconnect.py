"""A charger that reconnects while its old socket is still open.

Boots `serve_forever` on an ephemeral port with an in-memory database and
opens real WebSocket clients, like `test_ws_server_basic_auth.py`. The
registry is a stub so the presence calls can be counted.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import BigInteger, Integer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool
from websockets.asyncio.client import connect

from eveys_ocpp.connections import ConnectionMap
from eveys_ocpp.persistence.models import Base, ChargePoint
from eveys_ocpp.settings import Settings
from eveys_ocpp.transport.ws_server import OCPP_SUBPROTOCOL, serve_forever

CP_ID = "CP_RECONNECT"


def _free_port() -> int:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def _until(condition: Callable[[], bool], timeout: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


@pytest_asyncio.fixture
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    for table in Base.metadata.tables.values():
        for col in table.columns:
            if (
                col.primary_key
                and isinstance(col.type, BigInteger)
                and "sqlite" not in getattr(col.type, "_variant_mapping", {})
            ):
                col.type = col.type.with_variant(Integer(), "sqlite")  # type: ignore[assignment]
    engine = create_async_engine(
        "sqlite+aiosqlite:///file:ws_reconnect_mem?mode=memory&cache=shared&uri=true",
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        session.add(ChargePoint(cp_id=CP_ID, authorized_at=datetime.now(UTC)))
        await session.commit()
    yield factory
    await engine.dispose()


@pytest.fixture
def registry() -> MagicMock:
    stub = MagicMock()
    stub.mark_online = AsyncMock()
    stub.mark_offline = AsyncMock(return_value=True)
    stub.refresh = AsyncMock(return_value=True)
    stub.pop_offline_marker = AsyncMock(return_value=None)
    stub.record_offline_marker = AsyncMock()
    return stub


@pytest_asyncio.fixture
async def server(
    session_factory: async_sessionmaker[AsyncSession], registry: MagicMock
) -> AsyncIterator[tuple[int, ConnectionMap]]:
    port = _free_port()
    connections = ConnectionMap()
    task = asyncio.create_task(
        serve_forever(
            session_factory=session_factory,
            settings=Settings(ws_host="127.0.0.1", ws_port=port),
            pending_store=MagicMock(),
            registry=registry,
            connections=connections,
            ip_rate_limiter=None,
        )
    )
    await _until(lambda: _port_open(port))
    yield port, connections
    task.cancel()
    with contextlib.suppress(BaseException):
        await task


def _port_open(port: int) -> bool:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.settimeout(0.1)
        return s.connect_ex(("127.0.0.1", port)) == 0


async def _connect(port: int) -> Any:
    return await connect(f"ws://127.0.0.1:{port}/{CP_ID}", subprotocols=[OCPP_SUBPROTOCOL])


@pytest.mark.asyncio
async def test_reconnect_closes_the_replaced_socket(server: tuple[int, ConnectionMap]) -> None:
    port, connections = server
    old = await _connect(port)
    await _until(lambda: connections.get(CP_ID) is not None)
    old_cp = connections.get(CP_ID)

    new = await _connect(port)
    await _until(lambda: connections.get(CP_ID) is not old_cp)

    await asyncio.wait_for(old.wait_closed(), timeout=3)
    assert old.close_code == 1000
    assert old.close_reason == "replaced by a new connection"
    assert new.close_code is None
    await new.close()


@pytest.mark.asyncio
async def test_end_of_the_replaced_socket_does_not_mark_the_charger_offline(
    server: tuple[int, ConnectionMap], registry: MagicMock
) -> None:
    port, connections = server
    old = await _connect(port)
    await _until(lambda: connections.get(CP_ID) is not None)
    old_cp = connections.get(CP_ID)
    new = await _connect(port)
    await _until(lambda: connections.get(CP_ID) is not old_cp)

    await old.close()
    await asyncio.wait_for(old.wait_closed(), timeout=3)
    # Let the old connection's handler run its cleanup.
    await asyncio.sleep(0.3)

    registry.mark_offline.assert_not_awaited()
    assert connections.get(CP_ID) is not None

    await new.close()
    await _until(lambda: registry.mark_offline.await_count == 1)
    assert connections.get(CP_ID) is None
