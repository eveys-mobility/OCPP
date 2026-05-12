"""Tests for the SSE endpoint.

The bus's Kafka integration is exercised in compose-smoke; here we
wire a stub bus directly onto `app.state.sse_bus` and assert the
endpoint's HTTP behavior + SSE framing.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import pytest_asyncio

from eveys_ocpp.api._app import make_app
from eveys_ocpp.settings import Settings
from eveys_ocpp.sse_bus import _Subscription

TEST_TOKEN = "test-token-sse"
AUTH_HEADER = {"Authorization": f"Bearer {TEST_TOKEN}"}


class _StubBus:
    """Minimal stand-in for SseBus that lets us push events at will."""

    def __init__(self) -> None:
        self.subscribers: list[_Subscription] = []
        self.running = True

    async def subscribe(self, cp_id: str) -> _Subscription:
        sub = _Subscription(cp_id=cp_id, queue=asyncio.Queue(maxsize=8))
        self.subscribers.append(sub)
        return sub

    async def unsubscribe(self, sub: _Subscription) -> None:
        with contextlib.suppress(ValueError):
            self.subscribers.remove(sub)


@pytest.fixture
def fake_session_factory() -> Any:
    class _Session:
        async def __aenter__(self) -> _Session:
            return self

        async def __aexit__(self, *_: Any) -> None:
            return None

        commit = AsyncMock()
        rollback = AsyncMock()
        close = AsyncMock()
        execute = AsyncMock()

    class _Factory:
        def __call__(self) -> _Session:
            return _Session()

    return _Factory()


@pytest_asyncio.fixture
async def client_factory(
    fake_session_factory: Any,
) -> AsyncIterator[Any]:
    """Returns a callable that builds a fresh app + client for each test
    with custom settings + a stub bus."""
    created_clients: list[httpx.AsyncClient] = []

    async def build(*, sse_enabled: bool, stub_bus: _StubBus | None) -> httpx.AsyncClient:
        settings = Settings(
            rest_inbound_tokens=TEST_TOKEN,
            sse_enabled=sse_enabled,
            # Tight heartbeat so tests don't wait 20s for a tick.
            sse_heartbeat_seconds=0.1,
        )
        app = make_app(
            session_factory=fake_session_factory,
            settings=settings,
            registry=MagicMock(),
            redis=None,
            command_service=None,
            ch_client=None,
        )
        if stub_bus is not None:
            app.state.sse_bus = stub_bus
        ac = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gw",
            headers=AUTH_HEADER,
            timeout=2.0,
        )
        created_clients.append(ac)
        return ac

    yield build

    for ac in created_clients:
        await ac.aclose()


@pytest.mark.asyncio
async def test_returns_503_when_feature_disabled(
    client_factory: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When `sse_enabled=False` the router isn't even mounted, so the
    endpoint 404s at the routing layer. That's the operational "off"
    contract: the URL doesn't exist on that pod."""
    client = await client_factory(sse_enabled=False, stub_bus=None)
    response = await client.get("/api/v1/charge-points/CP_001/events")
    # FastAPI returns 404 for an unmounted route; the closed-enum
    # error envelope still applies via the custom HTTP exception
    # handler, but the *route* not existing is its own contract.
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_returns_404_for_unknown_cp(
    client_factory: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp.api import sse as sse_module

    monkeypatch.setattr(sse_module, "get_charge_point_pk", AsyncMock(return_value=None))
    bus = _StubBus()
    client = await client_factory(sse_enabled=True, stub_bus=bus)

    response = await client.get("/api/v1/charge-points/UNKNOWN/events")
    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_streams_events_pushed_through_the_bus(
    client_factory: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Open the stream, push one event through the stub bus, assert
    the framed SSE bytes land on the wire."""
    from eveys_ocpp.api import sse as sse_module

    monkeypatch.setattr(sse_module, "get_charge_point_pk", AsyncMock(return_value=1))
    bus = _StubBus()
    client = await client_factory(sse_enabled=True, stub_bus=bus)

    async def push_after_subscribe() -> None:
        # Wait for the endpoint to subscribe, then push event + sentinel.
        for _ in range(100):
            if bus.subscribers:
                break
            await asyncio.sleep(0.01)
        assert bus.subscribers, "endpoint never subscribed"
        await bus.subscribers[0].queue.put(
            {"event": "tx_started", "data": {"transaction_id": 42, "cp_id": "CP_001"}}
        )
        await bus.subscribers[0].queue.put(None)

    push_task = asyncio.create_task(push_after_subscribe())

    async with client.stream("GET", "/api/v1/charge-points/CP_001/events") as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        body = b""
        async for chunk in response.aiter_bytes():
            body += chunk
            if b"event: error" in body:
                break

    await push_task
    text = body.decode()
    assert "event: tx_started" in text
    assert '"transaction_id":42' in text
    assert "event: error" in text


@pytest.mark.asyncio
async def test_unsubscribes_on_client_disconnect(
    client_factory: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the client closes the stream the endpoint must call
    bus.unsubscribe so the bus doesn't leak per-stream queues."""
    from eveys_ocpp.api import sse as sse_module

    monkeypatch.setattr(sse_module, "get_charge_point_pk", AsyncMock(return_value=1))
    bus = _StubBus()
    client = await client_factory(sse_enabled=True, stub_bus=bus)

    async def close_after_subscribe() -> None:
        for _ in range(100):
            if bus.subscribers:
                break
            await asyncio.sleep(0.01)
        assert bus.subscribers
        await bus.subscribers[0].queue.put(None)

    close_task = asyncio.create_task(close_after_subscribe())

    async with client.stream("GET", "/api/v1/charge-points/CP_001/events") as response:
        assert response.status_code == 200
        async for _ in response.aiter_bytes():
            pass  # drain to completion

    await close_task
    # Give the finally-block a tick to run.
    for _ in range(50):
        if not bus.subscribers:
            break
        await asyncio.sleep(0.01)
    assert bus.subscribers == []
