"""Tests for `GET /api/v1/ready` and the drain controller."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import pytest_asyncio

from eveys_ocpp.api._app import make_app
from eveys_ocpp.settings import Settings
from eveys_ocpp.shutdown import DrainController


@pytest_asyncio.fixture
async def client_with_controller(
    settings: Settings,
    fake_session_factory: Any,
    fake_registry: MagicMock,
) -> AsyncIterator[tuple[httpx.AsyncClient, DrainController]]:
    """REST client wired with a real `DrainController` so tests can
    flip drain mode and assert the readiness response shape."""
    controller = DrainController()
    app = make_app(
        session_factory=fake_session_factory,
        settings=settings,
        registry=fake_registry,
        redis=None,
        drain_controller=controller,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gw",
    ) as ac:
        yield ac, controller


@pytest.mark.asyncio
async def test_ready_returns_200_when_not_draining(
    client_with_controller: tuple[httpx.AsyncClient, DrainController],
) -> None:
    ac, _ = client_with_controller

    response = await ac.get("/api/v1/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["request_id"]


@pytest.mark.asyncio
async def test_ready_returns_503_when_draining(
    client_with_controller: tuple[httpx.AsyncClient, DrainController],
) -> None:
    ac, controller = client_with_controller
    controller.begin_drain()

    response = await ac.get("/api/v1/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "draining"
    # `draining_for_seconds` is reported once a drain has begun. It
    # may be 0.0 on the first poll — assert the key, not a positive
    # value, so the test stays stable on fast hardware.
    assert "draining_for_seconds" in body


@pytest.mark.asyncio
async def test_ready_is_auth_exempt() -> None:
    """The LB's readiness probe doesn't carry a bearer token, so
    `/ready` must answer regardless of auth state. We build a fresh
    app with auth enabled and an empty token allowlist (the
    "production safe-by-default" mode that 401s every authed call)
    to prove `/ready` still responds.
    """
    settings = Settings(rest_inbound_tokens="", rest_auth_disabled=False)
    controller = DrainController()
    registry = MagicMock()
    registry.get_pod = AsyncMock(return_value=None)

    class _Session:
        async def __aenter__(self) -> _Session:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        execute = AsyncMock()

    class _Factory:
        def __call__(self) -> _Session:
            return _Session()

    app = make_app(
        session_factory=_Factory(),
        settings=settings,
        registry=registry,
        redis=None,
        drain_controller=controller,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gw",
    ) as ac:
        # No Authorization header at all.
        response = await ac.get("/api/v1/ready")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "ready"


@pytest.mark.asyncio
async def test_ready_falls_back_to_ready_when_controller_absent(
    settings: Settings,
    fake_session_factory: Any,
    fake_registry: MagicMock,
) -> None:
    """Apps built without a `DrainController` (unit-test rigs that
    skip the shutdown lifecycle) treat the absent controller as
    'never draining' rather than failing closed."""
    app = make_app(
        session_factory=fake_session_factory,
        settings=settings,
        registry=fake_registry,
        redis=None,
        drain_controller=None,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gw",
    ) as ac:
        response = await ac.get("/api/v1/ready")

    assert response.status_code == 200
    assert response.json()["status"] == "ready"
