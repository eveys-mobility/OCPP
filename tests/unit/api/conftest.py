"""Shared fixtures for the inbound REST API tests.

Each test gets a fresh FastAPI app + an `httpx.AsyncClient` wired
via `ASGITransport` (in-process; no real socket). The session
factory and registry are stub objects whose methods are
`AsyncMock`-ed; tests overwrite the repository function imports
with `monkeypatch.setattr` per the existing handler-test pattern.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import pytest_asyncio

from eveys_ocpp.api._app import make_app
from eveys_ocpp.settings import Settings

# Single hard-coded inbound bearer token for these tests. Real shape
# (CSV) is exercised in `test_auth.py`.
TEST_TOKEN = "test-token-foundation"
AUTH_HEADER = {"Authorization": f"Bearer {TEST_TOKEN}"}


@pytest.fixture
def settings() -> Settings:
    return Settings(rest_inbound_tokens=TEST_TOKEN)


@pytest.fixture
def fake_session_factory() -> Any:
    """A no-op async context manager — the per-test monkeypatch
    swaps repository functions, so the session object's only job is
    to be enterable and exitable."""

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


@pytest.fixture
def fake_registry() -> MagicMock:
    """Stub Registry — only `get_pod` is called by the routes.

    Default: charger appears offline (`get_pod` returns None). Tests
    that want online state override it directly."""
    registry = MagicMock()
    registry.get_pod = AsyncMock(return_value=None)
    return registry


@pytest_asyncio.fixture
async def client(
    settings: Settings,
    fake_session_factory: Any,
    fake_registry: MagicMock,
) -> AsyncIterator[httpx.AsyncClient]:
    """In-process REST client. ASGITransport routes calls to the app
    without booting a socket."""
    app = make_app(
        session_factory=fake_session_factory,
        settings=settings,
        registry=fake_registry,
        redis=None,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gw",
        headers=AUTH_HEADER,
    ) as ac:
        yield ac
