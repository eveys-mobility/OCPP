"""Tests for `POST /api/v1/admin/restart` — the config-reload helper.

Three slices:

1. Default-off behaviour: even with the admin token, the endpoint refuses.
2. Enabled behaviour: returns 202 and schedules a SIGTERM. The SIGTERM
   itself is mocked so the test process doesn't die mid-suite.
3. Debounce: a second call inside the window does NOT schedule a second
   exit but still returns 202 so the operator's UI doesn't surface a
   confusing failure.

The actual `os.kill(getpid(), SIGTERM)` is patched in every test —
otherwise pytest would die when the route fires it. The patching is at
the `_delayed_sigterm` boundary so we also assert the scheduler was
called (or wasn't).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import pytest_asyncio

from eveys_ocpp.api._app import make_app
from eveys_ocpp.api.admin import _reset_restart_debounce_for_tests
from eveys_ocpp.settings import Settings


@pytest.fixture(autouse=True)
def reset_debounce() -> Iterator[None]:
    """Module-level debounce state is shared across tests in the same pod
    (which the test runner emulates). Clear before + after so neighbours
    can't taint each other."""
    _reset_restart_debounce_for_tests()
    yield
    _reset_restart_debounce_for_tests()


@pytest.fixture(autouse=True)
def mock_sigterm(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    """Replace `_delayed_sigterm` with an AsyncMock that records calls but
    doesn't actually kill the test process. Returning the mock lets tests
    introspect call args."""
    mock = AsyncMock()
    monkeypatch.setattr("eveys_ocpp.api.admin._delayed_sigterm", mock)
    return mock


def _make_client_with_settings(
    settings: Settings,
    fake_session_factory: Any,
    fake_registry: MagicMock,
    fake_command_service: MagicMock,
    fake_ch_client: MagicMock,
) -> httpx.AsyncClient:
    app = make_app(
        session_factory=fake_session_factory,
        settings=settings,
        registry=fake_registry,
        redis=None,
        command_service=fake_command_service,
        ch_client=fake_ch_client,
    )
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gw",
        headers={"Authorization": "Bearer test-token-foundation"},
    )


@pytest_asyncio.fixture
async def client_disabled(
    settings: Settings,
    fake_session_factory: Any,
    fake_registry: MagicMock,
    fake_command_service: MagicMock,
    fake_ch_client: MagicMock,
) -> AsyncIterator[httpx.AsyncClient]:
    """Default Settings — `admin_restart_enabled` is False."""
    async with _make_client_with_settings(
        settings, fake_session_factory, fake_registry, fake_command_service, fake_ch_client
    ) as c:
        yield c


@pytest_asyncio.fixture
async def client_enabled(
    settings_factory: Any,
    fake_session_factory: Any,
    fake_registry: MagicMock,
    fake_command_service: MagicMock,
    fake_ch_client: MagicMock,
) -> AsyncIterator[httpx.AsyncClient]:
    """Settings with `admin_restart_enabled=True` and a short debounce so
    the debounce-replay test doesn't have to sleep 5 real seconds."""
    # rest_inbound_tokens has to be carried explicitly — the unit/api
    # `settings` fixture wires it in, but `settings_factory` builds a
    # fresh Settings from scratch.
    settings = settings_factory(
        rest_inbound_tokens="test-token-foundation",
        admin_restart_enabled=True,
        admin_restart_debounce_seconds=0.1,
    )
    async with _make_client_with_settings(
        settings, fake_session_factory, fake_registry, fake_command_service, fake_ch_client
    ) as c:
        yield c


@pytest.mark.asyncio
async def test_returns_503_when_disabled(
    client_disabled: httpx.AsyncClient,
    mock_sigterm: AsyncMock,
) -> None:
    response = await client_disabled.post("/api/v1/admin/restart")
    assert response.status_code == 503
    body = response.json()
    assert body["error_code"] == "SERVICE_UNAVAILABLE"
    assert "EVEYS_OCPP_ADMIN_RESTART_ENABLED" in body["error"]
    mock_sigterm.assert_not_called()


@pytest.mark.asyncio
async def test_returns_202_and_schedules_sigterm_when_enabled(
    client_enabled: httpx.AsyncClient,
    mock_sigterm: AsyncMock,
) -> None:
    response = await client_enabled.post("/api/v1/admin/restart")
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "scheduled"
    assert body["exits_in_ms"] == 500
    assert body["scope"] == "per-pod"
    # The scheduler was armed exactly once with the documented 0.5s delay.
    mock_sigterm.assert_called_once_with(0.5)


@pytest.mark.asyncio
async def test_debounce_returns_already_scheduled_without_second_sigterm(
    client_enabled: httpx.AsyncClient,
    mock_sigterm: AsyncMock,
) -> None:
    """A second POST inside the debounce window still returns 202 (no
    spurious failure for the operator) but does NOT schedule another
    SIGTERM. The debounce keeps a double-click from queuing two exits."""
    first = await client_enabled.post("/api/v1/admin/restart")
    assert first.status_code == 202
    assert first.json()["status"] == "scheduled"

    # Second call inside the 0.1s window (immediate; the test runs in
    # microseconds).
    second = await client_enabled.post("/api/v1/admin/restart")
    assert second.status_code == 202
    assert second.json()["status"] == "already_scheduled"
    assert second.json()["exits_in_ms"] == 0

    # Only the first call armed a SIGTERM.
    assert mock_sigterm.call_count == 1


@pytest.mark.asyncio
async def test_debounce_window_expires_allows_new_restart(
    client_enabled: httpx.AsyncClient,
    mock_sigterm: AsyncMock,
) -> None:
    """After the debounce window passes, a fresh restart is honoured.
    This guards against a stuck debounce that locks out future restarts
    after the gateway didn't actually die (e.g. operator hit Cancel)."""
    import asyncio

    first = await client_enabled.post("/api/v1/admin/restart")
    assert first.json()["status"] == "scheduled"

    # Wait past the 0.1s debounce.
    await asyncio.sleep(0.15)

    second = await client_enabled.post("/api/v1/admin/restart")
    assert second.json()["status"] == "scheduled"
    assert mock_sigterm.call_count == 2


@pytest.mark.asyncio
async def test_requires_auth(
    settings_factory: Any,
    fake_session_factory: Any,
    fake_registry: MagicMock,
    fake_command_service: MagicMock,
    fake_ch_client: MagicMock,
    mock_sigterm: AsyncMock,
) -> None:
    """Existing admin-token middleware applies — no token means 401, no
    SIGTERM. The endpoint inherits this gate from the `/api/v1/*` mount
    point; this test pins that it didn't accidentally get an `auth=False`
    override."""
    settings = settings_factory(
        rest_inbound_tokens="test-token-foundation",
        admin_restart_enabled=True,
    )
    app = make_app(
        session_factory=fake_session_factory,
        settings=settings,
        registry=fake_registry,
        redis=None,
        command_service=fake_command_service,
        ch_client=fake_ch_client,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gw",
        # NO Authorization header.
    ) as c:
        response = await c.post("/api/v1/admin/restart")
    assert response.status_code == 401
    mock_sigterm.assert_not_called()
