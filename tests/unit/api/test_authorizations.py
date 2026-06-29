"""Unit tests for the operator device-authorization surface (#0013).

Covers GET /authorizations, POST .../approve, POST .../reject,
POST .../revoke (including the live-WS force-disconnect).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import pytest_asyncio

from eveys_ocpp.api import authorizations as routes
from eveys_ocpp.api._app import make_app
from eveys_ocpp.settings import Settings

from .conftest import AUTH_HEADER, TEST_TOKEN


def _row(*, cp_id: str, status: str) -> dict[str, Any]:
    """Repository row shape that the route's `_to_response` projects."""
    now = datetime.now(UTC)
    return {
        "cp_id": cp_id,
        "status": status,
        "requested_at": now,
        "decided_at": now if status != "pending" else None,
        "decided_by": "ops@example.com" if status != "pending" else None,
        "last_attempt_ip": "1.2.3.4",
        "last_attempt_user_agent": "ua/test",
        "last_attempt_at": now,
        "updated_at": now,
    }


@pytest_asyncio.fixture
async def client_with_connections(
    settings: Settings,
    fake_session_factory: Any,
    fake_registry: MagicMock,
    fake_command_service: MagicMock,
    fake_ch_client: MagicMock,
) -> AsyncIterator[tuple[httpx.AsyncClient, MagicMock]]:
    """REST client that exposes a stub ConnectionMap on app.state.
    Returns (client, connections_mock) so revoke tests can assert
    `close()` was called."""
    connections = MagicMock()
    connections.get = MagicMock(return_value=None)  # default: no live WS
    app = make_app(
        session_factory=fake_session_factory,
        settings=settings,
        registry=fake_registry,
        redis=None,
        command_service=fake_command_service,
        ch_client=fake_ch_client,
        connections=connections,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gw",
        headers={"Authorization": f"Bearer {TEST_TOKEN}"},
    ) as ac:
        yield ac, connections


# ---- GET /authorizations ---------------------------------------------------


@pytest.mark.asyncio
async def test_list_returns_items(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        routes,
        "list_authorizations",
        AsyncMock(return_value=[_row(cp_id="CP_001", status="pending")]),
    )
    response = await client.get("/api/v1/authorizations?status=pending")
    assert response.status_code == 200
    body = response.json()
    assert len(body["items"]) == 1
    assert body["items"][0]["cp_id"] == "CP_001"
    assert body["items"][0]["status"] == "pending"
    # ISO-8601 timestamps, not Python objects.
    assert isinstance(body["items"][0]["requested_at"], str)


@pytest.mark.asyncio
async def test_list_rejects_unknown_status(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/authorizations?status=nonsense")
    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


# ---- POST /authorizations/{cp_id}/approve ---------------------------------


@pytest.mark.asyncio
async def test_approve_flips_status(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    decide = AsyncMock(return_value=_row(cp_id="CP_001", status="approved"))
    monkeypatch.setattr(routes, "decide_authorization", decide)

    response = await client.post("/api/v1/authorizations/CP_001/approve?actor=ops@example.com")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "approved"
    decide.assert_awaited_once()
    kwargs = decide.await_args.kwargs
    assert kwargs["cp_id"] == "CP_001"
    assert kwargs["new_status"] == "approved"
    assert kwargs["decided_by"] == "ops@example.com"


@pytest.mark.asyncio
async def test_approve_unknown_cp_id_404(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(routes, "decide_authorization", AsyncMock(return_value=None))
    response = await client.post("/api/v1/authorizations/GHOST/approve")
    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_approve_validates_cp_id_length(client: httpx.AsyncClient) -> None:
    """64-char ceiling matches the `charge_points.cp_id` column."""
    too_long = "x" * 65
    response = await client.post(f"/api/v1/authorizations/{too_long}/approve")
    assert response.status_code == 400


# ---- POST /authorizations/{cp_id}/reject ---------------------------------


@pytest.mark.asyncio
async def test_reject_routes_to_repo(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    decide = AsyncMock(return_value=_row(cp_id="CP_001", status="rejected"))
    monkeypatch.setattr(routes, "decide_authorization", decide)

    response = await client.post("/api/v1/authorizations/CP_001/reject")
    assert response.status_code == 200
    assert response.json()["status"] == "rejected"
    assert decide.await_args.kwargs["new_status"] == "rejected"


# ---- POST /authorizations/{cp_id}/revoke ---------------------------------


@pytest.mark.asyncio
async def test_revoke_closes_live_ws(
    client_with_connections: tuple[httpx.AsyncClient, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A revoke must force-close any live WS this pod holds for the
    charger — that's the load-bearing security guarantee versus 'wait
    for next reconnect'."""
    ac, connections = client_with_connections
    decide = AsyncMock(return_value=_row(cp_id="CP_001", status="revoked"))
    monkeypatch.setattr(routes, "decide_authorization", decide)

    fake_connection = MagicMock()
    fake_connection.close = AsyncMock()
    fake_cp = MagicMock()
    fake_cp.connection = fake_connection
    connections.get.return_value = fake_cp

    response = await ac.post("/api/v1/authorizations/CP_001/revoke")
    assert response.status_code == 200
    assert response.json()["status"] == "revoked"

    fake_connection.close.assert_awaited_once()
    args, _ = fake_connection.close.await_args
    assert args[0] == 1008
    assert "revoked" in args[1].lower()


@pytest.mark.asyncio
async def test_revoke_no_live_ws_still_succeeds(
    client_with_connections: tuple[httpx.AsyncClient, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Revoking a charger that's not currently connected is a clean
    200 — the DB state change is the durable effect."""
    ac, connections = client_with_connections
    monkeypatch.setattr(
        routes,
        "decide_authorization",
        AsyncMock(return_value=_row(cp_id="CP_001", status="revoked")),
    )
    connections.get.return_value = None

    response = await ac.post("/api/v1/authorizations/CP_001/revoke")
    assert response.status_code == 200


# Silence the unused-import warning on the bearer token constant.
_ = AUTH_HEADER
