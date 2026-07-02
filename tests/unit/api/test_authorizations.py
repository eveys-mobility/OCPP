"""Unit tests for the operator device-authorization surface.

Covers GET /authorizations, POST .../authorize, POST .../reject,
POST .../revoke — including the force-close of any live WS this pod
holds for the cp_id (the load-bearing security guarantee versus
"wait for next reconnect")."""

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


class _FakePendingStore:
    """In-memory stand-in for `PendingAuthorizations`.

    Backed by a dict so tests can seed rows, assert what `list_pending`
    sees, and check that `pop` / `remove` actually mutated state."""

    def __init__(self, rows: dict[str, dict[str, Any]] | None = None) -> None:
        self._rows: dict[str, dict[str, Any]] = dict(rows or {})

    async def list_pending(self) -> list[dict[str, Any]]:
        # Oldest-first, matching production semantics.
        return sorted(
            self._rows.values(),
            key=lambda r: r.get("first_seen_at", ""),
        )

    async def pop(self, cp_id: str) -> dict[str, Any] | None:
        return self._rows.pop(cp_id, None)

    async def remove(self, cp_id: str) -> bool:
        return self._rows.pop(cp_id, None) is not None


def _pending_row(cp_id: str, *, vendor: str = "Eveys", attempts: int = 1) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    return {
        "cp_id": cp_id,
        "first_seen_at": now,
        "last_seen_at": now,
        "peer_ip": "1.2.3.4",
        "user_agent": "ua/test",
        "vendor": vendor,
        "model": "Eveys-22kW",
        "firmware": "1.0.0",
        "serial_number": cp_id,
        "attempts": attempts,
    }


@pytest_asyncio.fixture
async def wired_app(
    settings: Settings,
    fake_session_factory: Any,
    fake_registry: MagicMock,
    fake_command_service: MagicMock,
    fake_ch_client: MagicMock,
) -> AsyncIterator[tuple[httpx.AsyncClient, _FakePendingStore, MagicMock]]:
    """REST client with a fake pending store + a fake ConnectionMap.

    Returns (client, pending_store, connections) so each test can
    seed the store and assert the WS close behaviour independently."""
    store = _FakePendingStore()
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
        pending_store=store,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gw",
        headers={"Authorization": f"Bearer {TEST_TOKEN}"},
    ) as ac:
        yield ac, store, connections


# ---- GET /authorizations ---------------------------------------------------


@pytest.mark.asyncio
async def test_list_returns_pending_rows(
    wired_app: tuple[httpx.AsyncClient, _FakePendingStore, MagicMock],
) -> None:
    ac, store, _ = wired_app
    store._rows["CP_A"] = _pending_row("CP_A")
    store._rows["CP_B"] = _pending_row("CP_B")

    response = await ac.get("/api/v1/authorizations")
    assert response.status_code == 200
    body = response.json()
    assert {r["cp_id"] for r in body["items"]} == {"CP_A", "CP_B"}


@pytest.mark.asyncio
async def test_list_empty_when_no_pending(
    wired_app: tuple[httpx.AsyncClient, _FakePendingStore, MagicMock],
) -> None:
    ac, _, _ = wired_app
    response = await ac.get("/api/v1/authorizations")
    assert response.status_code == 200
    assert response.json() == {"items": []}


# ---- POST /authorizations/{cp_id}/authorize -------------------------------


@pytest.mark.asyncio
async def test_authorize_moves_pending_to_charge_points(
    wired_app: tuple[httpx.AsyncClient, _FakePendingStore, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Authorize pops the pending row and seeds `charge_points` via
    `upsert_charge_point_boot` with the pending row's Boot metadata."""
    ac, store, _ = wired_app
    store._rows["CP_A"] = _pending_row("CP_A", vendor="Eveys")

    upsert = AsyncMock()
    monkeypatch.setattr(routes, "upsert_charge_point_boot", upsert)
    mark_authorized = AsyncMock(return_value=True)
    monkeypatch.setattr(routes, "mark_charge_point_authorized", mark_authorized)

    response = await ac.post("/api/v1/authorizations/CP_A/authorize")
    assert response.status_code == 200
    body = response.json()
    assert body["cp_id"] == "CP_A"
    assert body["status"] == "authorized"
    assert isinstance(body["authorized_at"], str)
    mark_authorized.assert_awaited_once()

    # Redis row is gone; seed hit the repo with the row's metadata.
    assert "CP_A" not in store._rows
    upsert.assert_awaited_once()
    kwargs = upsert.await_args.kwargs
    assert kwargs["cp_id"] == "CP_A"
    assert kwargs["vendor"] == "Eveys"
    assert kwargs["model"] == "Eveys-22kW"
    assert kwargs["firmware_version"] == "1.0.0"
    assert kwargs["serial_number"] == "CP_A"


@pytest.mark.asyncio
async def test_authorize_unknown_cp_id_404(
    wired_app: tuple[httpx.AsyncClient, _FakePendingStore, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ac, _, _ = wired_app
    monkeypatch.setattr(routes, "upsert_charge_point_boot", AsyncMock())
    monkeypatch.setattr(routes, "mark_charge_point_authorized", AsyncMock(return_value=True))

    response = await ac.post("/api/v1/authorizations/GHOST/authorize")
    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_authorize_closes_live_ws(
    wired_app: tuple[httpx.AsyncClient, _FakePendingStore, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pending authorize must force-close any live WS this pod is
    hosting for the cp_id so the charger reconnects and takes the
    authorized branch on its next upgrade."""
    ac, store, connections = wired_app
    store._rows["CP_A"] = _pending_row("CP_A")
    monkeypatch.setattr(routes, "upsert_charge_point_boot", AsyncMock())
    monkeypatch.setattr(routes, "mark_charge_point_authorized", AsyncMock(return_value=True))

    ws = MagicMock()
    ws.close = AsyncMock()
    cp = MagicMock()
    cp.connection = ws
    connections.get.return_value = cp

    response = await ac.post("/api/v1/authorizations/CP_A/authorize")
    assert response.status_code == 200
    ws.close.assert_awaited_once()
    args, _ = ws.close.await_args
    assert args[0] == 1008


@pytest.mark.asyncio
async def test_authorize_validates_cp_id_length(
    wired_app: tuple[httpx.AsyncClient, _FakePendingStore, MagicMock],
) -> None:
    """64-char ceiling matches the `charge_points.cp_id` column."""
    ac, _, _ = wired_app
    too_long = "x" * 65
    response = await ac.post(f"/api/v1/authorizations/{too_long}/authorize")
    assert response.status_code == 400


# ---- POST /authorizations/{cp_id}/reject ----------------------------------


@pytest.mark.asyncio
async def test_reject_removes_pending_row(
    wired_app: tuple[httpx.AsyncClient, _FakePendingStore, MagicMock],
) -> None:
    ac, store, _ = wired_app
    store._rows["CP_A"] = _pending_row("CP_A")

    response = await ac.post("/api/v1/authorizations/CP_A/reject")
    assert response.status_code == 200
    assert response.json() == {"cp_id": "CP_A", "status": "rejected"}
    assert "CP_A" not in store._rows


@pytest.mark.asyncio
async def test_reject_404_when_not_pending(
    wired_app: tuple[httpx.AsyncClient, _FakePendingStore, MagicMock],
) -> None:
    ac, _, _ = wired_app
    response = await ac.post("/api/v1/authorizations/GHOST/reject")
    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_reject_closes_live_ws(
    wired_app: tuple[httpx.AsyncClient, _FakePendingStore, MagicMock],
) -> None:
    ac, store, connections = wired_app
    store._rows["CP_A"] = _pending_row("CP_A")

    ws = MagicMock()
    ws.close = AsyncMock()
    cp = MagicMock()
    cp.connection = ws
    connections.get.return_value = cp

    response = await ac.post("/api/v1/authorizations/CP_A/reject")
    assert response.status_code == 200
    ws.close.assert_awaited_once()


# ---- POST /authorizations/{cp_id}/revoke ---------------------------------


@pytest.mark.asyncio
async def test_revoke_deletes_charge_point_row(
    wired_app: tuple[httpx.AsyncClient, _FakePendingStore, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ac, _, _ = wired_app
    delete = AsyncMock(return_value=True)
    monkeypatch.setattr(routes, "delete_charge_point", delete)

    response = await ac.post("/api/v1/authorizations/CP_A/revoke")
    assert response.status_code == 200
    assert response.json() == {"cp_id": "CP_A", "status": "revoked"}
    delete.assert_awaited_once()
    assert delete.await_args.kwargs["cp_id"] == "CP_A"


@pytest.mark.asyncio
async def test_revoke_404_when_row_missing(
    wired_app: tuple[httpx.AsyncClient, _FakePendingStore, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ac, _, _ = wired_app
    monkeypatch.setattr(routes, "delete_charge_point", AsyncMock(return_value=False))

    response = await ac.post("/api/v1/authorizations/GHOST/revoke")
    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_revoke_closes_live_ws(
    wired_app: tuple[httpx.AsyncClient, _FakePendingStore, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The load-bearing security guarantee: a revoke must force-close
    any live WS this pod holds, not wait for the next reconnect."""
    ac, _, connections = wired_app
    monkeypatch.setattr(routes, "delete_charge_point", AsyncMock(return_value=True))

    ws = MagicMock()
    ws.close = AsyncMock()
    cp = MagicMock()
    cp.connection = ws
    connections.get.return_value = cp

    response = await ac.post("/api/v1/authorizations/CP_A/revoke")
    assert response.status_code == 200
    ws.close.assert_awaited_once()
    args, _ = ws.close.await_args
    assert args[0] == 1008
    assert "revoked" in args[1].lower()


@pytest.mark.asyncio
async def test_revoke_no_live_ws_still_succeeds(
    wired_app: tuple[httpx.AsyncClient, _FakePendingStore, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Revoking a charger that's not currently connected is a clean
    200 — the DB delete is the durable effect."""
    ac, _, connections = wired_app
    monkeypatch.setattr(routes, "delete_charge_point", AsyncMock(return_value=True))
    connections.get.return_value = None

    response = await ac.post("/api/v1/authorizations/CP_A/revoke")
    assert response.status_code == 200


# Silence the unused-import warning on the bearer token constant.
_ = AUTH_HEADER
