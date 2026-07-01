"""Unit tests for the operator webhook-backlog surface (E3-9 tail).

Covers all five routes:

* GET  /webhook-backlog                   list + filters + pagination
* GET  /webhook-backlog/{id}              single row + body_b64
* POST /webhook-backlog/{id}/replay       dead-only, 409 on live
* DEL  /webhook-backlog/{id}              dead-only, 409 on live
* POST /webhook-backlog/replay-dead       bulk with optional filter

Repository helpers are monkeypatched — the router logic is what's
under test here. End-to-end tests against a real Postgres live in
``tests/integration/test_webhook_backlog_roundtrip.py``.
"""

from __future__ import annotations

import base64
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from eveys_ocpp.api import webhook_backlog as routes


def _row(
    *,
    dead: bool = False,
    event_type: str = "cp.boot",
    attempts: int = 0,
    last_error: str | None = None,
    row_id: str | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    """Repository row shape that the router's `_row_to_response` projects."""
    now = datetime.now(UTC)
    return {
        "id": row_id or str(uuid4()),
        "event_id": event_id or str(uuid4()),
        "event_type": event_type,
        "url": "https://backend.example/webhooks/cp-boot",
        "signature": "sha256=deadbeef",
        "created_at": now,
        "next_attempt_at": now,
        "attempts": attempts,
        "last_error": last_error,
        "dead": dead,
    }


# ---- GET /webhook-backlog -------------------------------------------------


@pytest.mark.asyncio
async def test_list_returns_rows_and_null_next_cursor_when_short(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = [_row(dead=True), _row(dead=False)]
    monkeypatch.setattr(routes, "list_webhook_backlog", AsyncMock(return_value=rows))

    resp = await client.get("/api/v1/webhook-backlog")

    assert resp.status_code == 200
    body = resp.json()
    assert body["next_cursor"] is None
    assert len(body["rows"]) == 2
    assert body["rows"][0]["dead"] is True
    # `body` field is NOT in the list projection (kept small; single-row
    # endpoint is where operators fetch the payload).
    assert "body_b64" not in body["rows"][0]


@pytest.mark.asyncio
async def test_list_encodes_next_cursor_when_page_overflows(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The repo helper returns ``limit + 1`` rows so the router carves
    the last one off as the cursor sentinel."""
    fake_id = str(uuid4())
    rows = [_row(row_id=str(uuid4())) for _ in range(50)] + [_row(row_id=fake_id)]
    monkeypatch.setattr(routes, "list_webhook_backlog", AsyncMock(return_value=rows))

    resp = await client.get("/api/v1/webhook-backlog", params={"limit": 50})

    body = resp.json()
    assert len(body["rows"]) == 50
    assert body["next_cursor"] is not None
    # Round-trip check: the cursor decodes back to a JSON object with `id`.
    import base64 as _b64
    import json as _json

    decoded = _json.loads(_b64.urlsafe_b64decode(body["next_cursor"].encode()))
    assert decoded == {"id": fake_id}


@pytest.mark.asyncio
async def test_list_passes_dead_filter(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock = AsyncMock(return_value=[])
    monkeypatch.setattr(routes, "list_webhook_backlog", mock)

    await client.get("/api/v1/webhook-backlog", params={"dead": "true"})

    assert mock.await_args.kwargs["dead"] is True


@pytest.mark.asyncio
async def test_list_passes_event_type_filter_as_list(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock = AsyncMock(return_value=[])
    monkeypatch.setattr(routes, "list_webhook_backlog", mock)

    await client.get("/api/v1/webhook-backlog?event_type=cp.boot&event_type=tx.stopped")

    assert mock.await_args.kwargs["event_types"] == ["cp.boot", "tx.stopped"]


@pytest.mark.asyncio
async def test_list_rejects_bad_cursor(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(routes, "list_webhook_backlog", AsyncMock(return_value=[]))
    resp = await client.get("/api/v1/webhook-backlog", params={"cursor": "not-b64"})
    assert resp.status_code == 400


# ---- GET /webhook-backlog/{id} --------------------------------------------


@pytest.mark.asyncio
async def test_get_returns_body_b64(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    row_id = str(uuid4())
    row = _row(row_id=row_id, dead=True)
    body_bytes = b'{"data":{"event_type":"cp.boot"}}'
    monkeypatch.setattr(
        routes,
        "get_webhook_backlog_by_id",
        AsyncMock(return_value=(row, body_bytes)),
    )

    resp = await client.get(f"/api/v1/webhook-backlog/{row_id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == row_id
    assert body["body_b64"] == base64.b64encode(body_bytes).decode("ascii")


@pytest.mark.asyncio
async def test_get_returns_404_for_missing(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(routes, "get_webhook_backlog_by_id", AsyncMock(return_value=None))
    resp = await client.get(f"/api/v1/webhook-backlog/{uuid4()}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_returns_400_for_non_uuid_id(client: httpx.AsyncClient) -> None:
    resp = await client.get("/api/v1/webhook-backlog/not-a-uuid")
    assert resp.status_code == 400


# ---- POST /webhook-backlog/{id}/replay ------------------------------------


@pytest.mark.asyncio
async def test_replay_marks_row_live(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    row_id = str(uuid4())
    dead_row = _row(row_id=row_id, dead=True, last_error="http_503")
    live_row = _row(row_id=row_id, dead=False)
    monkeypatch.setattr(
        routes,
        "get_webhook_backlog_by_id",
        AsyncMock(return_value=(dead_row, b"{}")),
    )
    mock_resurrect = AsyncMock(return_value=live_row)
    monkeypatch.setattr(routes, "resurrect_webhook_backlog", mock_resurrect)

    resp = await client.post(f"/api/v1/webhook-backlog/{row_id}/replay")

    assert resp.status_code == 200
    assert resp.json()["dead"] is False
    mock_resurrect.assert_awaited_once()


@pytest.mark.asyncio
async def test_replay_refuses_live_row(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    row_id = str(uuid4())
    live_row = _row(row_id=row_id, dead=False)
    monkeypatch.setattr(
        routes,
        "get_webhook_backlog_by_id",
        AsyncMock(return_value=(live_row, b"{}")),
    )
    mock_resurrect = AsyncMock()
    monkeypatch.setattr(routes, "resurrect_webhook_backlog", mock_resurrect)

    resp = await client.post(f"/api/v1/webhook-backlog/{row_id}/replay")

    assert resp.status_code == 409
    mock_resurrect.assert_not_awaited()


@pytest.mark.asyncio
async def test_replay_returns_404_when_missing(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(routes, "get_webhook_backlog_by_id", AsyncMock(return_value=None))
    resp = await client.post(f"/api/v1/webhook-backlog/{uuid4()}/replay")
    assert resp.status_code == 404


# ---- DELETE /webhook-backlog/{id} -----------------------------------------


@pytest.mark.asyncio
async def test_purge_deletes_dead_row(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    row_id = str(uuid4())
    dead_row = _row(row_id=row_id, dead=True)
    monkeypatch.setattr(
        routes,
        "get_webhook_backlog_by_id",
        AsyncMock(return_value=(dead_row, b"{}")),
    )
    mock_purge = AsyncMock(return_value=True)
    monkeypatch.setattr(routes, "purge_webhook_backlog", mock_purge)

    resp = await client.delete(f"/api/v1/webhook-backlog/{row_id}")

    assert resp.status_code == 200
    assert resp.json() == {"deleted": True, "id": row_id}
    mock_purge.assert_awaited_once()


@pytest.mark.asyncio
async def test_purge_refuses_live_row(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    row_id = str(uuid4())
    live_row = _row(row_id=row_id, dead=False)
    monkeypatch.setattr(
        routes,
        "get_webhook_backlog_by_id",
        AsyncMock(return_value=(live_row, b"{}")),
    )
    mock_purge = AsyncMock()
    monkeypatch.setattr(routes, "purge_webhook_backlog", mock_purge)

    resp = await client.delete(f"/api/v1/webhook-backlog/{row_id}")

    assert resp.status_code == 409
    mock_purge.assert_not_awaited()


# ---- POST /webhook-backlog/replay-dead ------------------------------------


@pytest.mark.asyncio
async def test_bulk_replay_without_filter_calls_repo_with_none(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock = AsyncMock(return_value=7)
    monkeypatch.setattr(routes, "resurrect_dead_webhook_backlog", mock)

    resp = await client.post("/api/v1/webhook-backlog/replay-dead")

    assert resp.status_code == 200
    assert resp.json() == {"count": 7}
    assert mock.await_args.kwargs["event_types"] is None


@pytest.mark.asyncio
async def test_bulk_replay_with_event_type_filter(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock = AsyncMock(return_value=2)
    monkeypatch.setattr(routes, "resurrect_dead_webhook_backlog", mock)

    resp = await client.post(
        "/api/v1/webhook-backlog/replay-dead",
        json={"event_type": ["cp.boot"]},
    )

    assert resp.status_code == 200
    assert resp.json() == {"count": 2}
    assert mock.await_args.kwargs["event_types"] == ["cp.boot"]


# ---- POST /webhook-backlog/purge-dead -------------------------------------


@pytest.mark.asyncio
async def test_bulk_purge_without_filter_calls_repo_with_none(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock = AsyncMock(return_value=5)
    monkeypatch.setattr(routes, "purge_dead_webhook_backlog", mock)

    resp = await client.post("/api/v1/webhook-backlog/purge-dead")

    assert resp.status_code == 200
    assert resp.json() == {"count": 5}
    assert mock.await_args.kwargs["event_types"] is None


@pytest.mark.asyncio
async def test_bulk_purge_with_event_type_filter(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock = AsyncMock(return_value=1)
    monkeypatch.setattr(routes, "purge_dead_webhook_backlog", mock)

    resp = await client.post(
        "/api/v1/webhook-backlog/purge-dead",
        json={"event_type": ["tx.stopped"]},
    )

    assert resp.status_code == 200
    assert resp.json() == {"count": 1}
    assert mock.await_args.kwargs["event_types"] == ["tx.stopped"]


# ---- Auth guard -----------------------------------------------------------


@pytest.mark.asyncio
async def test_list_requires_bearer(
    settings: Any,
    fake_session_factory: Any,
    fake_registry: Any,
    fake_command_service: Any,
    fake_ch_client: Any,
) -> None:
    """No Authorization header -> 401 from the middleware."""
    from eveys_ocpp.api._app import make_app

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
    ) as ac:
        resp = await ac.get("/api/v1/webhook-backlog")

    assert resp.status_code == 401
