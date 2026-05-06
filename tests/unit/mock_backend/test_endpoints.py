"""Unit tests for the mock Eveys backend (E3-10).

The mock is dev-only — these tests verify that every endpoint
returns the canonical envelope shape the gateway will consume,
that auth + idempotency work, and that the behaviour controls
(blocked id_tags, forced 503) do what they advertise.

Each test builds a fresh FastAPI app with an explicit config so
runs don't share idempotency-cache state.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from tests.mock_backend import build_app
from tests.mock_backend.config import MockBackendConfig

_TOKEN = "test-token"
_AUTH_HEADER = {"Authorization": f"Bearer {_TOKEN}"}


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    app = build_app(MockBackendConfig(bearer_token=_TOKEN))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://mock",
    ) as ac:
        yield ac


# ---- Envelope shape -------------------------------------------------------


@pytest.mark.asyncio
async def test_health_returns_envelope_with_ok_status(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/eveys/health")
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["status"] == "ok"
    assert "version" in body["data"]
    assert body["message"] == "ok"


@pytest.mark.asyncio
async def test_health_echoes_request_id_in_body_and_header(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/eveys/health", headers={"X-Request-ID": "rid-abc-123"})
    assert response.headers["x-request-id"] == "rid-abc-123"
    assert response.json()["data"]["request_id"] == "rid-abc-123"


# ---- Auth -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_authorize_rejects_missing_bearer(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/eveys/authorize",
        json={"id_tag": "RFID_X", "cp_id": "CP_001"},
    )
    assert response.status_code == 401
    body = response.json()
    assert body["success"] is False
    assert body["error_code"] == "UNAUTHORIZED"


@pytest.mark.asyncio
async def test_authorize_rejects_wrong_token(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/eveys/authorize",
        json={"id_tag": "RFID_X", "cp_id": "CP_001"},
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert response.status_code == 401


# ---- Authorize semantics --------------------------------------------------


@pytest.mark.asyncio
async def test_authorize_accepts_unknown_id_tag_by_default(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post(
        "/api/eveys/authorize",
        json={"id_tag": "RFID_HAPPY", "cp_id": "CP_001"},
        headers=_AUTH_HEADER,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["id_tag_info"]["status"] == "Accepted"
    assert body["data"]["id_tag"] == "RFID_HAPPY"


@pytest.mark.asyncio
async def test_authorize_returns_blocked_for_configured_id_tag() -> None:
    """`MockBackendConfig.blocked_id_tags` controls the rejection
    set without touching env vars; lets every test stand alone."""
    app = build_app(
        MockBackendConfig(
            bearer_token=_TOKEN,
            blocked_id_tags=frozenset({"RFID_BLOCKED"}),
        )
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://mock"
    ) as ac:
        response = await ac.post(
            "/api/eveys/authorize",
            json={"id_tag": "RFID_BLOCKED", "cp_id": "CP_001"},
            headers=_AUTH_HEADER,
        )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["id_tag_info"]["status"] == "Blocked"
    assert body["message"] == "id_tag is blocked"


@pytest.mark.asyncio
async def test_authorize_force_accept_all_overrides_blocked_list() -> None:
    """`force_accept_all` short-circuits the blocked check — useful for
    happy-path test runs that want every id_tag to succeed."""
    app = build_app(
        MockBackendConfig(
            bearer_token=_TOKEN,
            blocked_id_tags=frozenset({"RFID_BLOCKED"}),
            force_accept_all=True,
        )
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://mock"
    ) as ac:
        response = await ac.post(
            "/api/eveys/authorize",
            json={"id_tag": "RFID_BLOCKED", "cp_id": "CP_001"},
            headers=_AUTH_HEADER,
        )

    assert response.json()["data"]["id_tag_info"]["status"] == "Accepted"


@pytest.mark.asyncio
async def test_authorize_returns_503_when_fail_authorize_set() -> None:
    """`fail_authorize=True` simulates the backend's auth subsystem
    being down — used to test the gateway's circuit breaker."""
    app = build_app(MockBackendConfig(bearer_token=_TOKEN, fail_authorize=True))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://mock"
    ) as ac:
        response = await ac.post(
            "/api/eveys/authorize",
            json={"id_tag": "RFID_X", "cp_id": "CP_001"},
            headers=_AUTH_HEADER,
        )

    assert response.status_code == 503
    body = response.json()
    assert body["success"] is False
    assert body["error_code"] == "DB_UNAVAILABLE"


# ---- Sessions -------------------------------------------------------------


@pytest.mark.asyncio
async def test_sessions_open_returns_accepted(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/eveys/sessions/open",
        json={
            "transaction_id": 12345,
            "cp_id": "CP_001",
            "connector_id": 1,
            "id_tag": "RFID_VIP",
            "meter_start_wh": 4500000,
            "started_reported_at": "2026-05-05T14:32:11.847+00:00",
        },
        headers=_AUTH_HEADER,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["transaction_id"] == 12345
    assert body["data"]["id_tag_info"]["status"] == "Accepted"
    # `command_id` is populated and stable for the same input.
    assert isinstance(body["data"]["command_id"], int)


@pytest.mark.asyncio
async def test_sessions_close_returns_accepted(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/eveys/sessions/close",
        json={
            "transaction_id": 12345,
            "cp_id": "CP_001",
            "id_tag": "RFID_VIP",
            "meter_stop_wh": 4523500,
            "stopped_reported_at": "2026-05-05T15:14:30.012+00:00",
            "stop_reason": "Local",
        },
        headers=_AUTH_HEADER,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["transaction_id"] == 12345
    assert body["data"]["id_tag_info"]["status"] == "Accepted"


# ---- Charge-point register ------------------------------------------------


@pytest.mark.asyncio
async def test_charge_point_register_returns_heartbeat_interval() -> None:
    """The gateway uses this number as the OCPP `BootNotification.conf
    .interval` value — making sure it's configurable per app."""
    app = build_app(MockBackendConfig(bearer_token=_TOKEN, heartbeat_interval_seconds=120))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://mock"
    ) as ac:
        response = await ac.post(
            "/api/eveys/charge-points/register",
            json={
                "cp_id": "CP_NEW",
                "vendor": "ACME",
                "model": "X1",
                "boot_at": "2026-05-05T14:00:00+00:00",
            },
            headers=_AUTH_HEADER,
        )

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["registration_status"] == "Accepted"
    assert body["data"]["heartbeat_interval_seconds"] == 120


# ---- Idempotency ----------------------------------------------------------


@pytest.mark.asyncio
async def test_idempotency_replay_returns_same_response(
    client: httpx.AsyncClient,
) -> None:
    """Same key + same body → second call returns the cached
    response (not a fresh one)."""
    body = {
        "transaction_id": 99,
        "cp_id": "CP_IDEM",
        "connector_id": 1,
        "id_tag": "RFID_X",
        "meter_start_wh": 0,
        "started_reported_at": "2026-05-05T14:00:00+00:00",
    }
    headers = {**_AUTH_HEADER, "Idempotency-Key": "open-99"}

    first = await client.post("/api/eveys/sessions/open", json=body, headers=headers)
    second = await client.post("/api/eveys/sessions/open", json=body, headers=headers)

    assert first.status_code == 200
    assert second.status_code == 200
    # The cached response is byte-equal — including the same command_id.
    assert first.json() == second.json()


@pytest.mark.asyncio
async def test_idempotency_conflict_when_body_differs(
    client: httpx.AsyncClient,
) -> None:
    """Same key + different body → 409 Conflict per the contract."""
    headers = {**_AUTH_HEADER, "Idempotency-Key": "auth-key-1"}

    first = await client.post(
        "/api/eveys/authorize",
        json={"id_tag": "RFID_A", "cp_id": "CP_001"},
        headers=headers,
    )
    second = await client.post(
        "/api/eveys/authorize",
        json={"id_tag": "RFID_B", "cp_id": "CP_001"},  # different body
        headers=headers,
    )

    assert first.status_code == 200
    assert second.status_code == 409
    assert second.json()["error_code"] == "IDEMPOTENCY_CONFLICT"


@pytest.mark.asyncio
async def test_no_idempotency_key_means_no_caching(
    client: httpx.AsyncClient,
) -> None:
    """Without an Idempotency-Key, each call gets a fresh request_id
    (the cache is never consulted)."""
    body = {"id_tag": "RFID_X", "cp_id": "CP_001"}
    first = await client.post("/api/eveys/authorize", json=body, headers=_AUTH_HEADER)
    second = await client.post("/api/eveys/authorize", json=body, headers=_AUTH_HEADER)
    assert first.json()["data"]["request_id"] != second.json()["data"]["request_id"]
