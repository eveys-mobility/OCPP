"""Route tests for /api/v1/auth/login + /api/v1/auth/logout (issue
#84 PR-A).

The pre-existing `test_auth.py` covers the bearer-token middleware
contract (ADR-0026 D3). This file is the new login-flow surface."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import pytest_asyncio

from eveys_ocpp.api._app import make_app
from eveys_ocpp.auth import hash_password
from eveys_ocpp.persistence.models import User
from eveys_ocpp.settings import Settings

# ---- Fixtures --------------------------------------------------------------


@pytest.fixture
def fake_redis_kv() -> MagicMock:
    """Tiny in-memory dict satisfying the redis.asyncio surface auth
    uses (`set` / `get` / `delete`). Avoids fakeredis-as-a-dep and
    avoids real Redis."""
    store: dict[str, str] = {}

    async def _set(key: str, value: str, ex: int | None = None) -> None:
        store[key] = value

    async def _get(key: str) -> str | None:
        return store.get(key)

    async def _delete(key: str) -> int:
        return 1 if store.pop(key, None) is not None else 0

    redis = MagicMock()
    redis.set = AsyncMock(side_effect=_set)
    redis.get = AsyncMock(side_effect=_get)
    redis.delete = AsyncMock(side_effect=_delete)
    redis._store = store
    return redis


@pytest.fixture
def fake_user_session_factory() -> Any:
    """A `session_factory` whose session yields user `alice` for
    `select(User).where(User.username == 'alice')` and None for any
    other username. The auth core's only DB call is that one shape."""
    user = User(
        id=42,
        username="alice",
        password_hash=hash_password("hunter2"),
        expires_at=None,
        webhook_url=None,
    )

    class _Result:
        def __init__(self, value: Any) -> None:
            self._value = value

        def scalar_one_or_none(self) -> Any:
            return self._value

    class _Session:
        async def __aenter__(self) -> _Session:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def execute(self, stmt: Any) -> _Result:
            text = str(stmt.compile(compile_kwargs={"literal_binds": True}))
            if "'alice'" in text:
                return _Result(user)
            return _Result(None)

    class _Factory:
        def __call__(self) -> _Session:
            return _Session()

    return _Factory()


@pytest_asyncio.fixture
async def client_with_redis(
    fake_user_session_factory: Any,
    fake_redis_kv: MagicMock,
) -> AsyncIterator[tuple[httpx.AsyncClient, MagicMock]]:
    """Auth-flow tests need a real-shaped redis. The default conftest
    `client` fixture wires `redis=None`."""
    settings = Settings(
        rest_inbound_tokens="static-svc-token",
        # Superadmin pre-configured for env-only login tests.
        superadmin_username="root",
        superadmin_password_hash=hash_password("rootpass"),  # type: ignore[arg-type]
    )
    registry = MagicMock()
    registry.get_pod = AsyncMock(return_value=None)

    app = make_app(
        session_factory=fake_user_session_factory,
        settings=settings,
        registry=registry,
        redis=fake_redis_kv,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gw",
    ) as ac:
        yield ac, fake_redis_kv


# ---- Login -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_login_with_correct_user_password_issues_token(
    client_with_redis: tuple[httpx.AsyncClient, MagicMock],
) -> None:
    client, redis = client_with_redis
    response = await client.post(
        "/api/v1/auth/login",
        json={"username": "alice", "password": "hunter2"},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["token_type"] == "Bearer"
    assert body["scope"] == "user"
    assert body["access_token"]
    assert "expires_at" in body

    stored_keys = list(redis._store.keys())
    assert len(stored_keys) == 1
    assert stored_keys[0].startswith("auth:token:")
    payload = json.loads(redis._store[stored_keys[0]])
    assert payload == {"kind": "user", "username": "alice", "user_id": 42}


@pytest.mark.asyncio
async def test_login_with_correct_superadmin_credentials_issues_superadmin_token(
    client_with_redis: tuple[httpx.AsyncClient, MagicMock],
) -> None:
    client, redis = client_with_redis
    response = await client.post(
        "/api/v1/auth/login",
        json={"username": "root", "password": "rootpass"},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["scope"] == "superadmin"

    payload = json.loads(next(iter(redis._store.values())))
    assert payload["kind"] == "superadmin"
    assert payload["user_id"] is None


@pytest.mark.asyncio
async def test_login_with_wrong_password_returns_401(
    client_with_redis: tuple[httpx.AsyncClient, MagicMock],
) -> None:
    client, _ = client_with_redis
    response = await client.post(
        "/api/v1/auth/login",
        json={"username": "alice", "password": "WRONG"},
    )
    assert response.status_code == 401
    assert response.json()["error_code"] == "UNAUTHORIZED"


@pytest.mark.asyncio
async def test_login_with_unknown_user_returns_401(
    client_with_redis: tuple[httpx.AsyncClient, MagicMock],
) -> None:
    client, _ = client_with_redis
    response = await client.post(
        "/api/v1/auth/login",
        json={"username": "ghost", "password": "anything"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_login_failure_response_is_uniform_across_modes(
    client_with_redis: tuple[httpx.AsyncClient, MagicMock],
) -> None:
    """Unknown user vs wrong password should look identical to the
    client. Detailed reason goes to the structured log, not the
    wire."""
    client, _ = client_with_redis
    a = await client.post(
        "/api/v1/auth/login",
        json={"username": "ghost", "password": "anything"},
    )
    b = await client.post(
        "/api/v1/auth/login",
        json={"username": "alice", "password": "WRONG"},
    )
    assert a.status_code == b.status_code == 401
    a_body = a.json()
    b_body = b.json()
    a_body.pop("request_id", None)
    b_body.pop("request_id", None)
    assert a_body == b_body


# ---- Logout ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_logout_revokes_a_valid_token(
    client_with_redis: tuple[httpx.AsyncClient, MagicMock],
) -> None:
    client, redis = client_with_redis
    login = await client.post(
        "/api/v1/auth/login",
        json={"username": "alice", "password": "hunter2"},
    )
    token = login.json()["access_token"]
    assert redis._store

    response = await client.post(
        "/api/v1/auth/logout",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["revoked"] is True
    assert not redis._store


@pytest.mark.asyncio
async def test_logout_unknown_token_is_rejected_by_middleware(
    client_with_redis: tuple[httpx.AsyncClient, MagicMock],
) -> None:
    """Tokens that aren't in the static allowlist AND not in Redis
    are rejected by the auth middleware before logout's body even
    runs — protects /auth/logout from being a token-validity oracle."""
    client, _ = client_with_redis
    response = await client.post(
        "/api/v1/auth/logout",
        headers={"Authorization": "Bearer bogus-token"},
    )
    assert response.status_code == 401


# ---- Middleware coexistence ------------------------------------------------


@pytest.mark.asyncio
async def test_static_token_still_authorises_other_endpoints(
    client_with_redis: tuple[httpx.AsyncClient, MagicMock],
) -> None:
    """Existing service-to-service callers (eveys-backend) keep
    working — the static `rest_inbound_tokens` allowlist is still
    honoured alongside the new opaque-token scheme."""
    client, _ = client_with_redis
    response = await client.get(
        "/api/v1/health",
        headers={"Authorization": "Bearer static-svc-token"},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_user_token_authorises_other_endpoints(
    client_with_redis: tuple[httpx.AsyncClient, MagicMock],
) -> None:
    """A login-issued token should authorise non-auth endpoints,
    not just /auth/logout."""
    client, _ = client_with_redis
    login = await client.post(
        "/api/v1/auth/login",
        json={"username": "alice", "password": "hunter2"},
    )
    token = login.json()["access_token"]

    response = await client.get(
        "/api/v1/health",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
