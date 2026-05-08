"""Unit tests for the auth core (issue #84 PR-A).

Covers the pure helpers (bcrypt round-trip, identity construction)
and the Redis-backed token operations using fakeredis at the
boundary. The login flow itself is exercised via the route tests
(`tests/unit/api/test_auth.py`)."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from eveys_ocpp.auth import (
    AuthIdentity,
    hash_password,
    issue_token,
    lookup_token,
    revoke_token,
    verify_password,
)

# ---- bcrypt helpers --------------------------------------------------------


def test_hash_password_round_trips() -> None:
    h = hash_password("hunter2")
    assert verify_password("hunter2", h) is True
    assert verify_password("wrong", h) is False


def test_verify_password_returns_false_on_malformed_hash() -> None:
    """A corrupted DB row shouldn't crash the login path — bcrypt
    raises on a non-bcrypt input; the helper swallows that."""
    assert verify_password("anything", "not-a-bcrypt-hash") is False
    assert verify_password("anything", "") is False


def test_hash_is_not_predictable() -> None:
    """Two hashes of the same plaintext differ (bcrypt salts)."""
    h1 = hash_password("x")
    h2 = hash_password("x")
    assert h1 != h2
    # But both verify against the same plaintext.
    assert verify_password("x", h1)
    assert verify_password("x", h2)


# ---- AuthIdentity ---------------------------------------------------------


def test_auth_identity_is_frozen() -> None:
    """`AuthIdentity` is `frozen=True` — mutation raises `FrozenInstanceError`
    (a `dataclasses` subclass of `AttributeError`)."""
    from dataclasses import FrozenInstanceError

    identity = AuthIdentity(kind="user", username="alice", user_id=42)
    with pytest.raises(FrozenInstanceError):
        identity.username = "bob"  # type: ignore[misc]


def test_auth_identity_user_id_is_optional() -> None:
    superadmin = AuthIdentity(kind="superadmin", username="root")
    assert superadmin.user_id is None
    service = AuthIdentity(kind="service", username="static-token")
    assert service.user_id is None


# ---- Token issue / lookup / revoke ----------------------------------------


@pytest.mark.asyncio
async def test_issue_token_writes_redis_with_ttl() -> None:
    redis = AsyncMock()
    identity = AuthIdentity(kind="user", username="alice", user_id=42)
    token, expires_at = await issue_token(redis, identity=identity, ttl_seconds=3600)

    assert isinstance(token, str)
    assert len(token) > 32  # 48 bytes base64url is well above this
    redis.set.assert_awaited_once()
    args, kwargs = redis.set.await_args
    key, value = args
    assert key == f"auth:token:{token}"
    payload = json.loads(value)
    assert payload == {"kind": "user", "username": "alice", "user_id": 42}
    assert kwargs["ex"] == 3600

    # expires_at is roughly now + ttl.
    from datetime import UTC, datetime, timedelta

    delta = expires_at - datetime.now(UTC)
    assert timedelta(seconds=3500) < delta < timedelta(seconds=3700)


@pytest.mark.asyncio
async def test_lookup_token_returns_identity_on_hit() -> None:
    redis = AsyncMock()
    redis.get = AsyncMock(
        return_value=json.dumps({"kind": "user", "username": "alice", "user_id": 42})
    )
    identity = await lookup_token(redis, token="some-token")
    assert identity == AuthIdentity(kind="user", username="alice", user_id=42)


@pytest.mark.asyncio
async def test_lookup_token_returns_none_on_miss() -> None:
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    assert await lookup_token(redis, token="some-token") is None


@pytest.mark.asyncio
async def test_lookup_token_returns_none_on_redis_error() -> None:
    """Fail closed on Redis errors — the auth gate is not the place
    to fail open."""
    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=RuntimeError("redis down"))
    assert await lookup_token(redis, token="some-token") is None


@pytest.mark.asyncio
async def test_lookup_token_returns_none_on_malformed_payload() -> None:
    """A stray Redis key from a prior schema mustn't 500 the auth
    gate."""
    redis = AsyncMock()
    redis.get = AsyncMock(return_value="not-json-at-all")
    assert await lookup_token(redis, token="some-token") is None


@pytest.mark.asyncio
async def test_lookup_token_returns_none_on_bogus_kind() -> None:
    """A token whose `kind` isn't in the closed enum is rejected."""
    redis = AsyncMock()
    redis.get = AsyncMock(
        return_value=json.dumps({"kind": "ghost", "username": "alice", "user_id": 42})
    )
    assert await lookup_token(redis, token="some-token") is None


@pytest.mark.asyncio
async def test_revoke_token_deletes_when_present() -> None:
    redis = AsyncMock()
    redis.delete = AsyncMock(return_value=1)
    assert await revoke_token(redis, token="t") is True


@pytest.mark.asyncio
async def test_revoke_token_returns_false_when_absent() -> None:
    redis = AsyncMock()
    redis.delete = AsyncMock(return_value=0)
    assert await revoke_token(redis, token="t") is False


@pytest.mark.asyncio
async def test_revoke_token_returns_false_on_redis_error() -> None:
    redis = AsyncMock()
    redis.delete = AsyncMock(side_effect=RuntimeError("redis down"))
    assert await revoke_token(redis, token="t") is False
