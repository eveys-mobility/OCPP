"""Branch tests for `transport/_authorization.check_and_record_authorization`.

The gate has three interesting paths:

    charge_points row exists      -> accepted, OUTCOME_AUTHORIZED (IP RL bypassed)
    no row, IP under the limit    -> accepted, OUTCOME_PENDING_NEW / _REFRESHED
    no row, IP over the limit     -> rejected, OUTCOME_IP_BLOCKED

Plus two failure modes: Postgres error → fail-closed with `db_error`;
Redis error on the pending upsert → fail-closed with `redis_error`.

The Postgres query, the pending store, and the IP limiter are all
mocked — the SUT is the decision tree, not the SQL / Redis calls.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from eveys_ocpp.transport import _authorization as auth_mod
from eveys_ocpp.transport._ip_rate_limiter import IpRateLimitDecision


def _make_session_factory() -> Any:
    """No-op async context manager — the SUT's session is only used to
    call `get_charge_point_pk`, which is monkeypatched per test."""

    class _Session:
        async def __aenter__(self) -> _Session:
            return self

        async def __aexit__(self, *_: Any) -> None:
            return None

        commit = AsyncMock()

    class _Factory:
        def __call__(self) -> _Session:
            return _Session()

    return _Factory()


def _make_pending_store(*, upsert_return: dict[str, Any] | None = None) -> MagicMock:
    """Minimal `PendingAuthorizations` stub. Only `upsert` is called on
    the pending path; the admin API is exercised elsewhere."""
    store = MagicMock()
    store.upsert = AsyncMock(return_value=upsert_return or {"cp_id": "CP_001", "attempts": 1})
    return store


def _make_ip_limiter(*, allowed: bool, outcome: str) -> MagicMock:
    """Stub IP rate limiter that returns a fixed decision."""
    limiter = MagicMock()
    limiter.check = AsyncMock(return_value=IpRateLimitDecision(allowed=allowed, outcome=outcome))
    return limiter


@pytest.mark.asyncio
async def test_authorized_cp_bypasses_ip_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An existing `charge_points` row → accepted, IP limiter is NOT
    consulted (per design: fleet reconnects must never be throttled)."""
    monkeypatch.setattr(auth_mod, "get_charge_point_pk", AsyncMock(return_value=42))
    limiter = _make_ip_limiter(allowed=False, outcome="blocked")
    pending = _make_pending_store()

    result = await auth_mod.check_and_record_authorization(
        cp_id="CP_KNOWN",
        peer_ip="1.2.3.4",
        user_agent="ua/1",
        session_factory=_make_session_factory(),
        pending_store=pending,
        ip_rate_limiter=limiter,
        now=datetime.now(UTC),
    )

    assert result.accepted is True
    assert result.is_pending is False
    assert result.outcome == auth_mod.OUTCOME_AUTHORIZED
    # Load-bearing: limiter and pending store must not be touched.
    limiter.check.assert_not_awaited()
    pending.upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_cp_under_limit_is_pending_new(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No `charge_points` row + IP allowed + first attempt (attempts=1)
    → accepted flagged pending, outcome `pending_new`."""
    monkeypatch.setattr(auth_mod, "get_charge_point_pk", AsyncMock(return_value=None))
    limiter = _make_ip_limiter(allowed=True, outcome="allowed")
    pending = _make_pending_store(upsert_return={"cp_id": "CP_NEW", "attempts": 1})

    result = await auth_mod.check_and_record_authorization(
        cp_id="CP_NEW",
        peer_ip="1.2.3.4",
        user_agent="ua/1",
        session_factory=_make_session_factory(),
        pending_store=pending,
        ip_rate_limiter=limiter,
        now=datetime.now(UTC),
    )

    assert result.accepted is True
    assert result.is_pending is True
    assert result.outcome == auth_mod.OUTCOME_PENDING_NEW
    limiter.check.assert_awaited_once_with("1.2.3.4")
    pending.upsert.assert_awaited_once()


@pytest.mark.asyncio
async def test_unknown_cp_reconnect_is_pending_refreshed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pending device that reconnects → `attempts > 1` in the upsert
    return → outcome `pending_refreshed`."""
    monkeypatch.setattr(auth_mod, "get_charge_point_pk", AsyncMock(return_value=None))
    limiter = _make_ip_limiter(allowed=True, outcome="allowed")
    pending = _make_pending_store(upsert_return={"cp_id": "CP_NEW", "attempts": 3})

    result = await auth_mod.check_and_record_authorization(
        cp_id="CP_NEW",
        peer_ip="1.2.3.4",
        user_agent="ua/1",
        session_factory=_make_session_factory(),
        pending_store=pending,
        ip_rate_limiter=limiter,
        now=datetime.now(UTC),
    )

    assert result.accepted is True
    assert result.is_pending is True
    assert result.outcome == auth_mod.OUTCOME_PENDING_REFRESHED


@pytest.mark.asyncio
async def test_unknown_cp_over_ip_limit_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No `charge_points` row + IP over the per-minute cap → rejected
    with `ip_blocked`. The pending store is NOT touched (per design:
    the ban must not keep refreshing the pending TTL)."""
    monkeypatch.setattr(auth_mod, "get_charge_point_pk", AsyncMock(return_value=None))
    limiter = _make_ip_limiter(allowed=False, outcome="newly_blocked")
    pending = _make_pending_store()

    result = await auth_mod.check_and_record_authorization(
        cp_id="CP_UNKNOWN",
        peer_ip="9.9.9.9",
        user_agent=None,
        session_factory=_make_session_factory(),
        pending_store=pending,
        ip_rate_limiter=limiter,
        now=datetime.now(UTC),
    )

    assert result.accepted is False
    assert result.is_pending is False
    assert result.outcome == auth_mod.OUTCOME_IP_BLOCKED
    pending.upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_ip_limiter_still_reaches_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The IP limiter is optional (unit tests, dev without Redis).
    None means the gate skips straight to the pending upsert."""
    monkeypatch.setattr(auth_mod, "get_charge_point_pk", AsyncMock(return_value=None))
    pending = _make_pending_store(upsert_return={"cp_id": "CP_NEW", "attempts": 1})

    result = await auth_mod.check_and_record_authorization(
        cp_id="CP_NEW",
        peer_ip=None,
        user_agent=None,
        session_factory=_make_session_factory(),
        pending_store=pending,
        ip_rate_limiter=None,
        now=datetime.now(UTC),
    )

    assert result.accepted is True
    assert result.outcome == auth_mod.OUTCOME_PENDING_NEW
    pending.upsert.assert_awaited_once()


@pytest.mark.asyncio
async def test_db_error_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Postgres error on the authorized-lookup query → fail closed.
    The gate cannot accept a device on a stale/absent DB read."""
    monkeypatch.setattr(
        auth_mod,
        "get_charge_point_pk",
        AsyncMock(side_effect=RuntimeError("kaboom")),
    )
    pending = _make_pending_store()

    result = await auth_mod.check_and_record_authorization(
        cp_id="CP_ANY",
        peer_ip="1.2.3.4",
        user_agent=None,
        session_factory=_make_session_factory(),
        pending_store=pending,
        ip_rate_limiter=None,
        now=datetime.now(UTC),
    )

    assert result.accepted is False
    assert result.outcome == auth_mod.OUTCOME_DB_ERROR
    pending.upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_redis_error_on_pending_upsert_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pending write is the durable half of the pending flow — if
    Redis is down we can't record the device, so the accept has nothing
    to hang on. Rejects with `redis_error`."""
    monkeypatch.setattr(auth_mod, "get_charge_point_pk", AsyncMock(return_value=None))
    pending = MagicMock()
    pending.upsert = AsyncMock(side_effect=RuntimeError("redis down"))

    result = await auth_mod.check_and_record_authorization(
        cp_id="CP_NEW",
        peer_ip="1.2.3.4",
        user_agent=None,
        session_factory=_make_session_factory(),
        pending_store=pending,
        ip_rate_limiter=None,
        now=datetime.now(UTC),
    )

    assert result.accepted is False
    assert result.outcome == auth_mod.OUTCOME_REDIS_ERROR
