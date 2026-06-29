"""Branch tests for `transport/_authorization.check_and_record_authorization`.

Six DB states map to four outcomes:

    no row        -> accepted, pending_new        + fresh deadline
    approved      -> accepted, approved           + None
    pending       -> accepted, pending_within_window + existing deadline (within grace)
                  -> rejected, pending_expired      + None (past grace)
    rejected      -> rejected, rejected
    revoked       -> rejected, revoked
    DB raises     -> rejected, db_error             (fail closed)

The repository layer is mocked because the function under test is the
decision tree, not the SQL. `record_authorization_attempt` is just
expected to be called with the right args; `get_authorization` is the
state input.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest

from eveys_ocpp.settings import Settings
from eveys_ocpp.transport import _authorization as auth_mod

GRACE_SECONDS = 180


@pytest.fixture
def settings() -> Settings:
    return Settings(auth_pending_grace_seconds=GRACE_SECONDS)


@pytest.fixture
def session_factory() -> Any:
    """No-op async context manager — `_authorization` never touches
    the session directly; the repo functions are monkeypatched per
    test so their behaviour is fully controlled."""

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


async def _run(
    monkeypatch: pytest.MonkeyPatch,
    *,
    settings: Settings,
    session_factory: Any,
    existing: dict[str, Any] | None,
    recorded: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> auth_mod.AuthorizationCheck:
    """Drive `check_and_record_authorization` with explicit DB state."""
    monkeypatch.setattr(auth_mod, "get_authorization", AsyncMock(return_value=existing))
    # When the function under test calls `record_authorization_attempt`
    # we hand it back either an explicit `recorded` row or a synthetic
    # one mirroring `existing` (the production code only reads
    # `requested_at` off the return value).
    return_row = recorded if recorded is not None else existing
    monkeypatch.setattr(
        auth_mod,
        "record_authorization_attempt",
        AsyncMock(return_value=return_row),
    )
    return await auth_mod.check_and_record_authorization(
        cp_id="CP_001",
        peer_ip="1.2.3.4",
        user_agent="ua/test",
        session_factory=session_factory,
        settings=settings,
        now=now or datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_first_sighting_accepted_with_deadline(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, session_factory: Any
) -> None:
    now = datetime.now(UTC)
    recorded = {"requested_at": now, "status": "pending"}
    result = await _run(
        monkeypatch,
        settings=settings,
        session_factory=session_factory,
        existing=None,
        recorded=recorded,
        now=now,
    )
    assert result.accepted is True
    assert result.outcome == auth_mod.OUTCOME_PENDING_NEW
    assert result.pending_deadline == now + timedelta(seconds=GRACE_SECONDS)


@pytest.mark.asyncio
async def test_approved_accepted_no_deadline(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, session_factory: Any
) -> None:
    existing = {"status": "approved", "requested_at": datetime.now(UTC)}
    result = await _run(
        monkeypatch,
        settings=settings,
        session_factory=session_factory,
        existing=existing,
    )
    assert result.accepted is True
    assert result.outcome == auth_mod.OUTCOME_APPROVED
    assert result.pending_deadline is None


@pytest.mark.asyncio
async def test_pending_within_window_accepted_with_deadline(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, session_factory: Any
) -> None:
    # `requested_at` is 30 s ago; with 180 s grace, deadline lies in the future.
    now = datetime.now(UTC)
    requested_at = now - timedelta(seconds=30)
    existing = {"status": "pending", "requested_at": requested_at}
    result = await _run(
        monkeypatch,
        settings=settings,
        session_factory=session_factory,
        existing=existing,
        recorded={"status": "pending", "requested_at": requested_at},
        now=now,
    )
    assert result.accepted is True
    assert result.outcome == auth_mod.OUTCOME_PENDING_WITHIN_WINDOW
    # Deadline is fixed to original `requested_at`, NOT sliding from `now`.
    assert result.pending_deadline == requested_at + timedelta(seconds=GRACE_SECONDS)


@pytest.mark.asyncio
async def test_pending_past_window_rejected(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, session_factory: Any
) -> None:
    now = datetime.now(UTC)
    # 5 min ago — well past 180 s grace.
    requested_at = now - timedelta(seconds=300)
    existing = {"status": "pending", "requested_at": requested_at}
    result = await _run(
        monkeypatch,
        settings=settings,
        session_factory=session_factory,
        existing=existing,
        recorded={"status": "pending", "requested_at": requested_at},
        now=now,
    )
    assert result.accepted is False
    assert result.outcome == auth_mod.OUTCOME_PENDING_EXPIRED


@pytest.mark.asyncio
async def test_rejected_rejected(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, session_factory: Any
) -> None:
    existing = {"status": "rejected", "requested_at": datetime.now(UTC)}
    result = await _run(
        monkeypatch,
        settings=settings,
        session_factory=session_factory,
        existing=existing,
    )
    assert result.accepted is False
    assert result.outcome == auth_mod.OUTCOME_REJECTED


@pytest.mark.asyncio
async def test_revoked_rejected(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, session_factory: Any
) -> None:
    existing = {"status": "revoked", "requested_at": datetime.now(UTC)}
    result = await _run(
        monkeypatch,
        settings=settings,
        session_factory=session_factory,
        existing=existing,
    )
    assert result.accepted is False
    assert result.outcome == auth_mod.OUTCOME_REVOKED


@pytest.mark.asyncio
async def test_db_error_fails_closed(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, session_factory: Any
) -> None:
    monkeypatch.setattr(
        auth_mod, "get_authorization", AsyncMock(side_effect=RuntimeError("kaboom"))
    )
    monkeypatch.setattr(auth_mod, "record_authorization_attempt", AsyncMock())
    result = await auth_mod.check_and_record_authorization(
        cp_id="CP_001",
        peer_ip=None,
        user_agent=None,
        session_factory=session_factory,
        settings=settings,
        now=datetime.now(UTC),
    )
    assert result.accepted is False
    assert result.outcome == auth_mod.OUTCOME_DB_ERROR


@pytest.mark.asyncio
async def test_unknown_status_fails_closed(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, session_factory: Any
) -> None:
    """A row with a status we don't recognise (forward-compat: a future
    status the gateway doesn't know yet) must be treated as fail-closed
    rather than silently accepted."""
    existing = {"status": "future_state", "requested_at": datetime.now(UTC)}
    result = await _run(
        monkeypatch,
        settings=settings,
        session_factory=session_factory,
        existing=existing,
    )
    assert result.accepted is False
    assert result.outcome == auth_mod.OUTCOME_DB_ERROR
