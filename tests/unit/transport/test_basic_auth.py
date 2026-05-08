"""Unit tests for the WS-edge Basic Auth check (E5-6)."""

from __future__ import annotations

import base64
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from eveys_ocpp.settings import Settings
from eveys_ocpp.transport._basic_auth import (
    OUTCOME_BAD_PASSWORD,
    OUTCOME_DB_ERROR,
    OUTCOME_MALFORMED,
    OUTCOME_NO_CREDENTIAL,
    OUTCOME_NO_HEADER,
    OUTCOME_OK,
    OUTCOME_USERNAME_MISMATCH,
    AuthResult,
    _parse_basic_header,
    hash_password,
    verify_basic_auth,
)

# -- header parser ------------------------------------------------------------


def _basic(username: str, password: str) -> str:
    raw = f"{username}:{password}".encode()
    return f"Basic {base64.b64encode(raw).decode()}"


def test_parse_basic_header_extracts_username_and_password() -> None:
    parsed = _parse_basic_header(_basic("CP_001", "hunter2"))
    assert parsed == ("CP_001", "hunter2")


def test_parse_basic_header_returns_none_on_no_header() -> None:
    assert _parse_basic_header(None) is None
    assert _parse_basic_header("") is None


def test_parse_basic_header_returns_none_on_non_basic_scheme() -> None:
    assert _parse_basic_header("Bearer abc.def") is None
    assert _parse_basic_header("Digest username=foo") is None


def test_parse_basic_header_returns_none_on_invalid_base64() -> None:
    assert _parse_basic_header("Basic !!not-base64!!") is None


def test_parse_basic_header_returns_none_on_no_colon_payload() -> None:
    # Base64-decoded payload missing the `:` separator.
    payload = base64.b64encode(b"no-colon-here").decode()
    assert _parse_basic_header(f"Basic {payload}") is None


def test_parse_basic_header_handles_password_containing_colon() -> None:
    # Per RFC 7617, only the first `:` separates user and pass.
    parsed = _parse_basic_header(_basic("CP_001", "p:a:s:s"))
    assert parsed == ("CP_001", "p:a:s:s")


def test_parse_basic_header_is_case_insensitive_on_scheme() -> None:
    raw = base64.b64encode(b"CP_001:x").decode()
    assert _parse_basic_header(f"basic {raw}") == ("CP_001", "x")
    assert _parse_basic_header(f"BASIC {raw}") == ("CP_001", "x")


# -- verify_basic_auth --------------------------------------------------------


def _session_factory_returning(stored_hash: str | None) -> Any:
    """Build a fake `async_sessionmaker` whose session's call to
    `get_credential_hash` returns `stored_hash`. We monkey-patch the
    repository import because the verify path uses the public function."""
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)

    factory = MagicMock(return_value=session)
    factory._stored_hash = stored_hash
    return factory


@pytest.fixture
def settings_permissive() -> Settings:
    return Settings(ws_basic_auth_required=False)


@pytest.fixture
def settings_strict() -> Settings:
    return Settings(ws_basic_auth_required=True)


@pytest.mark.asyncio
async def test_no_header_in_permissive_mode_with_no_credential_accepts(
    settings_permissive: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Permissive default: a charger that doesn't send any auth and
    has no credential row provisioned still gets through. This is the
    fleet-migration shim — production flips ws_basic_auth_required."""
    monkeypatch.setattr(
        "eveys_ocpp.transport._basic_auth.get_credential_hash",
        AsyncMock(return_value=None),
    )
    result = await verify_basic_auth(
        cp_id="CP_001",
        auth_header=None,
        session_factory=_session_factory_returning(None),
        settings=settings_permissive,
    )
    assert result == AuthResult(accepted=True, outcome=OUTCOME_OK)


@pytest.mark.asyncio
async def test_no_header_with_provisioned_credential_rejects() -> None:
    """Once a credential is provisioned, a missing header is a hard
    reject — even in permissive mode. Otherwise rotating in a password
    would do nothing."""
    from eveys_ocpp.transport._basic_auth import hash_password

    settings_permissive = Settings(ws_basic_auth_required=False)
    stored = hash_password("hunter2")
    import unittest.mock as _mock

    with _mock.patch(
        "eveys_ocpp.transport._basic_auth.get_credential_hash",
        AsyncMock(return_value=stored),
    ):
        result = await verify_basic_auth(
            cp_id="CP_001",
            auth_header=None,
            session_factory=_session_factory_returning(stored),
            settings=settings_permissive,
        )
    assert result == AuthResult(accepted=False, outcome=OUTCOME_NO_HEADER)


@pytest.mark.asyncio
async def test_no_header_in_strict_mode_rejects(
    settings_strict: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Strict mode: no header → reject regardless of credential row."""
    monkeypatch.setattr(
        "eveys_ocpp.transport._basic_auth.get_credential_hash",
        AsyncMock(return_value=None),
    )
    result = await verify_basic_auth(
        cp_id="CP_001",
        auth_header=None,
        session_factory=_session_factory_returning(None),
        settings=settings_strict,
    )
    assert result == AuthResult(accepted=False, outcome=OUTCOME_NO_HEADER)


@pytest.mark.asyncio
async def test_malformed_header_in_permissive_mode_with_no_credential_accepts(
    settings_permissive: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same permissive-acceptance branch for a malformed header."""
    monkeypatch.setattr(
        "eveys_ocpp.transport._basic_auth.get_credential_hash",
        AsyncMock(return_value=None),
    )
    result = await verify_basic_auth(
        cp_id="CP_001",
        auth_header="Bearer wrong-scheme",
        session_factory=_session_factory_returning(None),
        settings=settings_permissive,
    )
    assert result == AuthResult(accepted=True, outcome=OUTCOME_OK)


@pytest.mark.asyncio
async def test_malformed_header_in_strict_mode_rejects(
    settings_strict: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "eveys_ocpp.transport._basic_auth.get_credential_hash",
        AsyncMock(return_value=None),
    )
    result = await verify_basic_auth(
        cp_id="CP_001",
        auth_header="Bearer wrong-scheme",
        session_factory=_session_factory_returning(None),
        settings=settings_strict,
    )
    assert result == AuthResult(accepted=False, outcome=OUTCOME_MALFORMED)


@pytest.mark.asyncio
async def test_username_mismatch_is_rejected(
    settings_permissive: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A charger holding `CP_A`'s password must not be able to
    connect as `CP_B`. The username field in the Basic header is
    enforced to match the URL's cp_id."""
    monkeypatch.setattr(
        "eveys_ocpp.transport._basic_auth.get_credential_hash",
        AsyncMock(return_value=None),
    )
    result = await verify_basic_auth(
        cp_id="CP_B",
        auth_header=_basic("CP_A", "anything"),
        session_factory=_session_factory_returning(None),
        settings=settings_permissive,
    )
    assert result == AuthResult(accepted=False, outcome=OUTCOME_USERNAME_MISMATCH)


@pytest.mark.asyncio
async def test_correct_password_is_accepted(
    settings_permissive: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = hash_password("hunter2")
    monkeypatch.setattr(
        "eveys_ocpp.transport._basic_auth.get_credential_hash",
        AsyncMock(return_value=stored),
    )
    result = await verify_basic_auth(
        cp_id="CP_001",
        auth_header=_basic("CP_001", "hunter2"),
        session_factory=_session_factory_returning(stored),
        settings=settings_permissive,
    )
    assert result == AuthResult(accepted=True, outcome=OUTCOME_OK)


@pytest.mark.asyncio
async def test_wrong_password_is_rejected(
    settings_permissive: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = hash_password("hunter2")
    monkeypatch.setattr(
        "eveys_ocpp.transport._basic_auth.get_credential_hash",
        AsyncMock(return_value=stored),
    )
    result = await verify_basic_auth(
        cp_id="CP_001",
        auth_header=_basic("CP_001", "wrong"),
        session_factory=_session_factory_returning(stored),
        settings=settings_permissive,
    )
    assert result == AuthResult(accepted=False, outcome=OUTCOME_BAD_PASSWORD)


@pytest.mark.asyncio
async def test_missing_credential_permissive_accepts(
    settings_permissive: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default mode: a charger without a credential row is accepted.
    Lets a fleet migrate gradually."""
    monkeypatch.setattr(
        "eveys_ocpp.transport._basic_auth.get_credential_hash",
        AsyncMock(return_value=None),
    )
    result = await verify_basic_auth(
        cp_id="CP_001",
        auth_header=_basic("CP_001", "anything"),
        session_factory=_session_factory_returning(None),
        settings=settings_permissive,
    )
    assert result == AuthResult(accepted=True, outcome=OUTCOME_OK)


@pytest.mark.asyncio
async def test_missing_credential_strict_rejects(
    settings_strict: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Strict mode: a charger without a credential row is rejected.
    Production posture."""
    monkeypatch.setattr(
        "eveys_ocpp.transport._basic_auth.get_credential_hash",
        AsyncMock(return_value=None),
    )
    result = await verify_basic_auth(
        cp_id="CP_001",
        auth_header=_basic("CP_001", "anything"),
        session_factory=_session_factory_returning(None),
        settings=settings_strict,
    )
    assert result == AuthResult(accepted=False, outcome=OUTCOME_NO_CREDENTIAL)


@pytest.mark.asyncio
async def test_malformed_stored_hash_is_rejected(
    settings_permissive: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A corrupted credential row (someone INSERT'd a plaintext into
    `password_hash`) must reject with bad_password rather than
    crash the verify path."""
    monkeypatch.setattr(
        "eveys_ocpp.transport._basic_auth.get_credential_hash",
        AsyncMock(return_value="not-a-bcrypt-hash"),
    )
    result = await verify_basic_auth(
        cp_id="CP_001",
        auth_header=_basic("CP_001", "anything"),
        session_factory=_session_factory_returning("not-a-bcrypt-hash"),
        settings=settings_permissive,
    )
    assert result == AuthResult(accepted=False, outcome=OUTCOME_BAD_PASSWORD)


def test_hash_password_round_trips() -> None:
    """`hash_password` produces a value that bcrypt verifies."""
    import bcrypt

    h = hash_password("hunter2")
    assert bcrypt.checkpw(b"hunter2", h.encode())
    assert not bcrypt.checkpw(b"wrong", h.encode())


# -- DB-error fail-closed (regression: PR after #117) --------------------


def _session_factory_raising(exc: Exception) -> Any:
    """Build a session_factory whose `__aenter__` raises. Models the
    real-world failure modes: `UndefinedTableError` (migrations
    haven't run on a fresh stack), `ConnectionRefusedError`
    (Postgres down), or any other exception that bubbles out of the
    SQLAlchemy session context."""

    factory = MagicMock()
    session = MagicMock()
    session.__aenter__ = AsyncMock(side_effect=exc)
    session.__aexit__ = AsyncMock(return_value=None)
    factory.return_value = session
    return factory


@pytest.mark.asyncio
async def test_db_error_fails_closed_with_credentials_present(
    settings_permissive: Settings,
) -> None:
    """A DB exception while looking up credentials must NOT bubble out
    as a 500 to the charger. Fail closed — return 401 with
    `db_error` outcome.

    This is the regression that produced "Reconnect failed:
    Unexpected server response: 500" against a fresh compose stack
    where alembic hadn't run (`UndefinedTableError`)."""
    factory = _session_factory_raising(
        RuntimeError('relation "charge_point_credentials" does not exist')
    )
    result = await verify_basic_auth(
        cp_id="CP_001",
        auth_header=_basic("CP_001", "anything"),
        session_factory=factory,
        settings=settings_permissive,
    )
    assert result == AuthResult(accepted=False, outcome=OUTCOME_DB_ERROR)


@pytest.mark.asyncio
async def test_db_error_fails_closed_in_no_header_permissive_path(
    settings_permissive: Settings,
) -> None:
    """The "no header + permissive mode + check whether a credential
    row exists" branch also touches the DB. A failure there must
    fail closed identically — otherwise an unprovisioned charger
    that should have been silently accepted hits a 500 on the WS
    upgrade just because Postgres blipped."""
    factory = _session_factory_raising(RuntimeError("connection refused"))
    result = await verify_basic_auth(
        cp_id="CP_001",
        auth_header=None,  # no header, falls to the permissive-lookup branch
        session_factory=factory,
        settings=settings_permissive,
    )
    assert result == AuthResult(accepted=False, outcome=OUTCOME_DB_ERROR)


@pytest.mark.asyncio
async def test_db_error_fails_closed_in_strict_mode(
    settings_strict: Settings,
) -> None:
    """Strict mode: a DB error doesn't change the rejection decision
    (we'd reject anyway because we can't verify the credential).
    Outcome label distinguishes "rejected because no creds" from
    "rejected because DB blew up" — operator alerting cares."""
    factory = _session_factory_raising(RuntimeError("postgres down"))
    result = await verify_basic_auth(
        cp_id="CP_001",
        auth_header=_basic("CP_001", "anything"),
        session_factory=factory,
        settings=settings_strict,
    )
    assert result == AuthResult(accepted=False, outcome=OUTCOME_DB_ERROR)
