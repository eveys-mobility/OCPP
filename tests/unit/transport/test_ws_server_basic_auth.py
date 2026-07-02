"""WS Basic Auth integration test (issue #100).

Unit tests for `_basic_auth.verify_basic_auth` and `_parse_basic_header`
already exist in `test_basic_auth.py`. What's missing — and what this
file fills — is the integration: does `serve_forever`'s `process_request`
callback actually call `verify_basic_auth` and reject the upgrade?

If someone removed the `process_request=_process_request` line in
`ws_server.serve_forever`, every existing test would still pass (the
function-under-test continues to work in isolation), but in production
chargers would silently bypass Basic Auth on the WS upgrade.

This test boots `serve_forever` on an ephemeral port against a real
aiosqlite-backed session factory with one provisioned credential row,
then opens real WebSocket connections from a `websockets` client and
asserts:

  - strict mode + missing creds → 401
  - strict mode + wrong password → 401
  - strict mode + correct creds → upgrade succeeds (we close right
    after the handshake; we're not testing the OCPP handler stack)

The bug class the audit highlighted is exactly the "wiring" gap that
the unit test for `verify_basic_auth` cannot cover: a missing call,
not a buggy implementation. Removing the `process_request=` wiring in
the source while running this test will make every "should reject"
case fail — verified locally.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# `websockets` is already a runtime dep; we use the asyncio variant the
# rest of the e2e tests use.
from websockets import InvalidStatus
from websockets.asyncio.client import connect

from eveys_ocpp.persistence.models import Base, ChargePoint, ChargePointCredential
from eveys_ocpp.settings import Settings
from eveys_ocpp.transport._basic_auth import hash_password
from eveys_ocpp.transport.ws_server import OCPP_SUBPROTOCOL, serve_forever


def _free_port() -> int:
    """Bind a temporary socket on :0 to grab an unused port. The
    SO_REUSEADDR dance lets the port be reused immediately when
    `serve_forever` rebinds it microseconds later."""
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest_asyncio.fixture
async def session_factory_with_credential() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """In-memory aiosqlite with the ORM schema + one charger
    (`CP_AUTH_TEST`) credentialled with password `secret-pw`.

    aiosqlite's :memory: is per-connection, so we use a connection
    pool that pins every checkout to the same in-memory database via
    `?cache=shared` + a fixed dsn that the pool returns to.

    SQLite quirk: only `INTEGER PRIMARY KEY` aliases the rowid for
    auto-increment. Our `BigInteger` PKs render as `BIGINT` and
    don't get the magic. Apply `with_variant(Integer(), "sqlite")`
    to every BigInteger PK so `INSERT INTO ... RETURNING id` works.
    Idempotent — skips columns that already carry the variant from
    a prior test in this process (metadata is module-global).
    """
    from sqlalchemy import BigInteger, Integer

    for table in Base.metadata.tables.values():
        for col in table.columns:
            if (
                col.primary_key
                and isinstance(col.type, BigInteger)
                and "sqlite" not in getattr(col.type, "_variant_mapping", {})
            ):
                col.type = col.type.with_variant(Integer(), "sqlite")  # type: ignore[assignment]

    # `:memory:` per-connection means every pool checkout is a fresh
    # blank DB — useless for tests that span >1 query. The shared-cache
    # form gives every connection in the pool the same DB.
    engine = create_async_engine(
        "sqlite+aiosqlite:///file:wsauth_mem?mode=memory&cache=shared&uri=true",
        future=True,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        cp = ChargePoint(cp_id="CP_AUTH_TEST")
        session.add(cp)
        await session.flush()
        session.add(
            ChargePointCredential(
                charge_point_id=cp.id,
                password_hash=hash_password("secret-pw"),
            )
        )
        await session.commit()

    yield factory
    await engine.dispose()


@pytest_asyncio.fixture
async def running_server(
    session_factory_with_credential: async_sessionmaker[AsyncSession],
) -> AsyncIterator[int]:
    """Boot `serve_forever` on an ephemeral port in a background task.
    Yields the port; cancels the task on teardown.

    `ws_basic_auth_required=True` (strict mode) — that's the
    production posture and the one the test cases are written for.
    """
    port = _free_port()
    settings = Settings(
        ws_host="127.0.0.1",
        ws_port=port,
        ws_basic_auth_required=True,
    )
    # These tests exercise the Basic Auth gate only — the pending store
    # and IP rate limiter don't fire on the paths under test (the
    # 401-on-missing/wrong-credentials branches return before
    # authorization is consulted), so a MagicMock is enough. Passing
    # `None` for the IP rate limiter takes the "not enabled" branch of
    # `check_and_record_authorization`.
    from unittest.mock import MagicMock

    pending_store = MagicMock()
    task = asyncio.create_task(
        serve_forever(
            session_factory=session_factory_with_credential,
            settings=settings,
            pending_store=pending_store,
            ip_rate_limiter=None,
        )
    )
    # Wait for the listener to actually accept connections — `serve()`
    # is async and the bind happens inside it. Probe the port until
    # it answers.
    deadline = asyncio.get_event_loop().time() + 2.0
    while asyncio.get_event_loop().time() < deadline:
        try:
            with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                s.settimeout(0.1)
                s.connect(("127.0.0.1", port))
                break
        except OSError:
            await asyncio.sleep(0.05)
    else:  # pragma: no cover — defensive
        task.cancel()
        with contextlib.suppress(BaseException):
            await task
        raise RuntimeError("ws_server didn't bind within 2s")

    yield port

    task.cancel()
    with contextlib.suppress(BaseException):
        await task


# ---- the actual test cases ------------------------------------------------


@pytest.mark.asyncio
async def test_ws_upgrade_rejects_when_missing_credentials(
    running_server: int,
) -> None:
    """Strict mode + no Authorization header → 401 from the
    `process_request` callback. The OCPP handler stack never sees
    the connection.

    If someone removed the `process_request=` wiring, the upgrade
    would succeed (websockets defaults are permissive); this test
    would then fail with `did not raise InvalidStatus`.
    """
    with pytest.raises(InvalidStatus) as excinfo:
        async with connect(
            f"ws://127.0.0.1:{running_server}/CP_AUTH_TEST",
            subprotocols=[OCPP_SUBPROTOCOL],
        ):
            pass
    assert excinfo.value.response.status_code == 401


@pytest.mark.asyncio
async def test_ws_upgrade_rejects_with_wrong_password(
    running_server: int,
) -> None:
    """Wrong password → 401. Same rejection path as missing creds."""
    with pytest.raises(InvalidStatus) as excinfo:
        async with connect(
            f"ws://127.0.0.1:{running_server}/CP_AUTH_TEST",
            subprotocols=[OCPP_SUBPROTOCOL],
            additional_headers={"Authorization": "Basic Q1BfQVVUSF9URVNUOndyb25n"},
        ):
            pass
    assert excinfo.value.response.status_code == 401


@pytest.mark.asyncio
async def test_ws_upgrade_accepts_with_correct_credentials(
    running_server: int,
) -> None:
    """Correct creds → upgrade succeeds. We don't drive the OCPP
    handler stack here — opening the connection is the assertion;
    if the handshake completes, the integration is wired.

    Header value: base64("CP_AUTH_TEST:secret-pw").
    """
    import base64

    creds = base64.b64encode(b"CP_AUTH_TEST:secret-pw").decode("ascii")
    async with connect(
        f"ws://127.0.0.1:{running_server}/CP_AUTH_TEST",
        subprotocols=[OCPP_SUBPROTOCOL],
        additional_headers={"Authorization": f"Basic {creds}"},
    ) as ws:
        # Reaching this line means the upgrade completed — we close
        # immediately. The OCPP handler is exercised by the e2e suite,
        # not here.
        assert ws.subprotocol == OCPP_SUBPROTOCOL
