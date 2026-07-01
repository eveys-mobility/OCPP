"""Integration-test conftest.

Same rules as ``tests/e2e/`` — the test opts into a real Postgres and
skips cleanly when it isn't reachable, so ``make tests`` on a laptop
without ``docker compose up`` stays green.

Two fixtures:

* ``postgres_session_factory`` — an ``async_sessionmaker`` bound to
  the scratch Postgres exposed on host port 55432 (compose default).
  Verifies the schema is applied; skips otherwise. Truncates
  ``webhook_delivery_backlog`` between tests so scenarios don't
  cross-contaminate.
* ``anyio_backend`` — pins asyncio so pytest-asyncio treats these as
  async tests (already the project default via pyproject.toml but
  explicit here for the new sub-tree).
"""

from __future__ import annotations

import os
import socket
from collections.abc import AsyncIterator
from contextlib import closing

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# Match the e2e defaults so operators only need one env-var override
# to point everything at CI's real Postgres.
_PG_HOST = os.environ.get("E2E_PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("E2E_PG_PORT", "55432"))
_TEST_DB_URL = f"postgresql+asyncpg://eveys:eveys@{_PG_HOST}:{_PG_PORT}/eveys_ocpp"


def _postgres_reachable() -> bool:
    """Cheap TCP probe. Half a second is more than enough for a
    docker-hosted Postgres on the loopback interface."""
    try:
        with closing(socket.create_connection((_PG_HOST, _PG_PORT), timeout=0.5)):
            return True
    except OSError:
        return False


@pytest_asyncio.fixture
async def postgres_session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Session factory bound to the scratch Postgres. Skips the test
    when Postgres is unreachable OR when the schema isn't applied.

    Between tests, we truncate ``webhook_delivery_backlog`` so a run
    always starts with an empty table. Truncating with ``RESTART
    IDENTITY CASCADE`` isn't needed — no FKs point at this table."""
    if not _postgres_reachable():
        pytest.skip(f"postgres unreachable at {_PG_HOST}:{_PG_PORT}")

    engine = create_async_engine(_TEST_DB_URL)
    # Verify schema. If the table doesn't exist, the migration hasn't
    # been applied — skip rather than fail so a laptop without
    # `alembic upgrade head` doesn't produce a scary error.
    async with engine.connect() as conn:
        try:
            await conn.execute(sa.text("SELECT 1 FROM webhook_delivery_backlog LIMIT 1"))
        except Exception:
            await engine.dispose()
            pytest.skip("webhook_delivery_backlog table missing — run `alembic upgrade head` first")

    factory = async_sessionmaker(engine, expire_on_commit=False)

    # Pre-truncate. In case a previous run left rows.
    async with factory() as session:
        await session.execute(sa.text("TRUNCATE TABLE webhook_delivery_backlog"))
        await session.commit()

    try:
        yield factory
    finally:
        # Clean up after this test's rows so the next test starts
        # fresh. We could also do this per-scenario inside the test
        # but centralising here keeps the tests short.
        async with factory() as session:
            await session.execute(sa.text("TRUNCATE TABLE webhook_delivery_backlog"))
            await session.commit()
        await engine.dispose()
