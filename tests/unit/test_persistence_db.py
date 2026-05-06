"""Unit tests for the db engine factory + session_scope context."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from eveys_ocpp.persistence.db import make_engine, make_session_factory, session_scope


def test_make_engine_uses_provided_dsn() -> None:
    engine = make_engine("postgresql+asyncpg://u:p@localhost:5432/db", pool_size=5)
    # SQLAlchemy obscures the password in `repr`, but the DSN parts are reachable
    # via `engine.url`.
    assert engine.url.host == "localhost"
    assert engine.url.database == "db"
    assert engine.pool.size() == 5


def test_make_session_factory_round_trip() -> None:
    engine = make_engine("postgresql+asyncpg://u:p@localhost:5432/db")
    factory = make_session_factory(engine)
    assert factory is not None


@pytest.mark.asyncio
async def test_session_scope_commits_on_clean_exit() -> None:
    session = AsyncMock()
    factory = MagicMock()
    factory.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.return_value.__aexit__ = AsyncMock(return_value=None)

    async with session_scope(factory) as s:
        assert s is session

    session.commit.assert_awaited_once()
    session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_scope_rolls_back_on_exception() -> None:
    session = AsyncMock()
    factory = MagicMock()
    factory.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.return_value.__aexit__ = AsyncMock(return_value=None)

    with pytest.raises(RuntimeError):
        async with session_scope(factory):
            raise RuntimeError("boom")

    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()
