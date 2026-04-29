"""Shared pytest fixtures."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from eveys_ocpp.settings import Settings


@pytest.fixture
def settings() -> Settings:
    """Default Settings — overridable by `monkeypatch.setenv` in tests."""
    return Settings()


@pytest.fixture
def fake_session() -> AsyncMock:
    """An AsyncSession stand-in. Each repository function is patched per test."""
    session = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def fake_session_factory(fake_session: AsyncMock) -> Any:
    """Factory whose `__aenter__` yields `fake_session`."""

    class _Ctx:
        async def __aenter__(self) -> AsyncMock:
            return fake_session

        async def __aexit__(self, *exc: object) -> None:
            return None

    class _Factory:
        def __call__(self) -> _Ctx:
            return _Ctx()

    return _Factory()


@pytest.fixture
def fake_cp(settings: Settings, fake_session_factory: Any) -> MagicMock:
    """A stand-in for EveysChargePoint with the fields handlers touch."""
    cp = MagicMock()
    cp.id = "TEST_CP_001"
    cp.settings = settings
    cp.session_factory = fake_session_factory
    return cp
