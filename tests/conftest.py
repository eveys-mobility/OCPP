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
def settings_factory() -> Any:
    """Build a `Settings` with arbitrary keyword overrides.

    Pydantic's `Settings(...)` constructor accepts field overrides
    directly when `frozen=True` is set on `model_config`; this
    factory wraps that for ergonomics in handler tests that want to
    flip one knob (e.g. `backend_authorize_fallback`).
    """

    def _build(**overrides: Any) -> Settings:
        return Settings(**overrides)

    return _build


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
    """A stand-in for EveysChargePoint with the fields handlers touch.

    `registry`, `event_producer`, and `backend_client` default to None —
    tests that need them set the attribute directly.
    """
    cp = MagicMock()
    cp.id = "TEST_CP_001"
    cp.settings = settings
    cp.session_factory = fake_session_factory
    cp.registry = None
    cp.event_producer = None
    cp.backend_client = None
    return cp
