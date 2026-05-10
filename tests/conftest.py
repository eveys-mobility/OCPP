"""Shared pytest fixtures."""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from eveys_ocpp.settings import Settings


@pytest.fixture(autouse=True)
def _strip_eveys_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Make every unit test hermetic against the developer's local `.env`.

    `Settings()` reads `EVEYS_OCPP_*` from two places: the process
    env, and the `.env` file at the repo root (via pydantic-settings).
    A test that asserts a setting's *default* value will silently
    pass or fail depending on what the developer has configured. CI
    runners have no `.env` and are accidentally hermetic; local
    machines aren't. The flake fixed in #155 was the latest example
    (`EVEYS_OCPP_BACKEND_AUTHORIZE_FALLBACK=accept_offline` in the
    dev `.env` flipped the test's expected status from Invalid to
    Accepted).

    Strategy — block both leak paths:

    1. `monkeypatch.delenv` every `EVEYS_OCPP_*` from the process env
       so a developer who exported one in their shell can't poison
       a test.
    2. `monkeypatch.setitem` the `Settings.model_config["env_file"]`
       to `None` so pydantic-settings doesn't read `.env` from disk
       during this test. Production env loading
       (Settings.model_config.env_file = ".env") is unchanged outside
       the patch's scope — `monkeypatch` reverts it on test teardown.

    Tests that need a specific value use `monkeypatch.setenv` /
    `settings_factory` and those still take effect (process-env
    overrides win over the empty-default after this fixture has run).

    Out of scope: e2e and compose-smoke tiers — those legitimately
    need real env (DSNs, ports, tokens). This fixture lives in
    `tests/conftest.py` and applies to anything that imports it; the
    e2e and compose-smoke dirs have their own conftests and don't
    inherit autouse fixtures from a parent that they don't import.
    """
    for key in list(os.environ):
        if key.startswith("EVEYS_OCPP_"):
            monkeypatch.delenv(key, raising=False)
    # Block .env file load by pointing pydantic-settings at nothing.
    # `Settings.model_config` is a dict on the class; monkeypatch.setitem
    # reverts it cleanly on teardown.
    monkeypatch.setitem(Settings.model_config, "env_file", None)
    yield


@pytest.fixture(autouse=True)
def _disable_metrics_server(
    _strip_eveys_env: None,  # autouse-ordering: env strip MUST run first
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[None]:
    """Force the Prometheus scrape server off across the unit suite.

    Boots are short-lived in tests and `prometheus_client.start_http_server`
    binds a real socket on port 9100. Two test processes overlapping (or
    one process's `_serve_all` running twice in pytest's same interpreter)
    would race on the bind. The `metrics_enabled=False` path skips the
    bind cleanly; counters/histograms still increment in-process so
    every per-emitter assertion still works.

    Depends on `_strip_eveys_env` so its `monkeypatch.setenv` runs
    *after* the env strip — otherwise the strip would wipe this knob.

    Tests that specifically exercise `MetricsServer` (e.g.
    `tests/unit/metrics/test_server.py`) override this on a fixture-by-
    fixture basis by monkeypatching the env var back on, OR construct
    the `MetricsServer` with an ephemeral port directly.
    """
    monkeypatch.setenv("EVEYS_OCPP_METRICS_ENABLED", "false")
    yield


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

    `registry`, `event_producer`, `backend_client`, and
    `authorize_cache` default to None — tests that need them set the
    attribute directly.
    """
    cp = MagicMock()
    cp.id = "TEST_CP_001"
    cp.settings = settings
    cp.session_factory = fake_session_factory
    cp.registry = None
    cp.event_producer = None
    cp.backend_client = None
    cp.authorize_cache = None
    return cp
