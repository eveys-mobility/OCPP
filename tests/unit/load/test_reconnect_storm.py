"""ReconnectStorm scenario — config defaults + run-quick/full shapes.

The full `run()` integrates with a live gateway over WS, so it lives
in the e2e tier (or operator-driven via `python -m tools.load
--scenario reconnect_storm`). The unit tests here lock down the
config defaults and the smoke that `run_quick` / `run_full` produce
sensible config shapes — both spec-relevant constants the scenario
must not silently drift from.
"""

from __future__ import annotations

import inspect

from tools.load.scenarios import reconnect_storm


def test_config_default_drop_fraction_is_50_percent() -> None:
    """Spec: 'kill 50% of pods'. Default must match without callers
    needing to specify it explicitly."""
    config = reconnect_storm.ReconnectStormConfig(count=10, settle_seconds=1.0, target_url="ws://x")
    assert config.drop_fraction == 0.5


def test_config_default_recovery_window_is_60s() -> None:
    """Spec: 'recovers within 60 seconds'."""
    config = reconnect_storm.ReconnectStormConfig(count=10, settle_seconds=1.0, target_url="ws://x")
    assert config.recovery_window_seconds == 60.0


def test_run_full_uses_spec_headline_numbers() -> None:
    """`--full` shape must match the roadmap's headline numbers
    (2k chargers, 60s window). A regression here would silently
    weaken the staging signal."""
    # We don't actually invoke run_full (it'd hit a real gateway);
    # we inspect its body for the literal config it builds. This is
    # uglier than calling the function but lets the test stay in the
    # unit tier. If `run_full` grows logic this approach won't scale,
    # at which point we extract a `_full_config()` helper.
    source = inspect.getsource(reconnect_storm.run_full)
    assert "count=2_000" in source
    assert "recovery_window_seconds=60.0" in source


def test_run_quick_uses_short_settle_and_window() -> None:
    """`--quick` must finish in under ~45s end-to-end so it stays
    within the rig's <2-minute target alongside other scenarios."""
    source = inspect.getsource(reconnect_storm.run_quick)
    # 5s settle + 30s window + ~5s buffer = ~40s total worst case.
    assert "settle_seconds=5.0" in source
    assert "recovery_window_seconds=30.0" in source
    assert "count=50" in source
