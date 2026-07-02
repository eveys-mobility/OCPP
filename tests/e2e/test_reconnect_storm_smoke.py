"""E4-7 — `reconnect_storm` scenario against the in-process gateway.

Acceptance per #19: scenario runs, fleet recovers within the window,
report shows pass/fail. This smoke uses a tiny fleet (10 chargers,
3s settle, 15s recovery window) so it fits the e2e tier's budget;
the real "2k chargers, 60s window" test is what the operator runs
on staging via `python -m tools.load --scenario reconnect_storm
--full`.

Reuses `running_service` from `test_local_smoke.py` (in-process
single-pod gateway). The single-pod limitation (no cross-pod
rebalance exercised) is acknowledged in the scenario's report
notes — that's what staging is for.
"""

from __future__ import annotations

import pytest
from tools.load.scenarios.reconnect_storm import ReconnectStormConfig, run

from tests.e2e.test_local_smoke import _TEST_WS_PORT, running_service
from tests.e2e.test_simulator_smoke import _seed_fleet_cp_ids

# Re-export the upstream fixture so pytest discovers it for our module.
__all__ = ["running_service"]


@pytest.mark.asyncio
async def test_reconnect_storm_recovers_within_window(
    running_service: None,
) -> None:
    """Compressed shape: 10 chargers, 3s settle, 15s recovery window.
    The full spec shape (2k / 60s) is for staging."""
    # Pre-authorize the storm fleet — the reconnect scenario dials
    # thousands of upgrades and would be CALLERROR'd on every non-Boot
    # frame otherwise. Same seed-by-prefix helper `test_simulator_smoke`
    # uses.
    await _seed_fleet_cp_ids("STORM_E4_7_SMOKE", 10)
    result = await run(
        ReconnectStormConfig(
            count=10,
            settle_seconds=3.0,
            drop_fraction=0.5,
            recovery_window_seconds=15.0,
            target_url=f"ws://localhost:{_TEST_WS_PORT}",
            cp_id_prefix="STORM_E4_7_SMOKE",
        )
    )

    # Surface the report to the test failure message so a CI break
    # gets the same diagnostic the operator would see.
    detail = (
        f"\nscenario={result.name} passed={result.passed} "
        f"duration={result.duration_seconds:.1f}s\n"
        + "\n".join(
            f"  {c.name}: actual={c.actual!r} threshold={c.threshold!r} passed={c.passed}"
            for c in result.criteria
        )
        + "\nnotes:\n"
        + "\n".join(f"  - {n}" for n in result.notes)
    )

    # Two structural assertions — pass overall, and three criteria
    # rendered (any future addition to `run()` lights up here).
    assert result.passed, f"reconnect_storm scenario failed:{detail}"
    assert len(result.criteria) == 3, detail
