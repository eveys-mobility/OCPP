"""E4-5 — `tools.sim` against a real local stack.

Acceptance per #17: 5 chargers against a live `make compose-up` stack,
run for 30s, assert at least one transaction lands in Postgres.

Reuses the data-plane reachability gate + `running_service` fixture
from `test_local_smoke.py` so the same `make compose-up` →
`alembic upgrade head` → `pytest` flow exercises both. The fleet
points at the same in-process gateway the smoke test spawns.

We compress the spec's 30s to a tighter window with a
`transaction_start_per_minute` override so a transaction is virtually
certain inside the budget — keeps CI fast without losing the "real
Postgres + real Kafka + real WS round-trip" coverage.
"""

from __future__ import annotations

import os

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from tools.sim.charger import Counters
from tools.sim.fleet import Fleet, FleetConfig
from tools.sim.profiles import REALISTIC, BehaviourProfile

from tests.e2e.test_local_smoke import _TEST_DB_URL, _TEST_WS_PORT, running_service

# Re-export the upstream fixture so pytest discovers it for our module.
__all__ = ["running_service"]


# Tighter per-minute rate so the 6-second window is virtually certain
# to drive at least one transaction across 5 chargers. The realistic
# profile's 1/hour rate would need ~12 minutes of run time to give
# even a 50/50 shot — fine for E4-6's actual load test, too slow for
# this smoke.
_FAST_PROFILE = BehaviourProfile(
    name="smoke",
    transaction_start_per_minute=600.0,  # ~10/sec/charger
    session_length_seconds_mean=2.0,
    meter_values_period_seconds=1.0,
    heartbeat_period_seconds=30.0,
    disconnect_per_minute=0.0,
)


@pytest.mark.asyncio
async def test_simulator_drives_transactions_through_real_gateway(
    running_service: None,
) -> None:
    config = FleetConfig(
        count=5,
        duration_seconds=6.0,
        target_url=f"ws://localhost:{_TEST_WS_PORT}",
        ramp_seconds=0.5,
        profile=_FAST_PROFILE,
        cp_id_prefix="SIM_E4_5_SMOKE",
        show_progress=False,
    )
    fleet = Fleet(config)
    counters: Counters = await fleet.run()

    # The fleet ran end-to-end without exceptions tearing it down.
    assert counters.boots >= 1, (
        f"no BootNotification accepted — gateway not reachable? boots={counters.boots}"
    )

    # Verify a transaction landed in Postgres for one of our cp_ids.
    # Join through `charge_points` because `transactions.charge_point_id`
    # is the FK to `charge_points.id` (bigint), not the cp_id string —
    # that one lives on `charge_points.cp_id`.
    engine = create_async_engine(_TEST_DB_URL)
    try:
        async with engine.connect() as conn:
            row = (
                await conn.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM transactions t "
                        "JOIN charge_points cp ON cp.id = t.charge_point_id "
                        "WHERE cp.cp_id LIKE :prefix"
                    ),
                    {"prefix": f"{config.cp_id_prefix}%"},
                )
            ).scalar()
    finally:
        await engine.dispose()

    assert row is not None and int(row) >= 1, (
        f"no transactions in Postgres for prefix {config.cp_id_prefix} "
        f"(counters: boots={counters.boots} txns={counters.transactions} "
        f"errors={counters.errors})"
    )


@pytest.mark.asyncio
async def test_simulator_idle_profile_connects_without_starting_transactions(
    running_service: None,
) -> None:
    """Sanity: idle profile boots chargers + sends Heartbeats but
    never starts a transaction. Smoke for E4-7 reconnect-storm
    warmup which uses a similar idle/churning shape."""
    config = FleetConfig(
        count=3,
        duration_seconds=3.0,
        target_url=f"ws://localhost:{_TEST_WS_PORT}",
        ramp_seconds=0.3,
        profile=REALISTIC,  # but we'll bound by short duration
        cp_id_prefix="SIM_E4_5_IDLE",
        show_progress=False,
    )
    # Use REALISTIC's 1/hour rate so we expect zero transactions in 3s.
    counters = await Fleet(config).run()
    assert counters.boots >= 1
    # Realistic has 1/hour transaction probability in 3s = ~0.083% per
    # charger. Accept zero or one as "well within expectation".
    assert counters.transactions <= 1


def test_module_runs_via_python_dash_m() -> None:
    """`python -m tools.sim --help` must exit 0 — verifies the script
    entry doesn't break import-side. Cheap regression guard for the
    decision to ship the simulator without a `[project.scripts]` entry."""
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-m", "tools.sim", "--help"],
        capture_output=True,
        text=True,
        timeout=10,
        env={**os.environ, "PYTHONPATH": "."},
    )
    assert result.returncode == 0, f"--help failed: {result.stderr}"
    assert "eveys-ocpp-sim" in result.stdout
