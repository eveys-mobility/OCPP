"""Postgres-kill drill (E5-10).

Validates SLO 4 (transaction durability = 100%) under a Postgres
primary outage. Drives N transactions through to completion, kills
Postgres, restarts it, then queries the `transactions` table
directly to verify every acked transaction persisted.

Why this is a meaningful drill: Postgres is the system of record
for billing-relevant transaction state (per ADR-0004 — ClickHouse
holds the meter-value firehose, Postgres holds StartTransaction /
StopTransaction). A drill that ignored Postgres would miss the
single most important durability invariant in the gateway.

What this drill **does** assert:

  1. Every transaction the gateway acknowledged (counters.transactions
     incremented) has a row in `transactions` after Postgres recovers.
  2. Postgres comes back healthy within a configurable deadline
     (validates `make compose-up` recovery story).

What this drill **does NOT** assert:

  - Transactions started *during* the outage. The gateway returns
    HandlerError to the charger (Postgres unreachable), and the
    charger retries — that's a separate flow. Adding it would
    require driving in-flight stops during the kill window, which
    is more orchestration than this drill needs.
  - Replication lag / read-replica fallover. Compose runs a single
    Postgres; HA is a staging concern.
  - Rollback of partial transactions during the kill — that's
    Postgres's own WAL story, not a gateway invariant.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import asyncpg

from eveys_ocpp.settings import get_settings
from tools.load._docker_helpers import (
    DockerNotAvailableError,
    start,
    stop,
    wait_healthy,
)
from tools.load.scenario import Criterion, ScenarioResult
from tools.sim.fleet import Fleet, FleetConfig
from tools.sim.profiles import BehaviourProfile


@dataclass(frozen=True, slots=True)
class PostgresKillConfig:
    count: int  # fleet size
    pre_kill_seconds: float  # let fleet drive transactions to completion
    outage_seconds: float  # Postgres is stopped this long
    recovery_window_seconds: float  # post-restart, wait this long
    target_url: str = "ws://localhost:19000"
    cp_id_prefix: str = "DR_PG"


# A short-session profile that drives transactions quickly. We want
# the fleet to *complete* transactions before we kill Postgres so
# the durability claim is testable.
_TX_DRIVER_PROFILE = BehaviourProfile(
    name="dr-tx-driver",
    # ~1 tx per minute per charger — high enough that 30 s of fleet
    # time for 20 chargers reliably produces ≥10 completed sessions.
    transaction_start_per_minute=2.0,
    # 15 s sessions so they complete within the pre-kill window.
    session_length_seconds_mean=15.0,
    meter_values_period_seconds=5.0,
    heartbeat_period_seconds=60.0,
    disconnect_per_minute=0.0,
)


async def _count_persisted_transactions(cp_id_prefix: str) -> int:
    """Count rows in `transactions` whose charge point matches the
    fleet's prefix. We can't filter by exact cp_ids without enumerating
    them, but the prefix scope is sufficient — no other test or
    operator activity uses the `DR_PG_*` prefix."""
    settings = get_settings()
    # Convert the SQLAlchemy DSN (`postgresql+asyncpg://...`) to the
    # asyncpg-native DSN (`postgresql://...`). asyncpg.connect doesn't
    # understand the `+asyncpg` driver suffix.
    raw_dsn = settings.db_url.get_secret_value().replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn=raw_dsn)
    try:
        # `transactions` joins to `charge_points.cp_id` via FK.
        row = await conn.fetchrow(
            """
            SELECT COUNT(*) AS n
            FROM transactions t
            JOIN charge_points cp ON cp.id = t.charge_point_id
            WHERE cp.cp_id LIKE $1
            """,
            f"{cp_id_prefix}%",
        )
        return int(row["n"]) if row else 0
    finally:
        await conn.close()


async def run(config: PostgresKillConfig) -> ScenarioResult:
    started_at_iso = datetime.now(UTC).isoformat()
    started_monotonic = time.monotonic()

    notes: list[str] = []
    criteria: list[Criterion] = []

    duration = (
        config.pre_kill_seconds + config.outage_seconds + config.recovery_window_seconds + 5.0
    )
    fleet_config = FleetConfig(
        count=config.count,
        duration_seconds=duration,
        target_url=config.target_url,
        ramp_seconds=2.0,
        profile=_TX_DRIVER_PROFILE,
        cp_id_prefix=config.cp_id_prefix,
        show_progress=False,
    )
    fleet = Fleet(fleet_config)
    fleet_task = asyncio.create_task(fleet.run(), name="pg-kill-fleet")

    pg_was_stopped = False
    healthy: bool = False
    persisted: int = -1
    acked_before_kill = 0
    try:
        # Phase 1 — drive transactions to completion before the kill.
        await asyncio.sleep(config.pre_kill_seconds)
        acked_before_kill = fleet.counters.transactions
        notes.append(
            f"pre-kill ({config.pre_kill_seconds:.0f}s): acked transactions={acked_before_kill}"
        )

        # Phase 2 — kill Postgres. We deliberately don't drive new
        # transactions during this window; the durability claim is
        # about *acked* transactions surviving the outage.
        try:
            stop("postgres")
            pg_was_stopped = True
        except DockerNotAvailableError as exc:
            criteria.append(
                Criterion(
                    name="docker compose available",
                    expression="shutil.which('docker') and compose file exists",
                    threshold="True",
                    actual=f"False ({exc})",
                    passed=False,
                )
            )
            return ScenarioResult(
                name="postgres_kill",
                started_at=started_at_iso,
                duration_seconds=time.monotonic() - started_monotonic,
                criteria=criteria,
                notes=notes,
            )
        outage_started = time.monotonic()
        notes.append(f"postgres stopped at t={outage_started - started_monotonic:.1f}s")

        await asyncio.sleep(config.outage_seconds)

        # Phase 3 — restart and wait for healthy.
        start("postgres")
        healthy = wait_healthy("postgres", deadline_seconds=30.0)
        if not healthy:
            notes.append("postgres did not report healthy within 30s of restart")
        await asyncio.sleep(config.recovery_window_seconds)

        # Phase 4 — count persisted transactions. If the kill rolled
        # back any acked transaction, this count is below the gateway's
        # ack count and the criterion fails.
        try:
            persisted = await _count_persisted_transactions(config.cp_id_prefix)
        except Exception as exc:
            notes.append(f"persistence query failed: {exc}")
            persisted = -1
    finally:
        if pg_was_stopped:
            with contextlib.suppress(Exception):
                start("postgres")
        fleet_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await fleet_task

    notes.append(f"persisted in postgres after recovery: {persisted}")

    # Pass criteria.

    # 1. Postgres health check came back. If it didn't, the rest of
    #    the criteria are unreliable.
    criteria.append(
        Criterion(
            name="postgres healthy after restart",
            expression="`docker compose ps postgres` reports healthy",
            threshold="healthy within 30s",
            actual=str(healthy),
            passed=bool(healthy),
        )
    )

    # 2. Durability — every acked transaction has a row.
    durability_pass = persisted >= acked_before_kill
    criteria.append(
        Criterion(
            name="acked transactions survived the kill",
            expression="COUNT(*) FROM transactions WHERE cp_id LIKE prefix% >= "
            "fleet.counters.transactions at kill time",
            threshold=f">= {acked_before_kill}",
            actual=str(persisted),
            passed=durability_pass,
        )
    )

    return ScenarioResult(
        name="postgres_kill",
        started_at=started_at_iso,
        duration_seconds=time.monotonic() - started_monotonic,
        criteria=criteria,
        notes=[
            f"fleet: count={config.count}",
            *notes,
            "single-pod compose limitation: replication lag / read-replica "
            "fallover not exercised — staging or k3d targets are required",
        ],
    )


async def run_quick(target_url: str, _prometheus_url: str) -> ScenarioResult:
    """`--quick` shape — 20 chargers, 30 s pre-kill, 8 s outage, 8 s recovery."""
    return await run(
        PostgresKillConfig(
            count=20,
            pre_kill_seconds=30.0,
            outage_seconds=8.0,
            recovery_window_seconds=8.0,
            target_url=target_url,
        )
    )


async def run_full(target_url: str, _prometheus_url: str) -> ScenarioResult:
    """Full-scale shape — 200 chargers, 5 min pre-kill, 30 s outage."""
    return await run(
        PostgresKillConfig(
            count=200,
            pre_kill_seconds=300.0,
            outage_seconds=30.0,
            recovery_window_seconds=30.0,
            target_url=target_url,
        )
    )
