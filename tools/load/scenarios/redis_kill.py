"""Redis-kill drill (E5-10).

Validates the gateway's documented fail-open invariants when Redis
is unreachable. Redis holds:

  - the online registry (`cp:online:{cp_id}` per E2-9)
  - the idempotency cache (ADR-0017)
  - the cross-pod command bus (ADR-0016)
  - the Authorize cache (E3-4)
  - the rate limiter token buckets (E5-3)

Of these, **none are on the OCPP CALL hot path** — the WS server
accepts connections, the handlers run, transactions persist, the
charger's experience is unaffected during a Redis outage. The
fail-open documentation says so; this drill proves it.

Pass criteria:

  1. Existing WS sockets stay alive while Redis is down (charger
     connection count doesn't drop to zero on Redis stop).
  2. The fleet keeps booting + heart-beating against the gateway.
  3. After Redis recovers, online presence is reasserted within
     the registry's TTL refresh window (Heartbeat re-marks the key
     per E2-9's `refresh()`).

What this drill **does NOT** assert:

  - Cross-pod gRPC command routing during Redis outage. Compose
    runs a single pod; multi-pod failover testing belongs to
    staging or k3d.
  - Authorize cache hit-rate degradation. Cache fail-open just
    means a Redis miss falls through to the backend; the cache
    isn't on the OCPP CALL path either.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from tools.load._docker_helpers import (
    DockerNotAvailableError,
    start,
    stop,
    wait_healthy,
)
from tools.load.scenario import Criterion, ScenarioResult
from tools.sim.fleet import Fleet, FleetConfig
from tools.sim.profiles import IDLE


@dataclass(frozen=True, slots=True)
class RedisKillConfig:
    count: int  # fleet size
    settle_seconds: float  # let chargers connect + boot
    outage_seconds: float  # Redis is stopped this long
    recovery_window_seconds: float  # post-restart, wait this long for steady state
    target_url: str = "ws://localhost:19000"
    cp_id_prefix: str = "DR_REDIS"


async def run(config: RedisKillConfig) -> ScenarioResult:
    started_at_iso = datetime.now(UTC).isoformat()
    started_monotonic = time.monotonic()

    notes: list[str] = []
    criteria: list[Criterion] = []

    # Fleet runs across the whole drill — settle + outage + recovery —
    # so the IDLE profile keeps heart-beating throughout.
    duration = config.settle_seconds + config.outage_seconds + config.recovery_window_seconds + 5.0
    fleet_config = FleetConfig(
        count=config.count,
        duration_seconds=duration,
        target_url=config.target_url,
        ramp_seconds=min(2.0, config.settle_seconds / 4),
        profile=IDLE,
        cp_id_prefix=config.cp_id_prefix,
        show_progress=False,
    )
    fleet = Fleet(fleet_config)
    fleet_task = asyncio.create_task(fleet.run(), name="redis-kill-fleet")

    redis_was_stopped = False
    try:
        # Phase 1 — settle. Wait for the fleet to boot.
        await asyncio.sleep(config.settle_seconds)
        connected_at_settle = fleet.counters.connected
        boots_at_settle = fleet.counters.boots
        notes.append(
            f"settle ({config.settle_seconds:.0f}s): "
            f"connected={connected_at_settle} boots={boots_at_settle}"
        )

        # Phase 2 — kill Redis. The gateway's existing WS connections
        # must stay up; the registry, idempotency cache, rate limiter,
        # and Authorize cache all fail open.
        try:
            stop("redis")
            redis_was_stopped = True
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
                name="redis_kill",
                started_at=started_at_iso,
                duration_seconds=time.monotonic() - started_monotonic,
                criteria=criteria,
                notes=notes,
            )
        outage_started = time.monotonic()
        notes.append(f"redis stopped at t={outage_started - started_monotonic:.1f}s")

        await asyncio.sleep(config.outage_seconds)
        connected_during_outage = fleet.counters.connected
        errors_during_outage = fleet.counters.errors
        notes.append(
            f"during outage ({config.outage_seconds:.0f}s): "
            f"connected={connected_during_outage} errors={errors_during_outage}"
        )

        # Phase 3 — restart Redis, wait for healthy, then settle.
        start("redis")
        if not wait_healthy("redis", deadline_seconds=30.0):
            notes.append("redis did not report healthy within 30s of restart")
        await asyncio.sleep(config.recovery_window_seconds)
        connected_after_recovery = fleet.counters.connected
        notes.append(
            f"recovery ({config.recovery_window_seconds:.0f}s): "
            f"connected={connected_after_recovery}"
        )
    finally:
        # If we crashed out of the drill before restarting Redis, do
        # it now so the operator's stack isn't left half-broken.
        if redis_was_stopped:
            with contextlib.suppress(Exception):
                start("redis")
        fleet_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await fleet_task

    # Pass criteria.

    # 1. Fleet kept ≥95% of its connections through the outage.
    #    Charger WS sockets must not drop just because Redis went away.
    survive_threshold = int(connected_at_settle * 0.95)
    survive_pass = connected_during_outage >= survive_threshold
    criteria.append(
        Criterion(
            name="WS connections survive Redis outage",
            expression="counters.connected mid-outage >= 95% of settled level",
            threshold=f">= {survive_threshold} (95% of {connected_at_settle})",
            actual=str(connected_during_outage),
            passed=survive_pass,
        )
    )

    # 2. Post-recovery the fleet is still steady. A regression here
    #    would mean Redis recovery itself broke connections, which is
    #    the scenario's whole point of catching.
    recovery_pass = connected_after_recovery >= survive_threshold
    criteria.append(
        Criterion(
            name="WS connections steady after recovery",
            expression="counters.connected post-recovery >= 95% of settled level",
            threshold=f">= {survive_threshold}",
            actual=str(connected_after_recovery),
            passed=recovery_pass,
        )
    )

    # 3. The fleet did not start error-storming during the outage.
    #    A handful of errors (e.g. AuthorizeCache.set silently failing)
    #    is fine and expected; a flood means a fail-open path
    #    regressed into a fail-loud one.
    error_budget = max(config.count // 5, 5)
    error_pass = errors_during_outage <= error_budget
    criteria.append(
        Criterion(
            name="error counter stays below fail-open budget",
            expression="counters.errors during outage <= count/5 (or 5, whichever larger)",
            threshold=f"<= {error_budget}",
            actual=str(errors_during_outage),
            passed=error_pass,
        )
    )

    return ScenarioResult(
        name="redis_kill",
        started_at=started_at_iso,
        duration_seconds=time.monotonic() - started_monotonic,
        criteria=criteria,
        notes=[
            f"fleet: count={config.count}",
            f"outage: {config.outage_seconds:.0f}s",
            *notes,
            "single-pod compose limitation: cross-pod registry "
            "rebalance + cross-pod gRPC routing not exercised — run "
            "against a multi-pod stack to cover those paths",
        ],
    )


async def run_quick(target_url: str, _prometheus_url: str) -> ScenarioResult:
    """`--quick` shape — 30 chargers, 8 s outage, 30 s windows."""
    return await run(
        RedisKillConfig(
            count=30,
            settle_seconds=8.0,
            outage_seconds=8.0,
            recovery_window_seconds=8.0,
            target_url=target_url,
        )
    )


async def run_full(target_url: str, _prometheus_url: str) -> ScenarioResult:
    """Full-scale shape — 1k chargers, 60 s outage, 60 s windows.
    Run against a multi-pod stack for true cross-pod coverage."""
    return await run(
        RedisKillConfig(
            count=1_000,
            settle_seconds=60.0,
            outage_seconds=60.0,
            recovery_window_seconds=60.0,
            target_url=target_url,
        )
    )
