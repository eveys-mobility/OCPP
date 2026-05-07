"""Reconnect-storm scenario — drop half the fleet, watch recovery.

Maps to the roadmap pass criterion "fleet recovers within 60s after
50% pod kill". On a single-pod compose stack we can't actually kill
pods, so we simulate the same shock at the WS layer: drop 50% of the
chargers' WS connections, then verify they all reconnect inside the
recovery window.

What this exercises (the spec's predicted hot-spots):

  - Re-register storm — every dropped charger boots again, hammering
    BootNotification + the idempotency cache (boots that arrive with
    the same `(cp_id, vendor, model)` are detected as replays).
  - Authorize cache — the storm's chargers all hit Authorize again
    (when they next start a transaction); cache hit-rate should
    visibly lift if the cache is doing its job. v0 doesn't drive
    transactions during the storm, so this is documented as a gap
    rather than asserted.
  - Per-pod registry gauge — drops + recovers as connections
    re-attach to the (single) surviving pod.

What this does NOT exercise (single-pod compose limitation):

  - Cross-pod registry rebalance (no second pod to take over).
  - Backend HTTP saturation under multi-pod boot fan-out.

For those, run the same scenario against a multi-pod k3d / staging
deployment — the scenario code is identical, only the target URL
changes.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from tools.load.scenario import Criterion, ScenarioResult
from tools.sim.fleet import Fleet, FleetConfig
from tools.sim.profiles import IDLE


@dataclass(frozen=True, slots=True)
class ReconnectStormConfig:
    count: int  # total fleet size
    settle_seconds: float  # let the fleet boot + idle before the storm
    drop_fraction: float = 0.5  # 0.5 = half the fleet dropped at the storm
    recovery_window_seconds: float = 60.0  # spec's pass bound
    target_url: str = "ws://localhost:19000"
    cp_id_prefix: str = "LOAD_STORM"


async def run(config: ReconnectStormConfig) -> ScenarioResult:
    started_at_iso = datetime.now(UTC).isoformat()
    started_monotonic = time.monotonic()

    fleet_config = FleetConfig(
        count=config.count,
        # Settle + recovery window = total run time. We add a small
        # buffer so the recovery measurement isn't truncated by fleet
        # cancellation.
        duration_seconds=config.settle_seconds + config.recovery_window_seconds + 5.0,
        target_url=config.target_url,
        ramp_seconds=min(2.0, config.settle_seconds / 4),
        profile=IDLE,
        cp_id_prefix=config.cp_id_prefix,
        show_progress=False,
    )
    fleet = Fleet(fleet_config)

    # Run the fleet as a background task so we can interleave the
    # storm trigger and recovery measurement.
    fleet_task = asyncio.create_task(fleet.run(), name="storm-fleet")

    notes: list[str] = []
    try:
        # Phase 1 — settle. Wait for ~all chargers to be connected.
        await asyncio.sleep(config.settle_seconds)
        connected_at_settle = fleet.counters.connected
        boots_at_settle = fleet.counters.boots
        errors_at_settle = fleet.counters.errors
        notes.append(
            f"after settle ({config.settle_seconds:.0f}s): "
            f"connected={connected_at_settle} boots={boots_at_settle} "
            f"errors={errors_at_settle}"
        )

        # Phase 2 — storm. Drop the requested fraction.
        storm_at = time.monotonic()
        dropped = await fleet.drop_random(config.drop_fraction)
        notes.append(
            f"storm: dropped {dropped} of {config.count} "
            f"({100 * dropped / max(1, config.count):.0f}%) at "
            f"t={storm_at - started_monotonic:.1f}s"
        )

        # Phase 3 — recovery. Watch the boots counter: when each
        # dropped charger has booted again, the fleet has recovered.
        # We can't poll `connected` alone because its decrement (in
        # `_one_session`'s finally) lags the actual close — a polling
        # loop would see `connected == settle_level` for one tick
        # before the dropped sessions tear down, producing a false
        # "instant recovery" signal.
        recovery_target_boots = boots_at_settle + dropped
        recovered_at: float | None = None
        deadline = storm_at + config.recovery_window_seconds
        while time.monotonic() < deadline:
            await asyncio.sleep(1.0)
            if fleet.counters.boots >= recovery_target_boots:
                recovered_at = time.monotonic()
                break
        recovery_seconds = (recovered_at - storm_at) if recovered_at else None
    finally:
        # We don't need the rest of the duration — cancel the fleet
        # task explicitly so the scenario exits promptly.
        fleet_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await fleet_task

    duration = time.monotonic() - started_monotonic
    final_counters = fleet.counters

    criteria: list[Criterion] = []

    # Criterion 1 — fleet reached the expected size before the storm.
    # If only 80% of chargers booted in time, the recovery measurement
    # is meaningless.
    settle_pass = connected_at_settle >= int(config.count * 0.95)
    criteria.append(
        Criterion(
            name="fleet settled before storm",
            expression=f"connected ({connected_at_settle}) >= 95% of count ({config.count})",
            threshold=f">= {int(config.count * 0.95)}",
            actual=str(connected_at_settle),
            passed=settle_pass,
        )
    )

    # Criterion 2 — recovery within window.
    if recovery_seconds is not None:
        recovery_pass = recovery_seconds <= config.recovery_window_seconds
        recovery_str = f"{recovery_seconds:.1f}s"
    else:
        recovery_pass = False
        recovery_str = (
            f"did not recover within {config.recovery_window_seconds:.0f}s "
            f"(connected={final_counters.connected})"
        )
    criteria.append(
        Criterion(
            name="fleet recovered within window",
            expression="time from storm until counters.connected >= settled level",
            threshold=f"<= {config.recovery_window_seconds:.0f}s",
            actual=recovery_str,
            passed=recovery_pass,
        )
    )

    # Criterion 3 — the storm produced re-boots (sanity that the test
    # itself worked). The dropped chargers should each boot again.
    boots_during_recovery = final_counters.boots - boots_at_settle
    reboot_pass = boots_during_recovery >= max(1, dropped // 2)
    criteria.append(
        Criterion(
            name="dropped chargers re-booted",
            expression=f"boots after settle ({boots_during_recovery}) "
            f">= half of dropped ({dropped // 2})",
            threshold=f">= {max(1, dropped // 2)}",
            actual=str(boots_during_recovery),
            passed=reboot_pass,
        )
    )

    return ScenarioResult(
        name="reconnect_storm",
        started_at=started_at_iso,
        duration_seconds=duration,
        criteria=criteria,
        notes=[
            f"fleet: count={config.count} drop_fraction={config.drop_fraction}",
            *notes,
            f"final counters: connected={final_counters.connected} "
            f"boots={final_counters.boots} errors={final_counters.errors}",
            "single-pod compose limitation: cross-pod registry rebalance "
            "not exercised; run against a multi-pod stack to cover that path",
        ],
    )


async def run_quick(target_url: str, _prometheus_url: str) -> ScenarioResult:
    """`--quick` shape — 50 chargers, 5s settle, 30s recovery window."""
    return await run(
        ReconnectStormConfig(
            count=50,
            settle_seconds=5.0,
            recovery_window_seconds=30.0,
            target_url=target_url,
        )
    )


async def run_full(target_url: str, _prometheus_url: str) -> ScenarioResult:
    """Full-scale shape — the spec's headline numbers (2k chargers,
    60s recovery). Run against a multi-pod stack for true coverage."""
    return await run(
        ReconnectStormConfig(
            count=2_000,
            settle_seconds=60.0,
            recovery_window_seconds=60.0,
            target_url=target_url,
        )
    )
