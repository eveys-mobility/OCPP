"""Boot-storm scenario — N chargers connect inside `ramp_seconds`.

Maps to the roadmap pass criterion "10k concurrent chargers
maintained for 1 hour" and "P95 RemoteStart end-to-end < 3s" — but
in v0 we only assert two cheap derivative criteria so the scenario
runs against `make compose-up` without a multi-hour budget:

  - ws_connections_active reaches `count` within `ramp_seconds + 30s`
  - handler P99 latency for `BootNotification` stays under
    `boot_p99_threshold_seconds`

The "1 hour / 10k chargers" full-scale check is what the operator
runs on staging — same scenario, just with `count=10000` and
`hold_seconds=3600`. The CI-grade `--quick` pass uses tiny numbers.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from tools.load.prometheus import PrometheusClient
from tools.load.scenario import Criterion, ScenarioResult
from tools.sim.fleet import Fleet, FleetConfig
from tools.sim.profiles import IDLE


@dataclass(frozen=True, slots=True)
class BootStormConfig:
    count: int  # number of virtual chargers
    ramp_seconds: float  # spread connect timing across this window
    hold_seconds: float  # keep them connected this long after the ramp
    target_url: str  # `ws://host:port` for the gateway
    prometheus_url: str
    boot_p99_threshold_seconds: float = 3.0
    cp_id_prefix: str = "LOAD_BOOT"


async def run(config: BootStormConfig) -> ScenarioResult:
    started_at_iso = datetime.now(UTC).isoformat()
    started_monotonic = time.monotonic()

    fleet_config = FleetConfig(
        count=config.count,
        duration_seconds=config.ramp_seconds + config.hold_seconds,
        target_url=config.target_url,
        ramp_seconds=config.ramp_seconds,
        profile=IDLE,
        cp_id_prefix=config.cp_id_prefix,
        show_progress=False,
    )
    fleet = Fleet(fleet_config)
    counters = await fleet.run()
    duration = time.monotonic() - started_monotonic

    # Pull metrics for the run window. Prometheus queries are sync —
    # we just finished the async simulator part.
    prom = PrometheusClient(base_url=config.prometheus_url)
    criteria: list[Criterion] = []

    # Criterion 1 — every charger booted at least once during the run.
    # Compares against the simulator's own counter rather than asking
    # Prometheus, because the simulator knows precisely how many it
    # tried to boot. Reasonable proxy for "fleet ramped to size".
    boots_pass = counters.boots >= config.count
    criteria.append(
        Criterion(
            name="all chargers booted",
            expression=f"counters.boots ({counters.boots}) >= count ({config.count})",
            threshold=f">= {config.count}",
            actual=str(counters.boots),
            passed=boots_pass,
        )
    )

    # Criterion 2 — BootNotification handler P99 stays under the
    # threshold across the run window.
    p99_query = (
        "histogram_quantile(0.99, sum by (le) ("
        "rate(eveys_ocpp_handler_latency_seconds_bucket"
        '{action="BootNotification"}[5m])))'
    )
    try:
        result = prom.instant(p99_query)
        if result and result[0].get("value"):
            actual_p99 = float(result[0]["value"][1])
            actual_str = f"{actual_p99:.3f}s"
            p99_pass = actual_p99 < config.boot_p99_threshold_seconds
        else:
            actual_str = "no data"
            p99_pass = False
    except Exception as exc:
        actual_str = f"prometheus error: {exc}"
        p99_pass = False
    criteria.append(
        Criterion(
            name="BootNotification P99 latency",
            expression=p99_query,
            threshold=f"< {config.boot_p99_threshold_seconds}s",
            actual=actual_str,
            passed=p99_pass,
        )
    )

    # Criterion 3 — no fleet-side errors during the run. A high error
    # count usually means the gateway dropped connections (overloaded
    # accept queue, port exhaustion).
    errors_pass = counters.errors == 0
    criteria.append(
        Criterion(
            name="no fleet-side errors",
            expression=f"counters.errors ({counters.errors}) == 0",
            threshold="== 0",
            actual=str(counters.errors),
            passed=errors_pass,
        )
    )

    return ScenarioResult(
        name="boot_storm",
        started_at=started_at_iso,
        duration_seconds=duration,
        criteria=criteria,
        notes=[
            f"fleet: count={config.count} ramp={config.ramp_seconds}s hold={config.hold_seconds}s",
            f"counters: boots={counters.boots} txns={counters.transactions} "
            f"errors={counters.errors}",
        ],
    )


# Convenient wrapper for the CLI / smoke test.
async def run_quick(target_url: str, prometheus_url: str) -> ScenarioResult:
    """`--quick` shape — runs in under 2 minutes against `make compose-up`."""
    return await run(
        BootStormConfig(
            count=10,
            ramp_seconds=2.0,
            hold_seconds=8.0,
            target_url=target_url,
            prometheus_url=prometheus_url,
        )
    )


async def run_full(target_url: str, prometheus_url: str) -> ScenarioResult:
    """Full-scale shape — the spec's headline numbers. Requires a
    production-shaped stack (3+ gateway pods, beefy DB, etc.). Run
    on staging, not on a workstation."""
    return await run(
        BootStormConfig(
            count=10_000,
            ramp_seconds=300.0,  # spread 10k connects across 5 minutes
            hold_seconds=3600.0,  # hold for 1 hour
            target_url=target_url,
            prometheus_url=prometheus_url,
        )
    )


# `python -m tools.load.scenarios.boot_storm` for ad-hoc use.
if __name__ == "__main__":  # pragma: no cover
    import json
    import sys

    target = sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:19000"
    prom = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:9090"
    result = asyncio.run(run_quick(target, prom))
    json.dump(result.to_dict(), sys.stdout, indent=2)
