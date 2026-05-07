"""Fleet — orchestrates N VirtualChargers and a live status renderer.

Connection ramping: spreading N connects across `ramp_seconds`
prevents a thundering-herd that would saturate the gateway's accept
queue and skew measurements. Each charger picks a uniform random
delay in `[0, ramp_seconds]` before its first connect.

The runtime is bounded by `duration_seconds`; the fleet cancels all
charger tasks at the deadline, awaits them, and prints a summary.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass

from tools.sim.charger import Counters, VirtualCharger
from tools.sim.profiles import BehaviourProfile


@dataclass(slots=True)
class FleetConfig:
    count: int
    duration_seconds: float
    target_url: str
    ramp_seconds: float
    profile: BehaviourProfile
    cp_id_prefix: str = "SIM"
    # If False, suppress the live status line — useful for tests + CI
    # so stdout doesn't fill with `[t=…] connected=…` updates.
    show_progress: bool = True


class Fleet:
    """One fleet per process. Build via `FleetConfig`, run via `run()`.

    Public attributes:
    - `counters` — live shared aggregate, readable from outside the
      run loop (e.g. tests asserting `counters.transactions > 0`).
    """

    def __init__(self, config: FleetConfig, *, rng: random.Random | None = None) -> None:
        self.config = config
        self.counters = Counters()
        self._rng = rng or random.Random()
        self._chargers: list[VirtualCharger] = list(self._build_chargers())

    @property
    def chargers(self) -> list[VirtualCharger]:
        """Read-only view of the charger list. Used by scenarios that
        need to operate on individual chargers (e.g. `reconnect_storm`
        force-drops a subset). Returns the underlying list — callers
        that mutate it earn whatever they get."""
        return self._chargers

    async def drop_random(self, fraction: float, *, rng: random.Random | None = None) -> int:
        """Force-close `fraction` (0..1) of the fleet's currently-live
        WS sessions. Returns the count actually dropped (chargers in
        mid-reconnect have no WS to close).

        Used by the reconnect-storm scenario to simulate "half the
        fleet drops at once". A real pod kill would drop the WSes
        the dead pod owned; this simulates the same shock at the
        WS layer without needing a multi-pod orchestration setup.
        """
        if not 0.0 <= fraction <= 1.0:
            raise ValueError(f"fraction must be in [0, 1], got {fraction}")
        rng = rng or self._rng
        target_count = round(len(self._chargers) * fraction)
        if target_count == 0:
            return 0
        sample = rng.sample(self._chargers, target_count)
        # Fire all closes concurrently so the storm hits the gateway
        # within a single asyncio scheduling round, not strung out.
        results = await asyncio.gather(*(c.force_drop() for c in sample), return_exceptions=True)
        return sum(1 for r in results if r is True)

    def _build_chargers(self) -> Iterable[VirtualCharger]:
        for i in range(self.config.count):
            # Per-charger RNG seeded from the fleet RNG so the whole
            # fleet is reproducible from one seed in tests.
            cp_rng = random.Random(self._rng.random())
            yield VirtualCharger(
                cp_id=f"{self.config.cp_id_prefix}_{i:06d}",
                target_url=self.config.target_url,
                profile=self.config.profile,
                counters=self.counters,
                rng=cp_rng,
            )

    async def run(self) -> Counters:
        """Run the fleet for `duration_seconds`, then return the
        final counters snapshot.

        Each charger gets a per-instance ramp delay so the connects
        spread across `[0, ramp_seconds]`. The charger's `run()` is
        a forever-loop; the fleet cancels them at the duration deadline.
        """
        deadline_at = time.monotonic() + self.config.duration_seconds
        tasks = [
            asyncio.create_task(
                self._run_one(cp, ramp_delay=self._rng.uniform(0.0, self.config.ramp_seconds)),
                name=f"sim-cp-{cp.cp_id}",
            )
            for cp in self._chargers
        ]
        progress_task: asyncio.Task[None] | None = None
        if self.config.show_progress:
            progress_task = asyncio.create_task(
                self._progress_loop(deadline_at), name="sim-progress"
            )

        try:
            await asyncio.sleep(self.config.duration_seconds)
        finally:
            for t in tasks:
                t.cancel()
            # Drain so we don't leak tasks. Errors during teardown are
            # expected (CancelledError, ConnectionClosed) — counted
            # already and otherwise silenced.
            for t in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await t
            if progress_task is not None:
                progress_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await progress_task
        self._print_summary()
        return self.counters

    async def _run_one(self, cp: VirtualCharger, *, ramp_delay: float) -> None:
        if ramp_delay > 0:
            await asyncio.sleep(ramp_delay)
        await cp.run()

    async def _progress_loop(self, deadline_at: float) -> None:
        """One status line per second on stderr (so stdout stays clean
        for any structured output a wrapper script might consume)."""
        started_at = time.monotonic()
        while True:
            await asyncio.sleep(1.0)
            elapsed = int(time.monotonic() - started_at)
            remaining = max(0, int(deadline_at - time.monotonic()))
            print(
                f"[t={elapsed:>4}s remaining={remaining:>4}s] "
                f"connected={self.counters.connected} "
                f"boots={self.counters.boots} "
                f"txns={self.counters.transactions} "
                f"errors={self.counters.errors}",
                file=sys.stderr,
                flush=True,
            )

    def _print_summary(self) -> None:
        print(
            "fleet summary: "
            f"chargers={self.config.count} "
            f"duration={self.config.duration_seconds:.0f}s "
            f"profile={self.config.profile.name} "
            f"boots={self.counters.boots} "
            f"transactions={self.counters.transactions} "
            f"errors={self.counters.errors}",
            file=sys.stderr,
            flush=True,
        )
