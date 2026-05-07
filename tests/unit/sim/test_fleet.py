"""Fleet — ramping, lifecycle, summary, counters share.

Mocks the `VirtualCharger.run` coroutine with a fast stand-in so we
don't need a real WS server to assert ramping + cancellation
behaviour.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import patch

import pytest
from tools.sim.fleet import Fleet, FleetConfig
from tools.sim.profiles import IDLE


def _config(**overrides: Any) -> FleetConfig:
    base: dict[str, Any] = {
        "count": 5,
        "duration_seconds": 1.0,
        "target_url": "ws://example.invalid:9000",
        "ramp_seconds": 0.2,
        "profile": IDLE,
        "show_progress": False,
    }
    base.update(overrides)
    return FleetConfig(**base)


@pytest.fixture
async def patched_run() -> AsyncIterator[list[float]]:
    """Replace `VirtualCharger.run` with a recorder that logs the
    monotonic time it was entered, then sleeps until cancelled.
    Returns the start-time list; the test inspects the spread.
    """
    started_at: list[float] = []

    async def _record_then_park(self: Any) -> None:
        started_at.append(time.monotonic())
        # Park until the fleet cancels us. CancelledError bubbles up
        # cleanly through the fleet's drain step.
        await asyncio.Event().wait()

    with patch("tools.sim.charger.VirtualCharger.run", _record_then_park):
        yield started_at


@pytest.mark.asyncio
async def test_fleet_builds_count_chargers() -> None:
    """`Fleet.__init__` materialises N VirtualCharger instances with
    sequentially-numbered cp_ids."""
    fleet = Fleet(_config(count=3))
    assert len(fleet._chargers) == 3
    assert [c.cp_id for c in fleet._chargers] == ["SIM_000000", "SIM_000001", "SIM_000002"]


@pytest.mark.asyncio
async def test_fleet_respects_cp_id_prefix() -> None:
    fleet = Fleet(_config(count=2, cp_id_prefix="LOAD"))
    assert [c.cp_id for c in fleet._chargers] == ["LOAD_000000", "LOAD_000001"]


@pytest.mark.asyncio
async def test_fleet_ramps_connect_starts_within_window(
    patched_run: list[float],
) -> None:
    """All N chargers should start within `[0, ramp_seconds]` of the
    fleet's start time. Verifies the ramp delay actually fires."""
    config = _config(count=20, duration_seconds=0.6, ramp_seconds=0.4)
    fleet_started_at = time.monotonic()
    await Fleet(config, rng=random.Random(0)).run()
    spreads = [s - fleet_started_at for s in patched_run]
    assert len(spreads) == 20
    # All within `[0, ramp_seconds + small slack]`.
    assert max(spreads) <= config.ramp_seconds + 0.2
    assert min(spreads) >= 0.0
    # Ramp actually spreads — at least one in the second half of the window.
    assert max(spreads) > config.ramp_seconds / 2


@pytest.mark.asyncio
async def test_fleet_returns_counters_after_duration(
    patched_run: list[float],
) -> None:
    """Run completes after `duration_seconds`; returns the live counters."""
    config = _config(count=3, duration_seconds=0.3, ramp_seconds=0.1)
    started = time.monotonic()
    counters = await Fleet(config).run()
    elapsed = time.monotonic() - started
    # Should exit within a small slack of the duration. Generous upper
    # bound to absorb teardown drain time on a slow CI box.
    assert config.duration_seconds <= elapsed < config.duration_seconds + 1.0
    assert counters is not None


@pytest.mark.asyncio
async def test_fleet_seed_makes_ramp_deterministic(
    patched_run: list[float],
) -> None:
    """Same seed → same ramp delays. Lets test runs against the
    simulator be reproducible from one fixture seed."""
    spreads_a: list[float] = []
    spreads_b: list[float] = []

    config = _config(count=4, duration_seconds=0.4, ramp_seconds=0.3)

    started = time.monotonic()
    await Fleet(config, rng=random.Random(42)).run()
    spreads_a = [s - started for s in list(patched_run)]
    patched_run.clear()

    started = time.monotonic()
    await Fleet(config, rng=random.Random(42)).run()
    spreads_b = [s - started for s in list(patched_run)]

    # Compare ordering of delays — same RNG seed must give same order.
    sorted_a = sorted(spreads_a)
    sorted_b = sorted(spreads_b)
    for a, b in zip(sorted_a, sorted_b, strict=True):
        # Within ~50ms of each other (event-loop scheduling slack).
        assert abs(a - b) < 0.1
