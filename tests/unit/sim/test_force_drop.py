"""`VirtualCharger.force_drop` and `Fleet.drop_random` — the levers
the reconnect-storm scenario pulls.

We don't connect to a real WS here; force_drop's contract is "close
the current cp's WS if there is one, otherwise no-op". The unit test
exercises the no-op path (no current session) and the live path with
a fake `_current_cp` whose `_connection.close` records the call.
"""

from __future__ import annotations

import asyncio
import random
from typing import Any
from unittest.mock import AsyncMock

import pytest
from tools.sim.charger import Counters, VirtualCharger
from tools.sim.fleet import Fleet, FleetConfig
from tools.sim.profiles import IDLE


def _charger(**overrides: Any) -> VirtualCharger:
    base: dict[str, Any] = {
        "cp_id": "DROP_TEST",
        "target_url": "ws://example.invalid:9000",
        "profile": IDLE,
        "counters": Counters(),
        "rng": random.Random(0),
    }
    base.update(overrides)
    return VirtualCharger(**base)


@pytest.mark.asyncio
async def test_force_drop_returns_false_when_no_session() -> None:
    """No live cp → no WS to close, returns False without raising."""
    c = _charger()
    assert c._current_cp is None
    assert await c.force_drop() is False


@pytest.mark.asyncio
async def test_force_drop_closes_current_ws_and_returns_true() -> None:
    c = _charger()
    fake_cp = AsyncMock()
    fake_cp._connection.close = AsyncMock()
    c._current_cp = fake_cp

    result = await c.force_drop()

    assert result is True
    fake_cp._connection.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_force_drop_swallows_close_exceptions() -> None:
    """A double-close or already-dead WS shouldn't take the storm
    scenario out — log nothing, just return True."""
    c = _charger()
    fake_cp = AsyncMock()
    fake_cp._connection.close.side_effect = ConnectionResetError("already dead")
    c._current_cp = fake_cp

    # Must NOT raise.
    assert await c.force_drop() is True


# ---- Fleet.drop_random ----------------------------------------------------


def _fleet(**overrides: Any) -> Fleet:
    config = FleetConfig(
        count=overrides.pop("count", 10),
        duration_seconds=1.0,
        target_url="ws://example.invalid:9000",
        ramp_seconds=0.1,
        profile=IDLE,
        cp_id_prefix="TEST",
        show_progress=False,
    )
    return Fleet(config, rng=random.Random(0))


@pytest.mark.asyncio
async def test_drop_random_zero_chargers_returns_zero() -> None:
    """0% drop is a valid no-op."""
    f = _fleet(count=10)
    assert await f.drop_random(0.0) == 0


@pytest.mark.asyncio
async def test_drop_random_rejects_out_of_range_fraction() -> None:
    f = _fleet(count=10)
    with pytest.raises(ValueError):
        await f.drop_random(1.5)
    with pytest.raises(ValueError):
        await f.drop_random(-0.1)


@pytest.mark.asyncio
async def test_drop_random_returns_count_actually_dropped() -> None:
    """Chargers with no live session can't be dropped; only the ones
    with `_current_cp` set count toward the return value."""
    f = _fleet(count=4)
    # Set up a fake live cp on 2 of the 4 chargers.
    for charger in f.chargers[:2]:
        fake_cp = AsyncMock()
        fake_cp._connection.close = AsyncMock()
        charger._current_cp = fake_cp

    # Ask for 100% — only 2 of the 4 are live, so 2 should be dropped.
    dropped = await f.drop_random(1.0)
    assert dropped == 2


@pytest.mark.asyncio
async def test_drop_random_uses_provided_rng_for_sample() -> None:
    """Same seed → same chargers picked. Lets the storm scenario be
    reproducible from one fixture seed."""
    f = _fleet(count=10)
    # Make every charger droppable.
    for charger in f.chargers:
        fake_cp = AsyncMock()
        fake_cp._connection.close = AsyncMock()
        charger._current_cp = fake_cp

    # `drop_random` will pick a sample of size 5 with the given rng.
    rng_a = random.Random(42)
    rng_b = random.Random(42)
    # Snapshot which cps see a close from each call by recording
    # which fakes had their close awaited. Reset the mocks first.
    for c in f.chargers:
        c._current_cp._connection.close.reset_mock()  # type: ignore[attr-defined]

    dropped_a = await f.drop_random(0.5, rng=rng_a)

    closes_a = [
        i
        for i, c in enumerate(f.chargers)
        if c._current_cp._connection.close.await_count > 0  # type: ignore[union-attr]
    ]
    # Reset for the second pass.
    for c in f.chargers:
        c._current_cp._connection.close.reset_mock()  # type: ignore[attr-defined]
    dropped_b = await f.drop_random(0.5, rng=rng_b)
    closes_b = [
        i
        for i, c in enumerate(f.chargers)
        if c._current_cp._connection.close.await_count > 0  # type: ignore[union-attr]
    ]

    assert dropped_a == dropped_b == 5
    assert closes_a == closes_b


@pytest.mark.asyncio
async def test_drop_random_concurrent_closes_via_gather() -> None:
    """Closes fire concurrently so the storm hits inside one
    asyncio scheduling round, not strung out one-by-one."""
    f = _fleet(count=20)
    enter_count = 0
    enter_event = asyncio.Event()
    release_event = asyncio.Event()

    async def slow_close() -> None:
        nonlocal enter_count
        enter_count += 1
        if enter_count >= 5:
            enter_event.set()
        await release_event.wait()

    for charger in f.chargers:
        fake_cp = AsyncMock()
        fake_cp._connection.close = slow_close
        charger._current_cp = fake_cp

    # Kick off the drop and wait until at least 5 closes have entered
    # — proves they're running concurrently, not serialised.
    drop_task = asyncio.create_task(f.drop_random(0.5))
    await asyncio.wait_for(enter_event.wait(), timeout=2.0)
    assert enter_count >= 5
    release_event.set()
    dropped = await drop_task
    assert dropped == 10
