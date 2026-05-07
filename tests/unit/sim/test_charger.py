"""VirtualCharger lifecycle FSM — covered against a mocked WS.

We don't stand up a real websockets server here; the smoke test in
`tests/e2e/test_simulator_smoke.py` does that. These tests verify
the per-tick decision logic in isolation.
"""

from __future__ import annotations

import random
import time
from typing import Any
from unittest.mock import AsyncMock

import pytest
from tools.sim.charger import Counters, VirtualCharger
from tools.sim.profiles import REALISTIC


def _charger(**overrides: Any) -> VirtualCharger:
    base: dict[str, Any] = {
        "cp_id": "TEST_CP",
        "target_url": "ws://example.invalid:9000",
        "profile": REALISTIC,
        "counters": Counters(),
        "rng": random.Random(0),
    }
    base.update(overrides)
    return VirtualCharger(**base)


def test_charger_initial_state() -> None:
    """Fresh instance has no transaction, no scheduled events."""
    c = _charger()
    assert c._transaction_id is None
    assert c._session_ends_at is None
    assert c._meter_value_wh == 0


@pytest.mark.asyncio
async def test_start_transaction_marks_session_active() -> None:
    """`_start_transaction` flips state when Authorize + Start succeed."""
    c = _charger()
    cp = AsyncMock()

    auth_result = AsyncMock()
    auth_result.id_tag_info = {"status": "Accepted"}
    start_result = AsyncMock()
    start_result.transaction_id = 12345
    cp.call.side_effect = [auth_result, start_result]

    await c._start_transaction(cp, now=100.0)

    assert c._transaction_id == 12345
    assert c._session_ends_at is not None and c._session_ends_at > 100.0
    assert c.counters.transactions == 1


@pytest.mark.asyncio
async def test_start_transaction_skips_when_authorize_rejected() -> None:
    """Non-Accepted Authorize must not advance to StartTransaction."""
    c = _charger()
    cp = AsyncMock()
    auth_result = AsyncMock()
    auth_result.id_tag_info = {"status": "Invalid"}
    cp.call.return_value = auth_result

    await c._start_transaction(cp, now=time.monotonic())

    # Only Authorize was called — StartTransaction was skipped.
    assert cp.call.call_count == 1
    assert c._transaction_id is None
    assert c.counters.transactions == 0


@pytest.mark.asyncio
async def test_send_meter_value_increments_register() -> None:
    """Each MeterValues call adds 100Wh; transaction_id flows through."""
    c = _charger()
    c._transaction_id = 42
    cp = AsyncMock()
    await c._send_meter_value(cp)
    assert c._meter_value_wh == 100
    await c._send_meter_value(cp)
    assert c._meter_value_wh == 200
    # Verify the wire call carried our transaction_id.
    last = cp.call.call_args.args[0]
    assert getattr(last, "transaction_id", None) == 42


@pytest.mark.asyncio
async def test_stop_transaction_clears_session() -> None:
    c = _charger()
    c._transaction_id = 7
    c._session_ends_at = 200.0
    cp = AsyncMock()

    await c._stop_transaction(cp)

    assert c._transaction_id is None
    assert c._session_ends_at is None
    # StopTransaction was called.
    assert cp.call.call_count == 1


@pytest.mark.asyncio
async def test_run_recovers_from_one_session_failure() -> None:
    """When `_one_session` raises, the outer loop counts the error and
    sleeps then tries again. We patch `_one_session` at the class level
    (slots=True blocks per-instance attribute assignment) with a shim
    that raises twice then signals via an Event so we can cancel cleanly.

    Verifies the resilience contract — one charger crashing must not
    take the fleet down."""
    import asyncio
    import contextlib
    from unittest.mock import patch

    call_count = 0
    done = asyncio.Event()

    async def _flaky_session(_self: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise RuntimeError("simulated failure")
        done.set()
        await asyncio.sleep(10)  # park; the test cancels us

    c = _charger()
    with patch.object(VirtualCharger, "_one_session", _flaky_session):
        task = asyncio.create_task(c.run())
        await asyncio.wait_for(done.wait(), timeout=10.0)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert call_count >= 3
    assert c.counters.errors == 2
