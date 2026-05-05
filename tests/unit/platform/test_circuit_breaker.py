"""Unit tests for the in-house circuit breaker."""

from __future__ import annotations

import time

import pytest

from eveys_ocpp.platform.circuit_breaker import CircuitBreaker
from eveys_ocpp.platform.errors import BackendCircuitOpenError


def _make_breaker(threshold: int = 3, cooldown: float = 0.05) -> CircuitBreaker:
    return CircuitBreaker(name="test", threshold=threshold, cooldown_seconds=cooldown)


@pytest.mark.asyncio
async def test_starts_closed_and_passes_calls() -> None:
    cb = _make_breaker()
    await cb.before_call()
    assert cb.state == "closed"


@pytest.mark.asyncio
async def test_opens_after_threshold_consecutive_failures() -> None:
    cb = _make_breaker(threshold=3)
    for _ in range(3):
        await cb.record_failure()
    assert cb.state == "open"
    with pytest.raises(BackendCircuitOpenError):
        await cb.before_call()


@pytest.mark.asyncio
async def test_success_resets_failure_counter() -> None:
    """Two failures + a success → counter back to 0; one more failure
    doesn't trip a 3-threshold breaker."""
    cb = _make_breaker(threshold=3)
    await cb.record_failure()
    await cb.record_failure()
    await cb.record_success()
    assert cb.consecutive_failures == 0
    await cb.record_failure()
    assert cb.state == "closed"


@pytest.mark.asyncio
async def test_open_breaker_transitions_to_half_open_after_cooldown() -> None:
    cb = _make_breaker(threshold=2, cooldown=0.05)
    await cb.record_failure()
    await cb.record_failure()
    assert cb.state == "open"

    # Within cooldown — still open.
    with pytest.raises(BackendCircuitOpenError):
        await cb.before_call()

    # After cooldown — before_call promotes to half-open.
    time.sleep(0.06)
    await cb.before_call()
    assert cb.state == "half_open"


@pytest.mark.asyncio
async def test_half_open_failure_reopens() -> None:
    """End-to-end: trip → cooldown → half-open → fail → open again."""
    cb = _make_breaker(threshold=2, cooldown=0.05)
    await cb.record_failure()
    await cb.record_failure()
    assert cb.state == "open"

    time.sleep(0.06)
    await cb.before_call()
    assert cb.state == "half_open"

    # One failure in half-open → re-open.
    await cb.record_failure()
    assert cb.state == "open"


@pytest.mark.asyncio
async def test_half_open_success_closes_breaker() -> None:
    """Trip → cooldown → half-open → success → closed (counter 0)."""
    cb = _make_breaker(threshold=2, cooldown=0.05)
    await cb.record_failure()
    await cb.record_failure()
    assert cb.state == "open"

    time.sleep(0.06)
    await cb.before_call()
    assert cb.state == "half_open"

    await cb.record_success()
    assert cb.state == "closed"
    assert cb.consecutive_failures == 0
