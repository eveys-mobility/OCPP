"""Unit tests for the per-charger rate limiter (E5-3).

The Lua script's algorithmic correctness (refill math, atomic
decrement, multi-pod consistency) is e2e territory — pure-Python
unit tests can't run Lua. These tests cover the Python wrapper:
the right Lua call shape, fail-open on Redis errors, allowed/
denied propagation from the script's return value, and the
metric increment.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.settings import Settings
from eveys_ocpp.transport._rate_limiter import RateLimiter, _key


@pytest.fixture
def settings() -> Settings:
    return Settings(
        ws_rate_limit_enabled=True,
        ws_rate_limit_capacity=10,
        ws_rate_limit_refill_per_second=2.0,
    )


@pytest.fixture
def fake_redis() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def limiter(fake_redis: AsyncMock, settings: Settings) -> RateLimiter:
    return RateLimiter(fake_redis, settings=settings)


def test_key_format() -> None:
    assert _key("CP_001") == "cp:rate:CP_001"


@pytest.mark.asyncio
async def test_check_invokes_lua_with_capacity_and_refill(
    limiter: RateLimiter, fake_redis: AsyncMock
) -> None:
    fake_redis.eval.return_value = [1, "9.5"]

    allowed = await limiter.check("CP_001")

    assert allowed is True
    fake_redis.eval.assert_awaited_once()
    args = fake_redis.eval.await_args.args
    # Args: (script, numkeys=1, key, capacity, refill, now_ms, cost, ttl)
    assert args[1] == 1
    assert args[2] == "cp:rate:CP_001"
    assert args[3] == 10.0  # capacity
    assert args[4] == 2.0  # refill_per_second
    assert args[6] == 1  # cost = 1


@pytest.mark.asyncio
async def test_check_returns_false_when_lua_says_denied(
    limiter: RateLimiter, fake_redis: AsyncMock
) -> None:
    fake_redis.eval.return_value = [0, "0.0"]

    allowed = await limiter.check("CP_001")

    assert allowed is False


@pytest.mark.asyncio
async def test_check_fails_open_on_redis_exception(
    limiter: RateLimiter, fake_redis: AsyncMock
) -> None:
    """Redis blip must not turn into a fleet-wide DoS — the
    limiter exists to protect us *from* a runaway charger, not
    as a security boundary."""
    fake_redis.eval.side_effect = RuntimeError("redis is down")

    allowed = await limiter.check("CP_001")

    assert allowed is True


@pytest.mark.asyncio
async def test_check_fails_open_on_unexpected_lua_return_shape(
    limiter: RateLimiter, fake_redis: AsyncMock
) -> None:
    """If the Lua script gets edited and starts returning a
    surprise shape, fail open + warn loud."""
    fake_redis.eval.return_value = "not-a-list"  # type: ignore[assignment]

    allowed = await limiter.check("CP_001")

    assert allowed is True


@pytest.mark.asyncio
async def test_record_throttled_increments_counter(
    limiter: RateLimiter,
) -> None:
    counter = metrics_registry.RATE_LIMIT_THROTTLED_TOTAL.labels(action="StatusNotification")
    before = counter._value.get()

    await limiter.record_throttled(action="StatusNotification")

    assert counter._value.get() == before + 1
