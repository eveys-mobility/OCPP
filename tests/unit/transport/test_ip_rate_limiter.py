"""Unit tests for the WS-upgrade IP rate limiter.

The limiter is a small Python wrapper around three Redis primitives
(EXISTS on the ban key, INCR on the counter, SET on the ban key).
Coverage focus:

- Currently-banned IP → blocked, no counter update.
- Under the cap → allowed, counter incremented.
- Fresh window's first INCR sets a 60 s TTL; subsequent INCRs don't
  refresh (otherwise a persistent trickle never triggers).
- Over the cap → newly_blocked, ban key set with the configured TTL.
- Missing peer_ip → allowed (unknowable source, nothing to key).
- Redis error → fail-open with a `redis_error` outcome so a broker
  blip can't lock the fleet out.

Redis fully mocked. Ban key existence is the `exists` return; the
counter's post-INCR value is the `incr` return.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from eveys_ocpp.settings import Settings
from eveys_ocpp.transport._ip_rate_limiter import (
    OUTCOME_ALLOWED,
    OUTCOME_BLOCKED,
    OUTCOME_NEWLY_BLOCKED,
    OUTCOME_REDIS_ERROR,
    IpRateLimiter,
    _block_key,
    _count_key,
)


@pytest.fixture
def settings() -> Settings:
    return Settings(
        ip_rate_limit_requests_per_minute=100,
        ip_rate_limit_block_seconds=3600,
    )


@pytest.fixture
def fake_redis() -> AsyncMock:
    r = AsyncMock()
    # Not banned by default; INCR returns 1 (fresh window).
    r.exists.return_value = 0
    r.incr.return_value = 1
    return r


@pytest.fixture
def limiter(fake_redis: AsyncMock, settings: Settings) -> IpRateLimiter:
    return IpRateLimiter(fake_redis, settings=settings)


def test_key_formats() -> None:
    assert _count_key("1.2.3.4") == "ws:ip:count:1.2.3.4"
    assert _block_key("1.2.3.4") == "ws:ip:block:1.2.3.4"


# ---- happy path -----------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_ip_is_allowed(limiter: IpRateLimiter, fake_redis: AsyncMock) -> None:
    """Websockets can't always attribute a peer address. Nothing to
    key against → the rest of the pipeline still runs."""
    decision = await limiter.check(None)
    assert decision.allowed is True
    assert decision.outcome == OUTCOME_ALLOWED
    fake_redis.exists.assert_not_awaited()
    fake_redis.incr.assert_not_awaited()


@pytest.mark.asyncio
async def test_first_hit_sets_60s_ttl(limiter: IpRateLimiter, fake_redis: AsyncMock) -> None:
    fake_redis.exists.return_value = 0
    fake_redis.incr.return_value = 1

    decision = await limiter.check("1.2.3.4")

    assert decision.allowed is True
    assert decision.outcome == OUTCOME_ALLOWED
    fake_redis.incr.assert_awaited_once_with("ws:ip:count:1.2.3.4")
    # Fresh window (post-INCR == 1) sets the 60 s ceiling.
    fake_redis.expire.assert_awaited_once_with("ws:ip:count:1.2.3.4", 60)


@pytest.mark.asyncio
async def test_subsequent_hit_does_not_refresh_ttl(
    limiter: IpRateLimiter, fake_redis: AsyncMock
) -> None:
    """Only the initial INCR sets EXPIRE — otherwise a persistent
    trickle would keep sliding the window and never fire."""
    fake_redis.exists.return_value = 0
    fake_redis.incr.return_value = 50  # mid-window

    await limiter.check("1.2.3.4")

    fake_redis.expire.assert_not_awaited()


# ---- ban paths -----------------------------------------------------------


@pytest.mark.asyncio
async def test_banned_ip_short_circuits_before_counter(
    limiter: IpRateLimiter, fake_redis: AsyncMock
) -> None:
    """A banned IP shouldn't consume counter budget — EXISTS-only path
    keeps Redis load bounded for a hostile source."""
    fake_redis.exists.return_value = 1

    decision = await limiter.check("1.2.3.4")

    assert decision.allowed is False
    assert decision.outcome == OUTCOME_BLOCKED
    fake_redis.incr.assert_not_awaited()
    fake_redis.set.assert_not_awaited()


@pytest.mark.asyncio
async def test_over_cap_sets_ban_key_and_returns_newly_blocked(
    limiter: IpRateLimiter, fake_redis: AsyncMock
) -> None:
    fake_redis.exists.return_value = 0
    fake_redis.incr.return_value = 101  # one over the cap

    decision = await limiter.check("1.2.3.4")

    assert decision.allowed is False
    assert decision.outcome == OUTCOME_NEWLY_BLOCKED
    fake_redis.set.assert_awaited_once()
    call = fake_redis.set.await_args
    assert call.args[0] == "ws:ip:block:1.2.3.4"
    assert call.args[1] == "1"
    assert call.kwargs.get("ex") == 3600


@pytest.mark.asyncio
async def test_at_the_cap_is_still_allowed(limiter: IpRateLimiter, fake_redis: AsyncMock) -> None:
    """100/min: the 100th hit is fine, the 101st trips the ban."""
    fake_redis.exists.return_value = 0
    fake_redis.incr.return_value = 100

    decision = await limiter.check("1.2.3.4")

    assert decision.allowed is True
    assert decision.outcome == OUTCOME_ALLOWED
    fake_redis.set.assert_not_awaited()


# ---- fail-open on Redis error --------------------------------------------


@pytest.mark.asyncio
async def test_redis_error_fails_open(limiter: IpRateLimiter, fake_redis: AsyncMock) -> None:
    """A broker blip must not lock the fleet out of reconnecting.
    Same posture as the per-CP limiter and the idempotency cache."""
    fake_redis.exists.side_effect = RuntimeError("connection reset")

    decision = await limiter.check("1.2.3.4")

    assert decision.allowed is True
    assert decision.outcome == OUTCOME_REDIS_ERROR
