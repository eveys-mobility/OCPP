"""WS-upgrade IP rate limiter.

Sits inside the authorization gate, on the unauthorized branch. Every
WS upgrade whose `cp_id` is NOT already a row in `charge_points` runs
through this limiter; authorized chargers bypass it entirely, so a
legitimate fleet reconnecting from a single NAT'd egress never trips
the ban even when the reconnect rate is high.

Two Redis keys per IP:

- `ws:ip:count:{ip}` — a plain counter, 60 s TTL. INCR on every
  upgrade attempt. If the post-INCR value crosses
  `ip_rate_limit_requests_per_minute`, the IP has earned a ban.
- `ws:ip:block:{ip}` — the ban marker, `ip_rate_limit_block_seconds`
  TTL (1 h by default). Every subsequent upgrade checks this key
  first with a single EXISTS; hit → 429 immediately, no counter
  bookkeeping.

Fixed-window counter (not a token bucket): the requirement is "≤N per
minute", not a smooth refill. A window boundary can let 2N through
across the boundary — acceptable for a coarse abuse filter, and cheap
enough to reason about that operators don't need a Lua script to trust
it. If we ever need sliding-window semantics, swap this out; the
public surface (`check(ip)`) is the same shape.

Fail mode: if Redis is unreachable, **fall through accept**. A Redis
blip must not lock the whole fleet out of reconnecting. Same posture as
the per-CP rate limiter and the idempotency cache.

Not counted:
- REST admin API traffic (different perimeter — bearer-token gated).
- CALLs on an already-established WS (that's the per-CP limiter).
Only fresh WS upgrade attempts are counted here.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING

from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


_COUNT_PREFIX = "ws:ip:count:"
_BLOCK_PREFIX = "ws:ip:block:"


def _count_key(ip: str) -> str:
    return f"{_COUNT_PREFIX}{ip}"


def _block_key(ip: str) -> str:
    return f"{_BLOCK_PREFIX}{ip}"


@contextlib.contextmanager
def _timed_redis(op: str) -> Iterator[None]:
    started = time.perf_counter()
    try:
        yield
    finally:
        metrics_registry.REDIS_COMMAND_LATENCY_SECONDS.labels(op=op).observe(
            time.perf_counter() - started
        )


# Bounded outcome enum for the metric label set.
OUTCOME_ALLOWED = "allowed"
OUTCOME_BLOCKED = "blocked"
OUTCOME_NEWLY_BLOCKED = "newly_blocked"
OUTCOME_REDIS_ERROR = "redis_error"


class IpRateLimitDecision:
    """Value class returned by `IpRateLimiter.check`.

    Kept as a plain class (not dataclass) to avoid the frozen-dataclass
    boilerplate for two fields — the transport layer reads only
    `allowed` and passes `outcome` straight to the metric counter."""

    __slots__ = ("allowed", "outcome")

    def __init__(self, *, allowed: bool, outcome: str) -> None:
        self.allowed = allowed
        self.outcome = outcome


class IpRateLimiter:
    """WS-upgrade rate limiter, keyed by source IP.

    One instance per process; created at boot from the shared Redis
    client. `None` means "not enabled" (unit tests, local dev without
    Redis) — the transport layer treats a None limiter as fail-open."""

    def __init__(self, redis: Redis, *, settings: Settings) -> None:
        self._redis = redis
        self._max_per_minute = int(settings.ip_rate_limit_requests_per_minute)
        self._block_seconds = int(settings.ip_rate_limit_block_seconds)

    async def check(self, ip: str | None) -> IpRateLimitDecision:
        """Decide whether an upgrade attempt from `ip` may proceed.

        - Unknown source (peer_ip is None) → always allowed. Websockets
          can't attribute the connection, so there's nothing to
          rate-limit; the rest of the pipeline still runs.
        - Currently banned → blocked, no counter update.
        - Otherwise → INCR the 60 s counter. Over the cap? Set the ban
          key and return blocked. Under → allowed.
        """
        if not ip:
            return IpRateLimitDecision(allowed=True, outcome=OUTCOME_ALLOWED)

        try:
            with _timed_redis("exists"):
                is_blocked = await self._redis.exists(_block_key(ip))
            if is_blocked:
                return IpRateLimitDecision(allowed=False, outcome=OUTCOME_BLOCKED)

            key = _count_key(ip)
            with _timed_redis("incr"):
                count = await self._redis.incr(key)
            if count == 1:
                # Fresh window: only set TTL on the initial INCR so a
                # burst doesn't keep resetting the countdown.
                with _timed_redis("expire"):
                    await self._redis.expire(key, 60)

            if count > self._max_per_minute:
                with _timed_redis("set"):
                    await self._redis.set(_block_key(ip), "1", ex=self._block_seconds)
                log.warning(
                    "ws.ip_rate_limit_ban",
                    ip=ip,
                    count=int(count),
                    max_per_minute=self._max_per_minute,
                    block_seconds=self._block_seconds,
                )
                return IpRateLimitDecision(allowed=False, outcome=OUTCOME_NEWLY_BLOCKED)

            return IpRateLimitDecision(allowed=True, outcome=OUTCOME_ALLOWED)

        except Exception as exc:
            # Fail-open on any Redis error so a broker outage can't lock
            # the whole fleet out. Loud log so an operator sees the
            # limiter is degraded.
            log.warning(
                "ws.ip_rate_limit_redis_error",
                ip=ip,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            return IpRateLimitDecision(allowed=True, outcome=OUTCOME_REDIS_ERROR)
