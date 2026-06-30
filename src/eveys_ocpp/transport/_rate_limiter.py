"""Per-charger inbound CALL rate cap (E5-3).

A misbehaving or compromised charger flooding the gateway with OCPP
CALLs (e.g. 1000 StatusNotifications/s) must not cascade — Postgres,
Kafka, and handler latency for the rest of the fleet stay
unaffected.

Token bucket per `cp_id`, in Redis (atomic Lua so concurrent pods
can't double-count). The bucket follows the charger across pods —
on reconnect, the new pod's first message inherits whatever budget
remained on the old pod's bucket (modulo the elapsed-time refill).

**Counted unit**: inbound CALLs only. CALLRESULT / CALLERROR are
correlated responses to commands *we* sent; throttling them would
break our own RemoteStart flows. The hook in
`connection.EveysChargePoint.route_message` filters by
`MessageType.Call` before consulting the limiter.

**Drop policy**: silently drop the offending message. Don't send
CALLERROR (some chargers retry immediately on certain error codes
→ feeds the storm). Don't close the WS (mass reconnect storm).
The charger's normal retry cadence is the right pacing signal;
the metric exists so operators can find offenders out-of-band.

**Failure mode**: when Redis is unreachable, **fall through accept**.
A Redis blip must not silently DoS the entire fleet. Same fail-open
pattern the idempotency cache uses (ADR-0017). Loud warning log
makes the failure visible.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


def _key(cp_id: str) -> str:
    """Redis hash key for a charger's bucket."""
    return f"cp:rate:{cp_id}"


# Atomic token-bucket Lua. Inputs:
#   KEYS[1]    bucket key
#   ARGV[1]    capacity (max tokens)
#   ARGV[2]    refill rate (tokens per second, float)
#   ARGV[3]    now (ms since epoch)
#   ARGV[4]    cost of this request (tokens to consume; usually 1)
#   ARGV[5]    idle TTL (seconds)
#
# Returns: { allowed (1/0), remaining (float) }
#
# Algorithm:
#   1. Read stored tokens + last_refill_ms (default capacity + now).
#   2. Refill: tokens = min(capacity, tokens + (now - last_refill_ms)/1000 * refill).
#   3. If tokens >= cost: tokens -= cost, allowed=1; else allowed=0.
#   4. Persist + reset TTL so an idle charger's bucket eventually
#      cleans itself up (no need for a sweeper).
#
# Tokens stored as a string-formatted float; precision-loss across
# the round-trip is fine — token counts are bookkeeping, not money.
_TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_sec = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local data = redis.call('HMGET', key, 'tokens', 'last_refill_ms')
local tokens = tonumber(data[1])
local last_refill_ms = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_refill_ms = now_ms
end

local elapsed_sec = (now_ms - last_refill_ms) / 1000.0
if elapsed_sec < 0 then elapsed_sec = 0 end
tokens = math.min(capacity, tokens + elapsed_sec * refill_per_sec)

local allowed = 0
if tokens >= cost then
    tokens = tokens - cost
    allowed = 1
end

redis.call('HSET', key, 'tokens', tokens, 'last_refill_ms', now_ms)
redis.call('EXPIRE', key, ttl)

return {allowed, tostring(tokens)}
"""


class RateLimiter:
    """Per-charger inbound-CALL rate limiter, Redis-backed.

    One instance per process; pass into `EveysChargePoint.route_message`
    via the cp constructor. `None` instance disables the limiter
    entirely (unit tests, Redis-less local dev) — `check()` returns
    True unconditionally on a None limiter.
    """

    def __init__(self, redis: Redis, *, settings: Settings) -> None:
        self._redis = redis
        self._capacity = float(settings.ws_rate_limit_capacity)
        self._refill = float(settings.ws_rate_limit_refill_per_second)
        # Idle TTL = enough that an active charger never sees a stale
        # bucket on reconnect, but a long-disconnected charger's key
        # cleans itself up. 1 hour is generous; the bucket is
        # reconstructed from defaults if it's gone.
        self._idle_ttl_seconds = 3600

    async def check(self, cp_id: str) -> bool:
        """Consume one token. Return True if allowed, False if throttled.

        On Redis failure, fail open (return True). The throttle exists
        to protect *us* from a runaway charger; it is not a security
        boundary. A broken Redis must not turn into a fleet-wide DoS.
        """
        try:
            now_ms = int(time.time() * 1000)
            # redis-py stub union (sync | async) — see registry.py for context.
            result = await self._redis.eval(  # type: ignore[misc]
                _TOKEN_BUCKET_LUA,
                1,
                _key(cp_id),
                self._capacity,
                self._refill,
                now_ms,
                1,  # cost
                self._idle_ttl_seconds,
            )
        except Exception as exc:
            # Fail open. Loud-warn so the alert path catches sustained
            # Redis trouble; rare blips don't matter for billing.
            log.warning("rate_limiter.redis_error", cp_id=cp_id, error=str(exc))
            return True

        # Lua returned [allowed, remaining]. redis-py decodes ints as
        # ints and the bytes/str of the second element depending on
        # client config. Coerce defensively.
        if not isinstance(result, list) or len(result) < 1:
            log.warning("rate_limiter.bad_lua_return", cp_id=cp_id, result=str(result))
            return True
        allowed = int(result[0]) == 1
        return allowed

    async def record_throttled(self, *, action: str) -> None:
        """Bump the throttled-counter. Separate method from `check` so the
        caller can decide whether the dropped message had a discoverable
        action label (CALLs do; malformed frames don't reach here)."""
        metrics_registry.RATE_LIMIT_THROTTLED_TOTAL.labels(action=action).inc()
