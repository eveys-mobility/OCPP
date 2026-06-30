"""Redis online-charger registry (E2-9).

Stores `cp:online:{cp_id}` keys with a TTL. Each connected charger has
exactly one such key; the value is the `pod_id` holding its WebSocket.

Lifecycle:
- WS connect       → `mark_online(cp_id, pod_id)`     (write key, TTL)
- Heartbeat handler → `refresh(cp_id)`                (extend TTL)
- WS disconnect    → `mark_offline(cp_id, pod_id)`    (delete key iff
                                                       still owned by us)

Cross-pod routing (E2-5, E2-10) reads `get_pod(cp_id)` to decide which
pod to forward a gRPC command to. A `None` result means the charger is
offline (or its key expired before the next heartbeat).

Why TTL-based rather than pub/sub on disconnect: a pod that crashes
without a clean shutdown still releases its chargers within `TTL`
seconds. No phantom keys after a pod kill.

The "still owned by us" check on `mark_offline` prevents a race where
a charger reconnects to a different pod immediately after disconnecting
from this one — we must not delete the new pod's key.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from redis.asyncio import Redis

from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


def _key(cp_id: str) -> str:
    """Redis key for a charger's online presence."""
    return f"cp:online:{cp_id}"


def _offline_marker_key(cp_id: str) -> str:
    """Redis key for the most recent disconnect timestamp.

    Used to compute per-CP offline duration on the next connect. Value
    is a HASH with `went_offline_at` (ISO-8601 UTC), `pod_id`, and
    `reason`. No TTL — survives an arbitrary outage; consumed by the
    next `pop_offline_marker` call on reconnect.
    """
    return f"cp:last_offline_at:{cp_id}"


@contextlib.contextmanager
def _timed_redis(op: str) -> Iterator[None]:
    """Context manager that observes the registry's Redis call latency.

    `op` is one of get / set / expire / eval / exists — bounded enum,
    no risk of cardinality blow-up. Defined locally so registry callers
    don't pull a generic helper through metrics/instrumentation.py for
    two lines of context-manager work.
    """
    started = time.perf_counter()
    try:
        yield
    finally:
        metrics_registry.REDIS_COMMAND_LATENCY_SECONDS.labels(op=op).observe(
            time.perf_counter() - started
        )


# Lua script for atomic compare-and-delete: only delete the key if its
# value still equals our pod_id. Prevents the reconnect-race described
# above. Redis runs Lua scripts atomically.
_DEL_IF_OWNER = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"""


class Registry:
    """Thin wrapper around a redis.asyncio client for the online registry.

    One instance per process; pass into the WS server and handlers.
    """

    def __init__(self, redis: Redis, *, settings: Settings) -> None:
        self._redis = redis
        self._settings = settings

    @classmethod
    def from_settings(cls, settings: Settings) -> Registry:
        """Build a registry from settings (one Redis pool per process)."""
        return cls(
            Redis.from_url(
                settings.redis_url,
                decode_responses=True,
                # Reconnect immediately on broker drop; don't sleep on EOF.
                health_check_interval=30,
            ),
            settings=settings,
        )

    async def close(self) -> None:
        await self._redis.aclose()

    async def mark_online(self, cp_id: str) -> None:
        """Record this pod as the owner of `cp_id`'s WS. Resets TTL."""
        with _timed_redis("set"):
            await self._redis.set(
                _key(cp_id),
                self._settings.pod_id,
                ex=self._settings.redis_online_ttl_seconds,
            )
        metrics_registry.REGISTRY_ONLINE_CHARGERS.inc()
        log.debug(
            "registry.mark_online",
            cp_id=cp_id,
            pod_id=self._settings.pod_id,
            ttl=self._settings.redis_online_ttl_seconds,
        )

    async def refresh(self, cp_id: str) -> bool:
        """Extend TTL on the charger's key. Returns False if key is gone.

        Used by the Heartbeat handler. A False return means the key
        expired (probably because heartbeats stopped briefly) — the
        caller should re-`mark_online` to recover.
        """
        with _timed_redis("expire"):
            result = await self._redis.expire(_key(cp_id), self._settings.redis_online_ttl_seconds)
        return bool(result)

    async def mark_offline(self, cp_id: str) -> bool:
        """Release the key iff this pod still owns it. Returns True on delete."""
        with _timed_redis("eval"):
            # redis-py's stubs annotate the union of sync/async return
            # types on its single client; the asyncio Redis class
            # actually returns a coroutine, but mypy sees the union.
            deleted_raw = await self._redis.eval(  # type: ignore[misc, unused-ignore]
                _DEL_IF_OWNER,
                1,
                _key(cp_id),
                self._settings.pod_id,
            )
        was_ours = bool(int(deleted_raw or 0))
        if was_ours:
            # Per-pod gauge — only decrement when *we* released the key.
            # The reconnect-to-different-pod race never decrements here;
            # the new pod's mark_online incremented its own gauge.
            metrics_registry.REGISTRY_ONLINE_CHARGERS.dec()
        log.debug(
            "registry.mark_offline",
            cp_id=cp_id,
            pod_id=self._settings.pod_id,
            was_ours=was_ours,
        )
        return was_ours

    async def record_offline_marker(self, cp_id: str, *, reason: str) -> None:
        """Record that `cp_id`'s WS just dropped.

        Writes the disconnect time + this pod's id + the close reason
        into a hash at `cp:last_offline_at:{cp_id}`. No TTL — outages of
        any length must remain measurable on the next reconnect.

        Called from the WS server's finally-block only when the
        compare-and-delete on the online key confirms we still owned
        it. Without that gate a reconnect-to-different-pod race would
        clobber the new pod's marker with this pod's stale one.
        """
        went_offline_at = datetime.now(UTC).isoformat()
        with _timed_redis("set"):
            await self._redis.hset(  # type: ignore[misc, unused-ignore]
                _offline_marker_key(cp_id),
                mapping={
                    "went_offline_at": went_offline_at,
                    "pod_id": self._settings.pod_id,
                    "reason": reason,
                },
            )
        log.debug(
            "registry.record_offline_marker",
            cp_id=cp_id,
            pod_id=self._settings.pod_id,
            reason=reason,
        )

    async def pop_offline_marker(self, cp_id: str) -> dict[str, str] | None:
        """Read-and-delete the offline marker.

        Returns the recorded `{went_offline_at, pod_id, reason}` dict
        on hit or None when no marker exists (first connect, or a pod
        crash skipped the prior disconnect's write). The read+delete
        pair is not atomic — duplicating it across two reconnects in
        quick succession is acceptable; downstream dedup keys on
        `event_id` handle it.
        """
        key = _offline_marker_key(cp_id)
        with _timed_redis("get"):
            data = await self._redis.hgetall(key)  # type: ignore[misc, unused-ignore]
        if not data:
            return None
        # decode_responses=True → str keys/values; normalize defensively.
        out = {str(k): str(v) for k, v in data.items()}
        with _timed_redis("set"):
            await self._redis.delete(key)
        return out

    async def get_pod(self, cp_id: str) -> str | None:
        """Return the pod_id currently holding cp_id's WS, or None if offline."""
        with _timed_redis("get"):
            value = await self._redis.get(_key(cp_id))
        return str(value) if value else None

    async def is_online(self, cp_id: str) -> bool:
        """Convenience wrapper — True if any pod holds the WS."""
        with _timed_redis("exists"):
            count = await self._redis.exists(_key(cp_id))
        return int(count) > 0

    async def count_online(self) -> int:
        """Fleet-wide online count via SCAN MATCH cp:online:*.

        Used by the `/sys/kpis` rollup. SCAN is non-blocking and runs
        in a single Redis pool round-trip per batch; for a fleet of N
        chargers it's O(N) but only the index page polls this (30s),
        so the load is bounded. Avoids holding any cross-pod gauge
        state since the per-pod `REGISTRY_ONLINE_CHARGERS` metric
        doesn't aggregate cleanly when the gateway scales horizontally.
        """
        total = 0
        with _timed_redis("scan"):
            async for _ in self._redis.scan_iter(match="cp:online:*", count=500):
                total += 1
        return total

    async def list_online_ids(self) -> list[str]:
        """Return every currently-online cp_id.

        Used by the `/charge-points?online=…` filter so the SQL count
        and page limits can both honour presence. Reads via the same
        SCAN iterator as count_online, then strips the `cp:online:`
        prefix off each key. Linear in fleet size but bounded by the
        list endpoint's 30s poll cadence on the Console side.
        """
        prefix = "cp:online:"
        ids: list[str] = []
        with _timed_redis("scan"):
            async for key in self._redis.scan_iter(match=f"{prefix}*", count=500):
                # decode_responses=True on the client → key is a str.
                s = key if isinstance(key, str) else str(key)
                if s.startswith(prefix):
                    ids.append(s[len(prefix) :])
        return ids
