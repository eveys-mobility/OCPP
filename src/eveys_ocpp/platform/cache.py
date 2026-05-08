"""Redis-backed cache for `IdTagInfo` results from the backend (E3-4).

The OCPP `Authorize` round-trip is on the hot path — the charger holds
its 30 s timeout open and the user is staring at the connector. Caching
the backend's `IdTagInfo` for ~30 s collapses repeated taps to one
backend call (P99 cache-hit latency is whatever Redis returns; sub-ms
in practice).

Wire model
----------
Per ``(cp_id, id_tag)`` we set a Redis key with the configured TTL.
Value is a JSON-serialised `IdTagInfo` (status / parent_id_tag /
expiry_date) so the *whole* charger reply round-trips, not just the
status. A cache hit forwards exactly what a recent backend call would
have produced.

    auth:{cp_id}:{id_tag}   →   {"status":"Accepted","parent_id_tag":"...","expiry_date":"..."}

What gets cached
----------------
- All five OCPP statuses returned successfully by the backend
  (Accepted / Blocked / Expired / Invalid / ConcurrentTx). Caching
  ``Blocked`` is just as valuable as caching ``Accepted`` — refuses
  a known-bad tag without a backend round-trip.

What does NOT get cached
------------------------
- ``BackendUnavailableError`` outcomes. The handler's fallback
  policy depends on current settings, not a stale call's result.
  Each unavailability path runs the policy fresh.
- ``BackendBusinessError`` (e.g. UNKNOWN_ID_TAG). The handler maps
  these to OCPP `Invalid` and that string IS cached as a successful
  status — but the cache is keyed on `(cp_id, id_tag)`, so a backend
  fix landing for an id_tag still has to wait for the TTL.

Failure model
-------------
Redis blip on read → log + return None (cache miss). The handler
proceeds to the backend round-trip; the user is unaffected.
Redis blip on write → log + drop the write. The cache stays empty
for that tag and the next tap re-roundtrips. Same defensive shape
as ``IdempotencyCache`` (ADR-0017).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from eveys_ocpp.observability import get_logger
from eveys_ocpp.platform.client import IdTagInfo

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


def _key(cp_id: str, id_tag: str) -> str:
    return f"auth:{cp_id}:{id_tag}"


class AuthorizeCache:
    """Tiny Redis-backed `IdTagInfo` cache. One instance per process."""

    def __init__(self, redis: Redis, *, settings: Settings) -> None:
        self._redis = redis
        self._settings = settings

    async def get(self, *, cp_id: str, id_tag: str) -> IdTagInfo | None:
        """Cache lookup.

        Returns the cached `IdTagInfo` on hit, None on miss / cache
        outage / malformed value. The caller continues to the backend
        on None (cache miss is indistinguishable from "no Redis").
        """
        # Read fresh per call so an admin override on
        # backend_authorize_cache_enabled takes effect without a pod
        # restart.
        from eveys_ocpp.runtime_overrides import get_override

        if not get_override(
            "backend_authorize_cache_enabled",
            self._settings.backend_authorize_cache_enabled,
        ):
            return None

        try:
            raw = await self._redis.get(_key(cp_id, id_tag))
        except Exception as exc:
            log.warning("authorize_cache.get_failed", cp_id=cp_id, id_tag=id_tag, error=str(exc))
            return None

        if raw is None:
            return None

        # Defensive: a value we can't parse is treated as a miss
        # (forces a backend round-trip; never serves wrong data).
        try:
            payload = json.loads(raw)
        except ValueError as exc:
            log.warning(
                "authorize_cache.malformed",
                cp_id=cp_id,
                id_tag=id_tag,
                error=str(exc),
            )
            return None

        if not isinstance(payload, dict) or "status" not in payload:
            return None

        return IdTagInfo(
            status=str(payload["status"]),
            parent_id_tag=payload.get("parent_id_tag"),
            expiry_date=payload.get("expiry_date"),
        )

    async def set(self, *, cp_id: str, id_tag: str, info: IdTagInfo) -> None:
        """Cache a freshly-resolved `IdTagInfo` for the configured TTL.

        TTL of 0 (disabled via the boolean knob) makes this a no-op;
        callers don't need to gate the call.
        """
        from eveys_ocpp.runtime_overrides import get_override

        if not get_override(
            "backend_authorize_cache_enabled",
            self._settings.backend_authorize_cache_enabled,
        ):
            return

        payload = json.dumps(
            {
                "status": info.status,
                "parent_id_tag": info.parent_id_tag,
                "expiry_date": info.expiry_date,
            }
        )

        try:
            await self._redis.set(
                _key(cp_id, id_tag),
                payload,
                ex=self._settings.backend_authorize_cache_ttl_seconds,
            )
        except Exception as exc:
            # Drop the write; next call re-roundtrips. Never wedge
            # the handler when the cache is the failure source.
            log.warning("authorize_cache.set_failed", cp_id=cp_id, id_tag=id_tag, error=str(exc))


__all__ = ["AuthorizeCache"]
