"""Idempotency cache for inbound OCPP replays (E2-11).

OCPP chargers retry inbound calls aggressively when our `CallResult`
is lost (network blip, pod restart, ack delay). Without dedup, a
retry triggers a duplicate DB write AND a duplicate Kafka event for
the same logical request — and downstream platform consumers
(billing, mobile BFF) MUST NOT see those duplicates.

AGENTS rule 3 names ``BootNotification`` and ``StopTransaction`` as
the two handlers that must be idempotent on replay. This module is
the gate they consult before doing any work.

Wire model
----------
Per (cp_id, message_id) we set a Redis key with a short TTL:

    cp:idem:{cp_id}:{message_id}   →   "1"   (TTL ~5 min)

On handler entry:
1. ``seen = await idem.check_and_record(cp_id, message_id)``
2. If ``seen`` is True: this is a replay — return the canonical
   response without any side effect (no DB write, no Kafka emit).
3. If ``seen`` is False: handler proceeds normally; the key was set
   atomically inside this same call.

The check-and-record uses Redis ``SET NX EX`` so the test-and-set is
atomic — two concurrent retries can't both pass the gate. The TTL is
short by design: an OCPP retry storm typically resolves within
seconds; 5 minutes covers that with margin without accumulating keys
forever. After TTL expiry, the same ``message_id`` would be treated
as fresh — which is fine because OCPP message_ids are UUIDs and
chargers don't reuse them across power cycles.

Failure model
-------------
If Redis is unreachable, ``check_and_record`` raises. Callers wrap
in try/except and treat any error as "fresh request" — better to
risk a rare double-write than to wedge the handler when the cache
is the problem. See ADR-0017.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


def _key(cp_id: str, message_id: str) -> str:
    return f"cp:idem:{cp_id}:{message_id}"


class IdempotencyCache:
    """Tiny Redis-backed dedup gate. One instance per process."""

    def __init__(self, redis: Redis, *, settings: Settings) -> None:
        self._redis = redis
        self._settings = settings

    async def check_and_record(self, *, cp_id: str, message_id: str) -> bool:
        """Atomic test-and-set.

        Returns True if this ``(cp_id, message_id)`` pair has been seen
        within the TTL window — caller should treat the inbound message
        as a replay. Returns False if it's the first sighting; the key
        is now recorded and future retries will see True.

        Raises if Redis is unreachable. Callers decide whether a cache
        outage should hard-fail the handler or fall through.
        """
        # ``SET key value NX EX ttl`` returns truthy on first write,
        # None when the key already exists. One round-trip; atomic.
        first_write = await self._redis.set(
            _key(cp_id, message_id),
            "1",
            ex=self._settings.idempotency_ttl_seconds,
            nx=True,
        )
        already_seen = not first_write
        if already_seen:
            log.info("idempotency.replay_detected", cp_id=cp_id, message_id=message_id)
        return already_seen


__all__ = ["IdempotencyCache"]
