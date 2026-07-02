"""Redis pending-authorization store.

A device the operator has never seen before doesn't get a row in
`charge_points` — that would put it in the fleet before anyone approved
it. Instead its metadata sits here, in Redis, under
`cp:pending:{cp_id}`. The value is a small JSON blob with what we
learned from the WS upgrade and the very first BootNotification:

    {
      "cp_id": "cp_ABC",
      "first_seen_at": "2026-07-02T16:00:00+00:00",
      "last_seen_at":  "2026-07-02T16:12:03+00:00",
      "peer_ip":       "203.0.113.7",
      "user_agent":    "OCPP-Charger/1.6",
      "vendor":        "Eveys",
      "model":         "Eveys-22kW-AC",
      "firmware":      "1.0.0",
      "serial_number": "cp_ABC",
      "attempts":      3
    }

The key has TTL `pending_authorization_ttl_seconds` (1 h by default).
Each fresh Boot / upgrade refreshes the TTL so an active-but-unapproved
charger doesn't drop off mid-decision — but the TTL is bounded, so a
forgotten pending row eventually clears itself without anyone remembering
to sweep it.

Lifecycle:
- WS upgrade for an unknown cp_id → `put(cp_id, …)` (creates or refreshes)
- BootNotification during pending → `update(cp_id, boot=…)` (Redis-only
  write; nothing hits Postgres)
- Operator authorises  → drain via `pop(cp_id)` and insert into
  `charge_points`
- Operator rejects     → `remove(cp_id)`; nothing else needed
- TTL expires          → key vanishes; next Boot restarts the flow

The `list_pending()` scan is used by the admin API to render the
operator's queue. SCAN is cheap enough at any realistic pending count
(operators size the fleet in thousands of chargers total, and only a
handful are pending at any moment); if that ever changes, back it with
a secondary sorted set keyed by `first_seen_at`.
"""

from __future__ import annotations

import contextlib
import json
import time
from collections.abc import Iterator
from datetime import datetime
from typing import TYPE_CHECKING, Any

from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


_KEY_PREFIX = "cp:pending:"


def _key(cp_id: str) -> str:
    return f"{_KEY_PREFIX}{cp_id}"


@contextlib.contextmanager
def _timed_redis(op: str) -> Iterator[None]:
    started = time.perf_counter()
    try:
        yield
    finally:
        metrics_registry.REDIS_COMMAND_LATENCY_SECONDS.labels(op=op).observe(
            time.perf_counter() - started
        )


class PendingAuthorizations:
    """Redis-backed store for devices awaiting operator authorisation.

    One instance per process. Passed into the transport layer + the
    admin API — both talk to the same key space so an operator
    approving a device sees the same row the WS handler wrote."""

    def __init__(self, redis: Redis, *, settings: Settings) -> None:
        self._redis = redis
        self._ttl_seconds = settings.pending_authorization_ttl_seconds

    async def upsert(
        self,
        *,
        cp_id: str,
        peer_ip: str | None,
        user_agent: str | None,
        now: datetime,
    ) -> dict[str, Any]:
        """Create or refresh a pending row for `cp_id`.

        On create, the row gets a fresh TTL. On refresh (existing row
        present), the ORIGINAL TTL is preserved via Redis's
        `KEEPTTL` — a chatty charger reconnecting every minute must
        not slide the "1 h from first sight" ceiling. `first_seen_at`
        is likewise preserved; only `last_seen_at`, `peer_ip`,
        `user_agent`, and `attempts` are updated."""
        key = _key(cp_id)
        existing = await self._read(key)
        if existing is None:
            row: dict[str, Any] = {
                "cp_id": cp_id,
                "first_seen_at": now.isoformat(),
                "last_seen_at": now.isoformat(),
                "peer_ip": peer_ip,
                "user_agent": user_agent,
                "vendor": None,
                "model": None,
                "firmware": None,
                "serial_number": None,
                "attempts": 1,
            }
            await self._write(key, row, preserve_ttl=False)
            return row

        row = dict(existing)
        row["last_seen_at"] = now.isoformat()
        if peer_ip is not None:
            row["peer_ip"] = peer_ip
        if user_agent is not None:
            row["user_agent"] = user_agent
        row["attempts"] = int(row.get("attempts", 0)) + 1
        await self._write(key, row, preserve_ttl=True)
        return row

    async def record_boot(
        self,
        *,
        cp_id: str,
        vendor: str | None,
        model: str | None,
        firmware: str | None,
        serial_number: str | None,
        now: datetime,
    ) -> dict[str, Any] | None:
        """Merge BootNotification metadata into the pending row.

        Returns `None` if the pending row has already expired between
        the upgrade and the Boot arrival — the caller should treat that
        the same as an authorization miss (usually: close the WS).

        Preserves the original TTL: Boot metadata arriving late in the
        pending window doesn't slide the ceiling."""
        key = _key(cp_id)
        existing = await self._read(key)
        if existing is None:
            return None
        row = dict(existing)
        row["last_seen_at"] = now.isoformat()
        if vendor is not None:
            row["vendor"] = vendor
        if model is not None:
            row["model"] = model
        if firmware is not None:
            row["firmware"] = firmware
        if serial_number is not None:
            row["serial_number"] = serial_number
        await self._write(key, row, preserve_ttl=True)
        return row

    async def get(self, cp_id: str) -> dict[str, Any] | None:
        """Read one pending row. `None` means either never-pending or
        the TTL has expired since the last write."""
        return await self._read(_key(cp_id))

    async def is_pending(self, cp_id: str) -> bool:
        """Fast path for the hot handler layer — one `EXISTS` call,
        no JSON decode. Used by non-Boot handlers to CALLERROR before
        they touch the DB."""
        with _timed_redis("exists"):
            return bool(await self._redis.exists(_key(cp_id)))

    async def remove(self, cp_id: str) -> bool:
        """Drop the pending row. Returns True if a row existed. Used
        on operator-reject and on operator-authorize (after the row's
        metadata is copied into `charge_points`)."""
        with _timed_redis("del"):
            deleted = await self._redis.delete(_key(cp_id))
        return bool(deleted)

    async def pop(self, cp_id: str) -> dict[str, Any] | None:
        """Read and delete atomically-enough (two commands; the admin
        route holds the operator's request-scoped session, so there is
        no concurrent writer). Returns the metadata so the caller can
        seed the `charge_points` row without a second round-trip."""
        row = await self.get(cp_id)
        if row is None:
            return None
        await self.remove(cp_id)
        return row

    async def list_pending(self) -> list[dict[str, Any]]:
        """Return every pending row currently in Redis.

        SCAN over the prefix, then batched MGET on the matches. Cheap
        at any realistic operator-facing size (pending queue is meant
        to hold single- or double-digit devices at a time)."""
        keys: list[str] = []
        with _timed_redis("scan"):
            async for k in self._redis.scan_iter(match=f"{_KEY_PREFIX}*", count=200):
                # redis-py returns bytes in some client modes; normalise.
                keys.append(k.decode("utf-8") if isinstance(k, bytes) else k)
        if not keys:
            return []
        with _timed_redis("mget"):
            raw_values = await self._redis.mget(keys)
        rows: list[dict[str, Any]] = []
        for k, raw in zip(keys, raw_values, strict=True):
            if raw is None:
                # Race: the key expired between SCAN and MGET. Skip.
                continue
            try:
                rows.append(json.loads(raw))
            except json.JSONDecodeError:
                log.warning("pending_authorizations.decode_failed", key=k)
        # Deterministic order for the operator UI: oldest first, so a
        # long-waiting device doesn't get lost behind fresh ones.
        rows.sort(key=lambda r: r.get("first_seen_at", ""))
        return rows

    async def _read(self, key: str) -> dict[str, Any] | None:
        with _timed_redis("get"):
            raw = await self._redis.get(key)
        if raw is None:
            return None
        try:
            decoded: dict[str, Any] = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("pending_authorizations.decode_failed", key=key)
            return None
        return decoded

    async def _write(self, key: str, row: dict[str, Any], *, preserve_ttl: bool) -> None:
        # `preserve_ttl=False` sets a fresh TTL — used only for the
        # first-sight write. Every subsequent write passes True,
        # sending `KEEPTTL` so the original ceiling (anchored at
        # first_seen_at) is preserved. Without that a chatty charger
        # could refresh its way into an unbounded pending row.
        payload = json.dumps(row, separators=(",", ":"))
        with _timed_redis("set"):
            if preserve_ttl:
                await self._redis.set(key, payload, keepttl=True)
            else:
                await self._redis.set(key, payload, ex=self._ttl_seconds)
