"""Unit tests for the Redis pending-authorization store.

The store is intentionally small: SET/GET/DEL/SCAN with a JSON blob
and a TTL. The Redis client is fully mocked — these tests cover the
Python-side shape:
- fresh row creates use `ex=<ttl>`; refreshes use `keepttl=True` so
  the "1 h from first sight" ceiling isn't sliding.
- `first_seen_at` is preserved across refreshes; `last_seen_at` and
  `attempts` update.
- `record_boot` returns None when the pending row has already expired
  (the caller needs that to close the WS).
- `list_pending` sorts oldest-first so a long-waiting device doesn't
  get lost behind fresh ones in the operator UI.
- `remove` returns True/False on hit/miss (used by the reject route
  to decide 200 vs 404).

Redis Lua scripting or SCAN iteration semantics aren't exercised —
that's e2e territory. The `AsyncMock` on `scan_iter` returns an async
iterator so the SUT's `async for` walks a fixed key set.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest

from eveys_ocpp.pending_authorizations import PendingAuthorizations, _key
from eveys_ocpp.settings import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(pending_authorization_ttl_seconds=3600)


@pytest.fixture
def fake_redis() -> AsyncMock:
    r = AsyncMock()
    # Sensible defaults so a test can override only the calls it cares
    # about. `delete` mimics DEL returning 1 (hit) unless overridden.
    r.get.return_value = None
    r.delete.return_value = 1
    r.exists.return_value = 0
    return r


@pytest.fixture
def store(fake_redis: AsyncMock, settings: Settings) -> PendingAuthorizations:
    return PendingAuthorizations(fake_redis, settings=settings)


def test_key_format() -> None:
    assert _key("CP_001") == "cp:pending:CP_001"


# ---- upsert ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_creates_fresh_row_with_ttl_when_absent(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    fake_redis.get.return_value = None
    now = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)

    row = await store.upsert(cp_id="CP_NEW", peer_ip="1.2.3.4", user_agent="ua/1", now=now)

    assert row["cp_id"] == "CP_NEW"
    assert row["first_seen_at"] == now.isoformat()
    assert row["last_seen_at"] == now.isoformat()
    assert row["attempts"] == 1
    assert row["peer_ip"] == "1.2.3.4"
    assert row["vendor"] is None  # populated later by record_boot

    # Fresh row -> ex=ttl, not keepttl.
    fake_redis.set.assert_awaited_once()
    kwargs = fake_redis.set.await_args.kwargs
    assert kwargs.get("ex") == 3600
    assert "keepttl" not in kwargs


@pytest.mark.asyncio
async def test_upsert_refresh_preserves_ttl_and_first_seen(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    """Second upgrade attempt: `first_seen_at` and TTL both frozen."""
    existing = {
        "cp_id": "CP_ABC",
        "first_seen_at": "2026-07-02T11:00:00+00:00",
        "last_seen_at": "2026-07-02T11:00:00+00:00",
        "peer_ip": "1.1.1.1",
        "user_agent": "ua/1",
        "vendor": None,
        "model": None,
        "firmware": None,
        "serial_number": None,
        "attempts": 1,
    }
    fake_redis.get.return_value = json.dumps(existing)
    now = datetime(2026, 7, 2, 12, 30, tzinfo=UTC)

    row = await store.upsert(cp_id="CP_ABC", peer_ip="2.2.2.2", user_agent="ua/2", now=now)

    # Ceiling preserved.
    assert row["first_seen_at"] == "2026-07-02T11:00:00+00:00"
    # Everything else advanced.
    assert row["last_seen_at"] == now.isoformat()
    assert row["attempts"] == 2
    assert row["peer_ip"] == "2.2.2.2"
    assert row["user_agent"] == "ua/2"

    fake_redis.set.assert_awaited_once()
    kwargs = fake_redis.set.await_args.kwargs
    # Refresh uses KEEPTTL, not a fresh ex — otherwise a chatty charger
    # slides the 1 h ceiling into an unbounded row.
    assert kwargs.get("keepttl") is True
    assert "ex" not in kwargs


@pytest.mark.asyncio
async def test_upsert_ignores_none_peer_fields_on_refresh(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    """A reconnect that lost its peer_ip/user_agent shouldn't erase
    the last known-good values."""
    existing = {
        "cp_id": "CP_ABC",
        "first_seen_at": "2026-07-02T11:00:00+00:00",
        "last_seen_at": "2026-07-02T11:00:00+00:00",
        "peer_ip": "1.1.1.1",
        "user_agent": "ua/1",
        "vendor": None,
        "model": None,
        "firmware": None,
        "serial_number": None,
        "attempts": 1,
    }
    fake_redis.get.return_value = json.dumps(existing)
    now = datetime(2026, 7, 2, 12, 30, tzinfo=UTC)

    row = await store.upsert(cp_id="CP_ABC", peer_ip=None, user_agent=None, now=now)

    assert row["peer_ip"] == "1.1.1.1"
    assert row["user_agent"] == "ua/1"


# ---- record_boot ----------------------------------------------------------


@pytest.mark.asyncio
async def test_record_boot_merges_metadata(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    existing = {
        "cp_id": "CP_ABC",
        "first_seen_at": "2026-07-02T11:00:00+00:00",
        "last_seen_at": "2026-07-02T11:00:00+00:00",
        "peer_ip": "1.1.1.1",
        "user_agent": "ua/1",
        "vendor": None,
        "model": None,
        "firmware": None,
        "serial_number": None,
        "attempts": 1,
    }
    fake_redis.get.return_value = json.dumps(existing)
    now = datetime(2026, 7, 2, 12, 30, tzinfo=UTC)

    row = await store.record_boot(
        cp_id="CP_ABC",
        vendor="Eveys",
        model="Eveys-22kW-AC",
        firmware="1.0.0",
        serial_number="cp_ABC",
        now=now,
    )

    assert row is not None
    assert row["vendor"] == "Eveys"
    assert row["model"] == "Eveys-22kW-AC"
    assert row["firmware"] == "1.0.0"
    assert row["serial_number"] == "cp_ABC"
    assert row["first_seen_at"] == "2026-07-02T11:00:00+00:00"  # preserved
    assert row["last_seen_at"] == now.isoformat()

    # Boot metadata is a refresh, not a fresh row — same KEEPTTL rule.
    kwargs = fake_redis.set.await_args.kwargs
    assert kwargs.get("keepttl") is True


@pytest.mark.asyncio
async def test_record_boot_returns_none_when_row_expired(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    """Race: WS accepted, then Redis TTL expires before the Boot
    arrives. The caller uses `None` to know to close the connection."""
    fake_redis.get.return_value = None

    row = await store.record_boot(
        cp_id="CP_GONE",
        vendor="Eveys",
        model="M",
        firmware="1.0.0",
        serial_number="s",
        now=datetime.now(UTC),
    )

    assert row is None
    fake_redis.set.assert_not_awaited()


# ---- get / is_pending / remove / pop --------------------------------------


@pytest.mark.asyncio
async def test_get_returns_none_when_key_missing(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    fake_redis.get.return_value = None
    assert await store.get("CP_UNKNOWN") is None


@pytest.mark.asyncio
async def test_get_decodes_json_payload(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    payload = {"cp_id": "CP_X", "first_seen_at": "2026-07-02T11:00:00+00:00", "attempts": 1}
    fake_redis.get.return_value = json.dumps(payload)
    row = await store.get("CP_X")
    assert row == payload


@pytest.mark.asyncio
async def test_get_returns_none_on_bad_json(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    """A corrupt Redis value shouldn't crash the admin API; log and
    return None so the caller treats it as absent."""
    fake_redis.get.return_value = "{not: json"
    assert await store.get("CP_X") is None


@pytest.mark.asyncio
async def test_is_pending_uses_exists(store: PendingAuthorizations, fake_redis: AsyncMock) -> None:
    fake_redis.exists.return_value = 1
    assert await store.is_pending("CP_ABC") is True
    fake_redis.exists.assert_awaited_with("cp:pending:CP_ABC")


@pytest.mark.asyncio
async def test_is_pending_returns_false_when_key_absent(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    fake_redis.exists.return_value = 0
    assert await store.is_pending("CP_ABC") is False


@pytest.mark.asyncio
async def test_remove_returns_true_when_key_existed(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    fake_redis.delete.return_value = 1
    assert await store.remove("CP_ABC") is True
    fake_redis.delete.assert_awaited_with("cp:pending:CP_ABC")


@pytest.mark.asyncio
async def test_remove_returns_false_when_key_absent(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    fake_redis.delete.return_value = 0
    assert await store.remove("CP_UNKNOWN") is False


@pytest.mark.asyncio
async def test_pop_reads_then_deletes(store: PendingAuthorizations, fake_redis: AsyncMock) -> None:
    payload = {"cp_id": "CP_ABC", "vendor": "Eveys", "attempts": 3}
    fake_redis.get.return_value = json.dumps(payload)
    fake_redis.delete.return_value = 1

    row = await store.pop("CP_ABC")

    assert row == payload
    fake_redis.get.assert_awaited_with("cp:pending:CP_ABC")
    fake_redis.delete.assert_awaited_with("cp:pending:CP_ABC")


@pytest.mark.asyncio
async def test_pop_returns_none_when_absent(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    fake_redis.get.return_value = None
    row = await store.pop("CP_UNKNOWN")
    assert row is None
    fake_redis.delete.assert_not_awaited()


# ---- list_pending ---------------------------------------------------------


class _AsyncIter:
    """Minimal async iterator so `async for k in redis.scan_iter(...)`
    walks a fixed key set. `str | bytes` matches redis-py's actual
    return type (bytes in some client modes)."""

    def __init__(self, keys: list[str | bytes]) -> None:
        self._keys = list(keys)

    def __aiter__(self) -> _AsyncIter:
        return self

    async def __anext__(self) -> str | bytes:
        if not self._keys:
            raise StopAsyncIteration
        return self._keys.pop(0)


@pytest.mark.asyncio
async def test_list_pending_returns_empty_when_no_keys(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    fake_redis.scan_iter = lambda **_kw: _AsyncIter([])
    rows = await store.list_pending()
    assert rows == []


@pytest.mark.asyncio
async def test_list_pending_sorts_oldest_first(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    """Two keys, one newer than the other; list_pending returns the
    older one first so the operator UI shows long-waiting devices at
    the top."""
    now = datetime.now(UTC)
    keys: list[str | bytes] = ["cp:pending:CP_NEW", "cp:pending:CP_OLD"]
    fake_redis.scan_iter = lambda **_kw: _AsyncIter(keys)
    fake_redis.mget = AsyncMock(
        return_value=[
            json.dumps({"cp_id": "CP_NEW", "first_seen_at": now.isoformat()}),
            json.dumps(
                {
                    "cp_id": "CP_OLD",
                    "first_seen_at": (now - timedelta(minutes=30)).isoformat(),
                }
            ),
        ]
    )

    rows = await store.list_pending()

    assert [r["cp_id"] for r in rows] == ["CP_OLD", "CP_NEW"]


@pytest.mark.asyncio
async def test_list_pending_skips_expired_race(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    """A key can expire between SCAN and MGET; the None slot is
    dropped instead of raising."""
    fake_redis.scan_iter = lambda **_kw: _AsyncIter(["cp:pending:CP_A", "cp:pending:CP_B"])
    fake_redis.mget = AsyncMock(
        return_value=[
            None,
            json.dumps({"cp_id": "CP_B", "first_seen_at": datetime.now(UTC).isoformat()}),
        ]
    )

    rows = await store.list_pending()

    assert [r["cp_id"] for r in rows] == ["CP_B"]


@pytest.mark.asyncio
async def test_list_pending_skips_corrupt_json(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    """A corrupt row shouldn't blow up the operator's list — log and
    skip so the good rows still render."""
    fake_redis.scan_iter = lambda **_kw: _AsyncIter(["cp:pending:CP_BAD", "cp:pending:CP_OK"])
    fake_redis.mget = AsyncMock(
        return_value=[
            "not-json",
            json.dumps({"cp_id": "CP_OK", "first_seen_at": datetime.now(UTC).isoformat()}),
        ]
    )

    rows = await store.list_pending()

    assert [r["cp_id"] for r in rows] == ["CP_OK"]


@pytest.mark.asyncio
async def test_list_pending_drops_and_deletes_expired_rows(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    """Read-time TTL enforcement: a row older than the currently-
    configured `pending_authorization_ttl_seconds` is filtered out AND
    DELETEd, so an operator who lowered the TTL in .env doesn't see
    ghost rows still riding their original longer Redis TTL."""
    now = datetime.now(UTC)
    # ttl is 3600 in the fixture; put one row two hours in the past.
    fake_redis.scan_iter = lambda **_kw: _AsyncIter(["cp:pending:CP_STALE", "cp:pending:CP_FRESH"])
    fake_redis.mget = AsyncMock(
        return_value=[
            json.dumps(
                {
                    "cp_id": "CP_STALE",
                    "first_seen_at": (now - timedelta(hours=2)).isoformat(),
                }
            ),
            json.dumps({"cp_id": "CP_FRESH", "first_seen_at": now.isoformat()}),
        ]
    )

    rows = await store.list_pending()

    assert [r["cp_id"] for r in rows] == ["CP_FRESH"]
    fake_redis.delete.assert_awaited_once_with("cp:pending:CP_STALE")


@pytest.mark.asyncio
async def test_list_pending_normalises_bytes_keys(
    store: PendingAuthorizations, fake_redis: AsyncMock
) -> None:
    """Some redis-py client modes return bytes; SCAN handles both."""
    fake_redis.scan_iter = lambda **_kw: _AsyncIter([b"cp:pending:CP_B"])
    payload = {"cp_id": "CP_B", "first_seen_at": datetime.now(UTC).isoformat()}
    fake_redis.mget = AsyncMock(return_value=[json.dumps(payload)])

    rows: list[dict[str, Any]] = await store.list_pending()

    assert rows == [payload]
