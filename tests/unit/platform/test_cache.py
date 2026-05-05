"""Unit tests for the Authorize cache (E3-4).

Drives the `AuthorizeCache` against an in-memory fake Redis (a thin
dict-backed stub) so tests don't need a running Redis. The cache
talks to its `Redis` only via `.get()` / `.set()`, which the stub
implements faithfully.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from eveys_ocpp.platform.cache import AuthorizeCache
from eveys_ocpp.platform.client import IdTagInfo
from eveys_ocpp.settings import Settings


class _FakeRedis:
    """Just enough of `redis.asyncio.Redis` for the cache:
    `get(key)` returns the last-set value (or None); `set(key, value, ex=...)`
    stores it. ``ex`` (TTL) is recorded but not enforced — the cache's
    TTL semantics are Redis's job, not the cache's, and the tests
    don't need fake-clock TTL expiry."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.last_ex: dict[str, int | None] = {}
        # When `fail_get` / `fail_set` is True the corresponding op
        # raises — used to verify the cache's defensive fallback.
        self.fail_get = False
        self.fail_set = False

    async def get(self, key: str) -> str | None:
        if self.fail_get:
            raise RuntimeError("simulated redis get failure")
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        if self.fail_set:
            raise RuntimeError("simulated redis set failure")
        self.store[key] = value
        self.last_ex[key] = ex


def _settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "backend_authorize_cache_enabled": True,
        "backend_authorize_cache_ttl_seconds": 30,
    }
    base.update(overrides)
    return Settings(**base)


# ---- get() ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_returns_none_on_miss() -> None:
    redis = _FakeRedis()
    cache = AuthorizeCache(redis, settings=_settings())  # type: ignore[arg-type]
    assert await cache.get(cp_id="CP1", id_tag="RFID_X") is None


@pytest.mark.asyncio
async def test_get_returns_id_tag_info_on_hit() -> None:
    redis = _FakeRedis()
    redis.store["auth:CP1:RFID_X"] = json.dumps(
        {
            "status": "Accepted",
            "parent_id_tag": "FAMILY",
            "expiry_date": "2026-12-31T23:59:59+00:00",
        }
    )
    cache = AuthorizeCache(redis, settings=_settings())  # type: ignore[arg-type]

    result = await cache.get(cp_id="CP1", id_tag="RFID_X")
    assert result is not None
    assert result.status == "Accepted"
    assert result.parent_id_tag == "FAMILY"
    assert result.expiry_date == "2026-12-31T23:59:59+00:00"


@pytest.mark.asyncio
async def test_get_treats_malformed_value_as_miss() -> None:
    """Defensive: a value we can't parse forces a backend round-trip
    instead of serving wrong data."""
    redis = _FakeRedis()
    redis.store["auth:CP1:RFID_X"] = "not-json"
    cache = AuthorizeCache(redis, settings=_settings())  # type: ignore[arg-type]

    assert await cache.get(cp_id="CP1", id_tag="RFID_X") is None


@pytest.mark.asyncio
async def test_get_treats_non_dict_value_as_miss() -> None:
    redis = _FakeRedis()
    redis.store["auth:CP1:RFID_X"] = json.dumps([1, 2, 3])
    cache = AuthorizeCache(redis, settings=_settings())  # type: ignore[arg-type]

    assert await cache.get(cp_id="CP1", id_tag="RFID_X") is None


@pytest.mark.asyncio
async def test_get_returns_none_on_redis_failure() -> None:
    """A Redis blip on read → log + miss. Handler proceeds to backend."""
    redis = _FakeRedis()
    redis.fail_get = True
    cache = AuthorizeCache(redis, settings=_settings())  # type: ignore[arg-type]

    assert await cache.get(cp_id="CP1", id_tag="RFID_X") is None


@pytest.mark.asyncio
async def test_get_disabled_short_circuits_redis() -> None:
    """`enabled=False` → skip the Redis call entirely. Easy ops switch
    without env-rewriting."""
    redis = _FakeRedis()
    redis.fail_get = True  # would raise if cache actually called Redis
    cache = AuthorizeCache(
        redis,  # type: ignore[arg-type]
        settings=_settings(backend_authorize_cache_enabled=False),
    )

    # Disabled cache returns None without calling redis.
    assert await cache.get(cp_id="CP1", id_tag="RFID_X") is None


# ---- set() ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_stores_serialised_id_tag_info() -> None:
    redis = _FakeRedis()
    cache = AuthorizeCache(redis, settings=_settings())  # type: ignore[arg-type]

    await cache.set(
        cp_id="CP1",
        id_tag="RFID_X",
        info=IdTagInfo(
            status="Blocked",
            parent_id_tag=None,
            expiry_date=None,
        ),
    )

    raw = redis.store["auth:CP1:RFID_X"]
    parsed = json.loads(raw)
    assert parsed["status"] == "Blocked"
    assert parsed["parent_id_tag"] is None


@pytest.mark.asyncio
async def test_set_uses_configured_ttl() -> None:
    redis = _FakeRedis()
    cache = AuthorizeCache(
        redis,  # type: ignore[arg-type]
        settings=_settings(backend_authorize_cache_ttl_seconds=120),
    )
    await cache.set(
        cp_id="CP1",
        id_tag="RFID_X",
        info=IdTagInfo(status="Accepted"),
    )
    assert redis.last_ex["auth:CP1:RFID_X"] == 120


@pytest.mark.asyncio
async def test_set_swallows_redis_failure() -> None:
    """A Redis blip on write → log + drop. Never wedge the handler."""
    redis = _FakeRedis()
    redis.fail_set = True
    cache = AuthorizeCache(redis, settings=_settings())  # type: ignore[arg-type]

    # Should not raise; the handler that called us continues to the
    # charger reply path.
    await cache.set(
        cp_id="CP1",
        id_tag="RFID_X",
        info=IdTagInfo(status="Accepted"),
    )
    # Nothing got stored.
    assert "auth:CP1:RFID_X" not in redis.store


@pytest.mark.asyncio
async def test_set_disabled_short_circuits_redis() -> None:
    redis = _FakeRedis()
    redis.fail_set = True  # would raise if cache actually called Redis
    cache = AuthorizeCache(
        redis,  # type: ignore[arg-type]
        settings=_settings(backend_authorize_cache_enabled=False),
    )

    await cache.set(
        cp_id="CP1",
        id_tag="RFID_X",
        info=IdTagInfo(status="Accepted"),
    )
    # Cache didn't write — and the failing fake didn't raise either.
    assert redis.store == {}


# ---- key shape ------------------------------------------------------------


@pytest.mark.asyncio
async def test_key_isolates_cp_id_namespace() -> None:
    """Same id_tag at different chargers caches independently — the
    backend's authorize semantics include cp_id, so the cache key
    must too."""
    redis = _FakeRedis()
    cache = AuthorizeCache(redis, settings=_settings())  # type: ignore[arg-type]

    await cache.set(cp_id="CP_A", id_tag="RFID_X", info=IdTagInfo(status="Accepted"))
    await cache.set(cp_id="CP_B", id_tag="RFID_X", info=IdTagInfo(status="Blocked"))

    a = await cache.get(cp_id="CP_A", id_tag="RFID_X")
    b = await cache.get(cp_id="CP_B", id_tag="RFID_X")
    assert a is not None and a.status == "Accepted"
    assert b is not None and b.status == "Blocked"
