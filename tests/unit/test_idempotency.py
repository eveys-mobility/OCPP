"""Unit tests for the Redis-backed idempotency cache (E2-11).

Tests run against a real Redis when reachable; in CI the tests job
declares a redis service so the module-level reachability probe always
finds it. ``E2E_REQUIRE=1`` turns silent skips into hard failures so a
CI config drift can't accidentally produce false-green coverage on
this module.
"""

from __future__ import annotations

import os
import socket
import uuid
from collections.abc import AsyncIterator
from contextlib import closing

import pytest
from redis.asyncio import Redis

from eveys_ocpp.idempotency import IdempotencyCache
from eveys_ocpp.settings import Settings

_REDIS_HOST = os.environ.get("E2E_REDIS_HOST", "localhost")
_REDIS_PORT = int(os.environ.get("E2E_REDIS_PORT", "6379"))
_REDIS_REQUIRED = os.environ.get("E2E_REQUIRE") == "1"


def _redis_reachable() -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.settimeout(0.5)
        try:
            s.connect((_REDIS_HOST, _REDIS_PORT))
        except OSError:
            return False
        return True


if not _redis_reachable():
    _msg = f"Redis at {_REDIS_HOST}:{_REDIS_PORT} unreachable; idempotency tests need it"
    if _REDIS_REQUIRED:
        pytest.fail(
            f"{_msg}. E2E_REQUIRE=1 — the tests job must declare a "
            "`redis:7-alpine` service. CI config bug, not env issue.",
            pytrace=False,
        )
    pytestmark = pytest.mark.skip(reason=_msg)


@pytest.fixture
async def redis_client() -> AsyncIterator[Redis]:
    client = Redis.from_url(
        f"redis://{_REDIS_HOST}:{_REDIS_PORT}/0",
        decode_responses=True,
    )
    yield client
    await client.aclose()


@pytest.fixture
def settings() -> Settings:
    return Settings()


@pytest.mark.asyncio
async def test_first_sighting_returns_false(redis_client: Redis, settings: Settings) -> None:
    """A never-before-seen (cp_id, message_id) pair is not a replay."""
    cache = IdempotencyCache(redis_client, settings=settings)
    cp_id = "CP_FIRST"
    message_id = str(uuid.uuid4())

    seen = await cache.check_and_record(cp_id=cp_id, message_id=message_id)
    assert seen is False


@pytest.mark.asyncio
async def test_second_sighting_returns_true(redis_client: Redis, settings: Settings) -> None:
    """Same pair within the TTL window is a replay."""
    cache = IdempotencyCache(redis_client, settings=settings)
    cp_id = "CP_REPLAY"
    message_id = str(uuid.uuid4())

    first = await cache.check_and_record(cp_id=cp_id, message_id=message_id)
    second = await cache.check_and_record(cp_id=cp_id, message_id=message_id)

    assert first is False
    assert second is True


@pytest.mark.asyncio
async def test_different_message_ids_are_independent(
    redis_client: Redis, settings: Settings
) -> None:
    cache = IdempotencyCache(redis_client, settings=settings)
    cp_id = "CP_INDEP"
    msg_a = str(uuid.uuid4())
    msg_b = str(uuid.uuid4())

    assert await cache.check_and_record(cp_id=cp_id, message_id=msg_a) is False
    assert await cache.check_and_record(cp_id=cp_id, message_id=msg_b) is False


@pytest.mark.asyncio
async def test_different_cp_ids_are_independent(redis_client: Redis, settings: Settings) -> None:
    """Two chargers happening to use the same UUID don't collide."""
    cache = IdempotencyCache(redis_client, settings=settings)
    msg = str(uuid.uuid4())

    assert await cache.check_and_record(cp_id="CP_A", message_id=msg) is False
    assert await cache.check_and_record(cp_id="CP_B", message_id=msg) is False


@pytest.mark.asyncio
async def test_ttl_is_applied(redis_client: Redis, settings: Settings) -> None:
    """The Redis key carries the configured TTL.

    Verifies the cache doesn't accumulate keys forever — without the
    TTL, a long-running pod would leak one key per inbound replay-able
    OCPP message.
    """
    cache = IdempotencyCache(redis_client, settings=settings)
    cp_id = "CP_TTL"
    message_id = str(uuid.uuid4())

    await cache.check_and_record(cp_id=cp_id, message_id=message_id)
    ttl = await redis_client.ttl(f"cp:idem:{cp_id}:{message_id}")

    assert ttl > 0
    assert ttl <= settings.idempotency_ttl_seconds
