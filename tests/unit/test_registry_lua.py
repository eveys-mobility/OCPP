"""Real-Redis tests for the registry's Lua compare-and-delete (issue #100).

The mocked tests in `test_registry.py` stub `redis.eval()` return values,
so a Lua syntax error or KEYS/ARGV mismatch in `_DEL_IF_OWNER` would still
pass them. In production that would silently break charger reconnect
routing (the wrong-pod-deletes-our-key race becomes possible again).

These tests exercise the script against a real Redis. Same skip pattern
as `test_idempotency.py`: silent skip when Redis is unreachable, hard
fail under `E2E_REQUIRE=1` so a CI config drift can't false-green this
module.
"""

from __future__ import annotations

import os
import socket
from collections.abc import AsyncIterator
from contextlib import closing

import pytest
from redis.asyncio import Redis

from eveys_ocpp.registry import Registry, _key
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
    _msg = f"Redis at {_REDIS_HOST}:{_REDIS_PORT} unreachable; registry-Lua tests need it"
    if _REDIS_REQUIRED:
        pytest.fail(
            f"{_msg}. E2E_REQUIRE=1 — the tests job must declare a "
            "`redis:7-alpine` service. CI config bug, not env issue.",
            pytrace=False,
        )
    pytestmark = pytest.mark.skip(reason=_msg)


@pytest.fixture
async def redis_client() -> AsyncIterator[Redis]:
    """Fresh client per test. The fixture also flushes the keys it
    touches on teardown so a failed test doesn't poison the next run."""
    client = Redis.from_url(
        f"redis://{_REDIS_HOST}:{_REDIS_PORT}/0",
        decode_responses=True,
    )
    yield client
    # Clean up any cp:online:* keys we created in tests.
    async for k in client.scan_iter("cp:online:LUA_TEST_*"):
        await client.delete(k)
    await client.aclose()


def _registry(redis: Redis, *, pod_id: str) -> Registry:
    return Registry(redis, settings=Settings(pod_id=pod_id))


@pytest.mark.asyncio
async def test_mark_offline_deletes_when_we_own_the_key(
    redis_client: Redis,
) -> None:
    """The basic happy path: this pod set the key, this pod releases it.

    Verifies the Lua script's GET/DEL chain and the success-path return
    value. If the script were syntactically broken (e.g. typo in
    `redis.call`), this test would crash with a NOSCRIPT-shaped error
    even though the unit-test mock is silent.
    """
    cp_id = "LUA_TEST_OWN"
    reg = _registry(redis_client, pod_id="pod-A")

    await reg.mark_online(cp_id)
    assert await redis_client.get(_key(cp_id)) == "pod-A"

    deleted = await reg.mark_offline(cp_id)
    assert deleted is True
    assert await redis_client.get(_key(cp_id)) is None


@pytest.mark.asyncio
async def test_mark_offline_preserves_key_when_owned_by_another_pod(
    redis_client: Redis,
) -> None:
    """The reconnect race: charger reconnected to pod-B between pod-A's
    disconnect detection and its mark_offline call. Pod-A must NOT
    delete pod-B's key.

    This is the bug the Lua compare-and-delete prevents. If someone
    accidentally swaps it for a plain DEL, this test fails — the mocked
    test would still pass.
    """
    cp_id = "LUA_TEST_RACE"
    pod_a = _registry(redis_client, pod_id="pod-A")
    pod_b = _registry(redis_client, pod_id="pod-B")

    # Pod A had the key, then the charger reconnected to pod B.
    await pod_a.mark_online(cp_id)
    await pod_b.mark_online(cp_id)  # overwrites with pod-B
    assert await redis_client.get(_key(cp_id)) == "pod-B"

    # Pod A's late mark_offline must NOT delete pod-B's key.
    deleted_by_a = await pod_a.mark_offline(cp_id)
    assert deleted_by_a is False
    assert await redis_client.get(_key(cp_id)) == "pod-B"

    # Pod B can still release its own key.
    deleted_by_b = await pod_b.mark_offline(cp_id)
    assert deleted_by_b is True
    assert await redis_client.get(_key(cp_id)) is None


@pytest.mark.asyncio
async def test_mark_offline_returns_false_when_key_already_expired(
    redis_client: Redis,
) -> None:
    """If the TTL elapsed before mark_offline runs, the GET returns
    nil (not the pod_id), so the script returns 0 — no key, nothing to
    delete, no decrement of the per-pod gauge."""
    cp_id = "LUA_TEST_EXPIRED"
    reg = _registry(redis_client, pod_id="pod-A")

    # No mark_online — simulate the key never existed (or expired).
    deleted = await reg.mark_offline(cp_id)
    assert deleted is False
