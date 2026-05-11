"""Unit tests for the Redis online registry (E2-9).

These tests mock the redis.asyncio client. End-to-end correctness against
a real Redis is covered by the e2e suite (`tests/e2e/test_local_smoke.py`)
once the live-stack tests pick it up.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from eveys_ocpp.registry import Registry, _key
from eveys_ocpp.settings import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(pod_id="test-pod-001")


@pytest.fixture
def fake_redis() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def registry(fake_redis: AsyncMock, settings: Settings) -> Registry:
    return Registry(fake_redis, settings=settings)


def test_key_format() -> None:
    assert _key("CP_001") == "cp:online:CP_001"


@pytest.mark.asyncio
async def test_mark_online_writes_pod_id_with_ttl(
    registry: Registry, fake_redis: AsyncMock, settings: Settings
) -> None:
    await registry.mark_online("CP_001")
    fake_redis.set.assert_awaited_once_with(
        "cp:online:CP_001",
        "test-pod-001",
        ex=settings.redis_online_ttl_seconds,
    )


@pytest.mark.asyncio
async def test_refresh_returns_true_when_key_exists(
    registry: Registry, fake_redis: AsyncMock
) -> None:
    fake_redis.expire.return_value = 1  # Redis returns 1 if key updated
    assert await registry.refresh("CP_001") is True


@pytest.mark.asyncio
async def test_refresh_returns_false_when_key_gone(
    registry: Registry, fake_redis: AsyncMock
) -> None:
    fake_redis.expire.return_value = 0  # Redis returns 0 if key didn't exist
    assert await registry.refresh("CP_001") is False


@pytest.mark.asyncio
async def test_mark_offline_returns_true_when_we_owned_the_key(
    registry: Registry, fake_redis: AsyncMock
) -> None:
    fake_redis.eval.return_value = 1  # Lua script returned 1 = deleted
    assert await registry.mark_offline("CP_001") is True


@pytest.mark.asyncio
async def test_mark_offline_returns_false_when_owned_by_another_pod(
    registry: Registry, fake_redis: AsyncMock
) -> None:
    """Reconnect-race scenario: charger reconnected to another pod
    between our disconnect and our mark_offline call. Lua compare-and-
    delete sees the value is now `pod-B`, not us; returns 0.
    """
    fake_redis.eval.return_value = 0
    assert await registry.mark_offline("CP_001") is False


@pytest.mark.asyncio
async def test_get_pod_returns_pod_id(registry: Registry, fake_redis: AsyncMock) -> None:
    fake_redis.get.return_value = "pod-other-002"
    assert await registry.get_pod("CP_001") == "pod-other-002"


@pytest.mark.asyncio
async def test_get_pod_returns_none_when_offline(registry: Registry, fake_redis: AsyncMock) -> None:
    fake_redis.get.return_value = None
    assert await registry.get_pod("CP_001") is None


@pytest.mark.asyncio
async def test_is_online_true_when_key_present(registry: Registry, fake_redis: AsyncMock) -> None:
    fake_redis.exists.return_value = 1
    assert await registry.is_online("CP_001") is True


@pytest.mark.asyncio
async def test_is_online_false_when_key_absent(registry: Registry, fake_redis: AsyncMock) -> None:
    fake_redis.exists.return_value = 0
    assert await registry.is_online("CP_001") is False


@pytest.mark.asyncio
async def test_close_disposes_redis_client(registry: Registry, fake_redis: AsyncMock) -> None:
    await registry.close()
    fake_redis.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_record_offline_marker_writes_hash_without_ttl(
    registry: Registry, fake_redis: AsyncMock, settings: Settings
) -> None:
    await registry.record_offline_marker("CP_001", reason="clean")
    fake_redis.hset.assert_awaited_once()
    args, kwargs = fake_redis.hset.call_args
    assert args[0] == "cp:last_offline_at:CP_001"
    mapping = kwargs["mapping"]
    assert mapping["pod_id"] == settings.pod_id
    assert mapping["reason"] == "clean"
    # ISO-8601 with tz suffix; parsing must succeed.
    from datetime import datetime as _dt

    parsed = _dt.fromisoformat(mapping["went_offline_at"])
    assert parsed.tzinfo is not None


@pytest.mark.asyncio
async def test_pop_offline_marker_returns_none_when_absent(
    registry: Registry, fake_redis: AsyncMock
) -> None:
    fake_redis.hgetall.return_value = {}
    assert await registry.pop_offline_marker("CP_001") is None
    fake_redis.delete.assert_not_called()


@pytest.mark.asyncio
async def test_pop_offline_marker_returns_dict_and_deletes(
    registry: Registry, fake_redis: AsyncMock
) -> None:
    fake_redis.hgetall.return_value = {
        "went_offline_at": "2026-05-11T12:00:00+00:00",
        "pod_id": "pod-prev",
        "reason": "error",
    }
    result = await registry.pop_offline_marker("CP_001")
    assert result == {
        "went_offline_at": "2026-05-11T12:00:00+00:00",
        "pod_id": "pod-prev",
        "reason": "error",
    }
    fake_redis.delete.assert_awaited_once_with("cp:last_offline_at:CP_001")


def test_settings_redis_defaults() -> None:
    s = Settings()
    assert s.redis_url == "redis://localhost:6379/0"
    assert s.redis_online_ttl_seconds == 120
    assert s.pod_id != ""  # populated by socket.gethostname()
