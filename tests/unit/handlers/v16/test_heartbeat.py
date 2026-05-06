"""Unit tests for the Heartbeat handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result

from eveys_ocpp.handlers.v16 import heartbeat


@pytest.mark.asyncio
async def test_returns_current_time_iso(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(heartbeat, "update_heartbeat", AsyncMock())

    result = await heartbeat.handle(fake_cp)

    assert isinstance(result, call_result.Heartbeat)
    # Reasonable shape: ISO 8601 with timezone.
    assert "T" in result.current_time
    assert result.current_time.endswith("+00:00")


@pytest.mark.asyncio
async def test_refreshes_last_heartbeat(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    update = AsyncMock()
    monkeypatch.setattr(heartbeat, "update_heartbeat", update)

    await heartbeat.handle(fake_cp)

    update.assert_awaited_once()
    assert update.await_args is not None
    assert update.await_args.kwargs["cp_id"] == "TEST_CP_001"


@pytest.mark.asyncio
async def test_refreshes_redis_ttl_when_registry_attached(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(heartbeat, "update_heartbeat", AsyncMock())

    fake_registry = AsyncMock()
    fake_registry.refresh = AsyncMock(return_value=True)
    fake_cp.registry = fake_registry

    await heartbeat.handle(fake_cp)

    fake_registry.refresh.assert_awaited_once_with("TEST_CP_001")
    fake_registry.mark_online.assert_not_awaited()


@pytest.mark.asyncio
async def test_reclaims_registry_when_key_expired(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If TTL expired between heartbeats, re-mark the charger as online."""
    monkeypatch.setattr(heartbeat, "update_heartbeat", AsyncMock())

    fake_registry = AsyncMock()
    fake_registry.refresh = AsyncMock(return_value=False)
    fake_cp.registry = fake_registry

    await heartbeat.handle(fake_cp)

    fake_registry.refresh.assert_awaited_once_with("TEST_CP_001")
    fake_registry.mark_online.assert_awaited_once_with("TEST_CP_001")


@pytest.mark.asyncio
async def test_no_registry_calls_when_registry_is_none(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """W1-style local stack without Redis: handler must still work."""
    monkeypatch.setattr(heartbeat, "update_heartbeat", AsyncMock())
    fake_cp.registry = None

    result = await heartbeat.handle(fake_cp)
    assert result is not None  # no exception
