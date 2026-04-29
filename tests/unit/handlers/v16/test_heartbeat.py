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
