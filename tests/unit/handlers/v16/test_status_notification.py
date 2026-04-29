"""Unit tests for the StatusNotification handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result

from eveys_ocpp.handlers.v16 import status_notification


@pytest.mark.asyncio
async def test_records_status(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    update = AsyncMock()
    monkeypatch.setattr(status_notification, "update_status", update)

    result = await status_notification.handle(
        fake_cp, connector_id=1, status="Available", error_code="NoError"
    )

    assert isinstance(result, call_result.StatusNotification)
    update.assert_awaited_once()
    assert update.await_args is not None
    assert update.await_args.kwargs == {"cp_id": "TEST_CP_001", "status": "Available"}


@pytest.mark.asyncio
async def test_ignores_extra_fields(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(status_notification, "update_status", AsyncMock())

    # OCPP 1.6 also defines `info`, `vendor_id`, `vendor_error_code`,
    # `timestamp` — we must accept them silently for now.
    result = await status_notification.handle(
        fake_cp,
        connector_id=1,
        status="Charging",
        error_code="NoError",
        info="ignored",
        vendor_id="ACME",
        vendor_error_code="V1",
        timestamp="2026-04-29T00:00:00+00:00",
    )

    assert isinstance(result, call_result.StatusNotification)
