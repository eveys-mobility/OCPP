"""Unit tests for the BootNotification handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result

from eveys_ocpp.handlers.v16 import boot_notification


@pytest.mark.asyncio
async def test_returns_accepted_with_configured_interval(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    result = await boot_notification.handle(
        fake_cp,
        charge_point_vendor="ACME",
        charge_point_model="X1",
        firmware_version="1.0.0",
        charge_point_serial_number="SN001",
    )

    assert isinstance(result, call_result.BootNotification)
    assert result.status == "Accepted"
    assert result.interval == fake_cp.settings.heartbeat_interval_seconds
    upsert.assert_awaited_once()


@pytest.mark.asyncio
async def test_persists_charger_metadata(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    await boot_notification.handle(
        fake_cp,
        charge_point_vendor="ACME",
        charge_point_model="X1",
        firmware_version="1.0.0",
        charge_point_serial_number="SN001",
    )

    call = upsert.await_args
    assert call is not None
    kwargs = call.kwargs
    assert kwargs["cp_id"] == "TEST_CP_001"
    assert kwargs["vendor"] == "ACME"
    assert kwargs["model"] == "X1"
    assert kwargs["firmware_version"] == "1.0.0"
    assert kwargs["serial_number"] == "SN001"


@pytest.mark.asyncio
async def test_handles_missing_optional_fields(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.status == "Accepted"
