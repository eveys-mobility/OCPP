"""Unit tests for the FirmwareStatusNotification handler (E2-1F)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result

from eveys_ocpp.handlers.v16 import firmware_status_notification


@pytest.mark.asyncio
async def test_persists_status_and_returns_empty_conf(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Latest-wins update of `last_firmware_status`; empty conf back."""
    update = AsyncMock()
    monkeypatch.setattr(firmware_status_notification, "update_firmware_status", update)

    result = await firmware_status_notification.handle(fake_cp, status="Downloading")

    assert isinstance(result, call_result.FirmwareStatusNotification)
    update.assert_awaited_once()
    assert update.await_args is not None
    assert update.await_args.kwargs["cp_id"] == "TEST_CP_001"
    assert update.await_args.kwargs["status"] == "Downloading"


@pytest.mark.asyncio
async def test_accepts_security_profile_statuses(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Phase 5 / Security profile widens the FirmwareStatus enum
    (e.g. `InvalidSignature`, `SignatureVerified`). The handler must
    not narrow — it persists whatever string the charger reports so
    the column is automatically forward-compatible."""
    monkeypatch.setattr(firmware_status_notification, "update_firmware_status", AsyncMock())
    for status in (
        "Downloading",
        "Downloaded",
        "DownloadFailed",
        "Installing",
        "Installed",
        "InstallationFailed",
        "Idle",
        # Security profile additions — must still flow through.
        "SignatureVerified",
        "InvalidSignature",
    ):
        result = await firmware_status_notification.handle(fake_cp, status=status)
        assert isinstance(result, call_result.FirmwareStatusNotification)


@pytest.mark.asyncio
async def test_ignores_extra_kwargs(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(firmware_status_notification, "update_firmware_status", AsyncMock())
    result = await firmware_status_notification.handle(fake_cp, status="Installed", request_id=123)
    assert isinstance(result, call_result.FirmwareStatusNotification)
