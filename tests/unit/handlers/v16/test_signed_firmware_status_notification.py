"""Unit tests for the SignedFirmwareStatusNotification handler.

OCPP 1.6 Security Whitepaper §4.4 / TC_080, TC_081. Charger reports
progress on a SignedUpdateFirmware. Same shape as the plain
firmware_status_notification handler — same column, same metric —
but the security-specific status values (`InvalidSignature`,
`SignatureVerified`, `InstallVerificationFailed`) are what
distinguish a *signed* firmware update from a plain one.

The verify-fails dance pins these specific status values: a
regression that filtered or remapped them would silently break
operator alerting on signature failures.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result

from eveys_ocpp.handlers.v16 import signed_firmware_status_notification


@pytest.mark.asyncio
async def test_records_status_and_returns_empty_conf(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    update = AsyncMock()
    monkeypatch.setattr(signed_firmware_status_notification, "update_firmware_status", update)

    result = await signed_firmware_status_notification.handle(
        fake_cp, status="Installed", request_id=42
    )

    assert isinstance(result, call_result.SignedFirmwareStatusNotification)
    update.assert_awaited_once()
    assert update.await_args.kwargs == {
        "cp_id": "TEST_CP_001",
        "status": "Installed",
    }


@pytest.mark.asyncio
async def test_persists_invalid_signature_status(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`InvalidSignature` is the TC_081 page-worthy value — the
    charger downloaded firmware but couldn't verify the signature.
    Operator alerting on this value is the entire point of having
    SignedUpdateFirmware as a separate RPC."""
    update = AsyncMock()
    monkeypatch.setattr(signed_firmware_status_notification, "update_firmware_status", update)

    await signed_firmware_status_notification.handle(
        fake_cp, status="InvalidSignature", request_id=99
    )

    assert update.await_args.kwargs["status"] == "InvalidSignature"


@pytest.mark.asyncio
async def test_persists_signature_verified_status(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`SignatureVerified` is the affirmative success step in the
    signed-firmware flow — operators want to see it on the dashboard
    alongside `Installed` to confirm the security checks ran."""
    update = AsyncMock()
    monkeypatch.setattr(signed_firmware_status_notification, "update_firmware_status", update)

    await signed_firmware_status_notification.handle(
        fake_cp, status="SignatureVerified", request_id=1
    )

    assert update.await_args.kwargs["status"] == "SignatureVerified"


@pytest.mark.asyncio
async def test_persists_install_verification_failed_status(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`InstallVerificationFailed` — signature checked out at
    download time but the post-install verification step failed.
    Different incident shape from `InvalidSignature`."""
    update = AsyncMock()
    monkeypatch.setattr(signed_firmware_status_notification, "update_firmware_status", update)

    await signed_firmware_status_notification.handle(
        fake_cp, status="InstallVerificationFailed", request_id=1
    )

    assert update.await_args.kwargs["status"] == "InstallVerificationFailed"


@pytest.mark.asyncio
async def test_handler_does_not_require_request_id(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """OCPP marks `request_id` required, but charger firmwares vary;
    a missing field shouldn't crash the handler."""
    update = AsyncMock()
    monkeypatch.setattr(signed_firmware_status_notification, "update_firmware_status", update)

    result = await signed_firmware_status_notification.handle(fake_cp, status="Downloading")

    assert isinstance(result, call_result.SignedFirmwareStatusNotification)
    update.assert_awaited_once()


@pytest.mark.asyncio
async def test_metric_label_carries_status(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """`FIRMWARE_STATUS_TOTAL{status}` is shared with the plain
    firmware-status handler — that's intentional, operators read
    one panel. The label must reflect security-specific values too
    (`InvalidSignature`)."""
    from eveys_ocpp.metrics import registry as metrics_registry

    monkeypatch.setattr(signed_firmware_status_notification, "update_firmware_status", AsyncMock())

    before = metrics_registry.FIRMWARE_STATUS_TOTAL.labels(status="InvalidSignature")._value.get()

    await signed_firmware_status_notification.handle(
        fake_cp, status="InvalidSignature", request_id=42
    )

    after = metrics_registry.FIRMWARE_STATUS_TOTAL.labels(status="InvalidSignature")._value.get()
    assert after == before + 1
