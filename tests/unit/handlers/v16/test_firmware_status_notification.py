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


# ----- cp.firmware_status_changed Kafka publish (#169) ----------------------


@pytest.mark.asyncio
async def test_publishes_envelope_on_status_change(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Successful firmware-status notification publishes one
    CpFirmwareStatusChanged envelope on the configured topic."""
    from eveys_ocpp._generated.events.v1 import events_pb2

    monkeypatch.setattr(firmware_status_notification, "update_firmware_status", AsyncMock())
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await firmware_status_notification.handle(fake_cp, status="Downloading")

    fake_producer.publish.assert_awaited_once()
    kwargs = fake_producer.publish.await_args.kwargs
    assert kwargs["topic"] == fake_cp.settings.kafka_topic_cp_firmware_status
    assert kwargs["key"] == "TEST_CP_001"

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(kwargs["value"])
    assert envelope.cp_id == "TEST_CP_001"
    assert envelope.WhichOneof("payload") == "cp_firmware_status_changed"
    assert envelope.cp_firmware_status_changed.status == "Downloading"


@pytest.mark.asyncio
async def test_no_producer_does_not_publish(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """`event_producer = None` (Kafka-less local stack / unit tests)
    runs the handler clean — no publish attempted, no crash."""
    monkeypatch.setattr(firmware_status_notification, "update_firmware_status", AsyncMock())
    fake_cp.event_producer = None
    # No assertion target — the absence of a raised exception IS the test.
    await firmware_status_notification.handle(fake_cp, status="Installed")


@pytest.mark.asyncio
async def test_publish_failure_does_not_crash_handler(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Best-effort publish: a broker drop must not break the OCPP
    response — the charger needs an empty conf back regardless."""
    monkeypatch.setattr(firmware_status_notification, "update_firmware_status", AsyncMock())
    fake_producer = AsyncMock()
    fake_producer.publish = AsyncMock(side_effect=RuntimeError("kafka down"))
    fake_cp.event_producer = fake_producer

    result = await firmware_status_notification.handle(fake_cp, status="Installing")
    assert isinstance(result, call_result.FirmwareStatusNotification)
