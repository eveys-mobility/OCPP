"""Unit tests for the DiagnosticsStatusNotification handler (E2-1F)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result

from eveys_ocpp.handlers.v16 import diagnostics_status_notification


@pytest.mark.asyncio
async def test_persists_status_and_returns_empty_conf(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Latest-wins update of `last_diagnostics_status`; empty conf back."""
    update = AsyncMock()
    monkeypatch.setattr(diagnostics_status_notification, "update_diagnostics_status", update)

    result = await diagnostics_status_notification.handle(fake_cp, status="Uploading")

    assert isinstance(result, call_result.DiagnosticsStatusNotification)
    update.assert_awaited_once()
    assert update.await_args is not None
    assert update.await_args.kwargs["cp_id"] == "TEST_CP_001"
    assert update.await_args.kwargs["status"] == "Uploading"


@pytest.mark.asyncio
async def test_accepts_each_spec_status(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """The four OCPP 1.6 statuses all flow through the same path."""
    monkeypatch.setattr(diagnostics_status_notification, "update_diagnostics_status", AsyncMock())
    for status in ("Idle", "Uploading", "Uploaded", "UploadFailed"):
        result = await diagnostics_status_notification.handle(fake_cp, status=status)
        assert isinstance(result, call_result.DiagnosticsStatusNotification)


@pytest.mark.asyncio
async def test_ignores_extra_kwargs(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec defines no other fields, but a vendor extension passing
    extra kwargs through the schema must not trip the handler."""
    monkeypatch.setattr(diagnostics_status_notification, "update_diagnostics_status", AsyncMock())
    result = await diagnostics_status_notification.handle(
        fake_cp, status="Uploaded", vendor_extra="ignored"
    )
    assert isinstance(result, call_result.DiagnosticsStatusNotification)


# ----- cp.diagnostics_status_changed Kafka publish (#169) -------------------


@pytest.mark.asyncio
async def test_publishes_envelope_on_status_change(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Successful diagnostics-status notification publishes one
    CpDiagnosticsStatusChanged envelope on the configured topic."""
    from eveys_ocpp._generated.events.v1 import events_pb2

    monkeypatch.setattr(diagnostics_status_notification, "update_diagnostics_status", AsyncMock())
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await diagnostics_status_notification.handle(fake_cp, status="Uploading")

    fake_producer.publish.assert_awaited_once()
    kwargs = fake_producer.publish.await_args.kwargs
    assert kwargs["topic"] == fake_cp.settings.kafka_topic_cp_diagnostics_status
    assert kwargs["key"] == "TEST_CP_001"

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(kwargs["value"])
    assert envelope.cp_id == "TEST_CP_001"
    assert envelope.WhichOneof("payload") == "cp_diagnostics_status_changed"
    assert envelope.cp_diagnostics_status_changed.status == "Uploading"


@pytest.mark.asyncio
async def test_no_producer_does_not_publish(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """`event_producer = None` (Kafka-less local stack / unit tests)
    runs the handler clean — no publish attempted, no crash."""
    monkeypatch.setattr(diagnostics_status_notification, "update_diagnostics_status", AsyncMock())
    fake_cp.event_producer = None
    await diagnostics_status_notification.handle(fake_cp, status="Uploaded")


@pytest.mark.asyncio
async def test_publish_failure_does_not_crash_handler(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Best-effort publish: a broker drop must not break the OCPP
    response — the charger needs an empty conf back regardless."""
    monkeypatch.setattr(diagnostics_status_notification, "update_diagnostics_status", AsyncMock())
    fake_producer = AsyncMock()
    fake_producer.publish = AsyncMock(side_effect=RuntimeError("kafka down"))
    fake_cp.event_producer = fake_producer

    result = await diagnostics_status_notification.handle(fake_cp, status="UploadFailed")
    assert isinstance(result, call_result.DiagnosticsStatusNotification)
