"""Unit tests for the StatusNotification handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result

from eveys_ocpp._generated.events.v1 import events_pb2
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
async def test_accepts_extra_fields(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(status_notification, "update_status", AsyncMock())

    # OCPP 1.6 defines `info`, `vendor_id`, `vendor_error_code`,
    # `timestamp` — we accept and forward them on the Kafka event.
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


# ---- E2-8 Kafka emit -------------------------------------------------------


@pytest.mark.asyncio
async def test_publishes_to_cp_status_topic_when_producer_attached(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(status_notification, "update_status", AsyncMock())
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await status_notification.handle(
        fake_cp,
        connector_id=2,
        status="Charging",
        error_code="NoError",
        info="some info",
        vendor_id="ACME",
        vendor_error_code="V1",
        timestamp="2026-04-29T00:00:00+00:00",
    )

    fake_producer.publish.assert_awaited_once()
    call_kwargs = fake_producer.publish.await_args.kwargs
    assert call_kwargs["topic"] == fake_cp.settings.kafka_topic_cp_status
    assert call_kwargs["key"] == "TEST_CP_001"

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(call_kwargs["value"])
    assert envelope.cp_id == "TEST_CP_001"
    assert envelope.HasField("cp_status")
    assert envelope.cp_status.connector_id == 2
    assert envelope.cp_status.status == "Charging"
    assert envelope.cp_status.error_code == "NoError"
    assert envelope.cp_status.info == "some info"
    assert envelope.cp_status.vendor_id == "ACME"
    assert envelope.cp_status.vendor_error_code == "V1"
    assert envelope.cp_status.charger_reported_at == "2026-04-29T00:00:00+00:00"


@pytest.mark.asyncio
async def test_no_publish_when_producer_is_none(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(status_notification, "update_status", AsyncMock())
    fake_cp.event_producer = None

    result = await status_notification.handle(fake_cp, connector_id=1, status="Available")

    assert isinstance(result, call_result.StatusNotification)


@pytest.mark.asyncio
async def test_handler_survives_kafka_publish_exception(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Broker drop must not crash the OCPP handler."""
    monkeypatch.setattr(status_notification, "update_status", AsyncMock())
    fake_producer = AsyncMock()
    fake_producer.publish = AsyncMock(side_effect=RuntimeError("broker is down"))
    fake_cp.event_producer = fake_producer

    result = await status_notification.handle(fake_cp, connector_id=1, status="Available")

    assert isinstance(result, call_result.StatusNotification)
    fake_producer.publish.assert_awaited_once()


# ---- TC_024 — non-NoError error_code propagation --------------------------
#
# OCPP 1.6 §6.21 enumerates 17 charger error codes (ConnectorLockFailure,
# PowerSwitchFailure, EVCommunicationError, etc). The existing tests
# above all pass `error_code="NoError"` — they don't prove the handler
# carries a real fault through to Kafka or the Prometheus counter. A
# hypothetical filter that strips non-NoError codes (or a typo in the
# proto field) would silently break operator visibility into faults.
# This test pins the propagation contract.


@pytest.mark.asyncio
async def test_lock_failure_error_code_flows_to_kafka_envelope(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A charger reporting a faulted connector with
    `error_code=ConnectorLockFailure` must surface that exact value
    on the Kafka envelope's `cp_status.error_code` field. Operator
    dashboards (and downstream alerting) rely on it."""
    monkeypatch.setattr(status_notification, "update_status", AsyncMock())
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await status_notification.handle(
        fake_cp,
        connector_id=1,
        status="Faulted",
        error_code="ConnectorLockFailure",
        info="lock motor stalled",
        vendor_error_code="VENDOR-LOCK-7",
        timestamp="2026-05-08T22:00:00+00:00",
    )

    fake_producer.publish.assert_awaited_once()
    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(fake_producer.publish.await_args.kwargs["value"])
    assert envelope.cp_status.status == "Faulted"
    assert envelope.cp_status.error_code == "ConnectorLockFailure"
    # Vendor-extension fields must round-trip too — operators page on
    # vendor_error_code when the OCPP standard code isn't specific
    # enough.
    assert envelope.cp_status.vendor_error_code == "VENDOR-LOCK-7"
    assert envelope.cp_status.info == "lock motor stalled"


@pytest.mark.asyncio
async def test_metric_label_carries_error_code(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`STATUS_NOTIFICATIONS_TOTAL` is labelled by both `status` and
    `error_code`; both must reflect the charger's report. Without
    this, the fleet dashboard's "faults by error_code" panel is
    silently empty."""
    from eveys_ocpp.metrics import registry as metrics_registry

    monkeypatch.setattr(status_notification, "update_status", AsyncMock())

    # Capture the current value to confirm it increments by 1.
    before = metrics_registry.STATUS_NOTIFICATIONS_TOTAL.labels(
        status="Faulted", error_code="EVCommunicationError"
    )._value.get()

    await status_notification.handle(
        fake_cp,
        connector_id=1,
        status="Faulted",
        error_code="EVCommunicationError",
    )

    after = metrics_registry.STATUS_NOTIFICATIONS_TOTAL.labels(
        status="Faulted", error_code="EVCommunicationError"
    )._value.get()
    assert after == before + 1


@pytest.mark.asyncio
async def test_pending_cp_raises_security_error(fake_cp: Any) -> None:
    """A pending device must be refused with a CALLERROR and never
    touch Postgres."""
    from unittest.mock import MagicMock

    from ocpp.exceptions import SecurityError

    fake_cp.is_pending = True
    fake_cp.session_factory = MagicMock(
        side_effect=AssertionError("session_factory must not be used while pending")
    )

    with pytest.raises(SecurityError):
        await status_notification.handle(fake_cp, connector_id=1, status="Available")
