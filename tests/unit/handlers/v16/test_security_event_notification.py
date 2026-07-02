"""Unit tests for the SecurityEventNotification handler (TC_077, TC_078).

OCPP 1.6 Security Whitepaper §4. Audit-grade event log; one row per
event in `security_events`. Operators tail the Kafka topic for
SIEM-style alerting.

Charger clocks are untrusted (AGENTS rule 7), so the handler:
- persists the charger's claimed `timestamp` AS-IS in `reported_at`
- stamps server-receive time in `received_at`
- forwards both unchanged to the Kafka envelope's payload + envelope
  outer fields respectively

Tests below pin those contracts with verify-fails-without-fix
checked before commit (commit message has the dance).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.handlers.v16 import security_event_notification


@pytest.mark.asyncio
async def test_records_event_and_returns_empty_conf(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Happy path: handler invokes the repo function with the right
    args and returns the empty conf the spec mandates."""
    record = AsyncMock()
    monkeypatch.setattr(security_event_notification, "record_security_event", record)

    result = await security_event_notification.handle(
        fake_cp,
        type="FirmwareUpdated",
        timestamp="2026-05-08T22:00:00+00:00",
    )

    assert isinstance(result, call_result.SecurityEventNotification)
    record.assert_awaited_once()
    kwargs = record.await_args.kwargs
    assert kwargs["cp_id"] == "TEST_CP_001"
    assert kwargs["event_type"] == "FirmwareUpdated"
    assert kwargs["tech_info"] is None
    # `reported_at` is parsed to a real datetime (not the raw string).
    from datetime import datetime

    assert isinstance(kwargs["reported_at"], datetime)
    assert kwargs["reported_at"].tzinfo is not None


@pytest.mark.asyncio
async def test_persists_tech_info_when_provided(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Vendor extensions arrive in `tech_info` (string). Empty/None
    is the proto3 default; a real value must round-trip."""
    record = AsyncMock()
    monkeypatch.setattr(security_event_notification, "record_security_event", record)

    await security_event_notification.handle(
        fake_cp,
        type="InvalidFirmwareSignature",
        timestamp="2026-05-08T22:00:00+00:00",
        tech_info="vendor:acme cert_serial=ABC123",
    )

    assert record.await_args.kwargs["tech_info"] == "vendor:acme cert_serial=ABC123"


@pytest.mark.asyncio
async def test_unparseable_timestamp_falls_back_to_now(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A vendor that emits a non-ISO-8601 timestamp shouldn't crash
    the handler — log loud, fall back to received-at-style 'now'.
    Charger clocks are already untrusted, so the only correctness
    guarantee is that we keep ingesting events."""
    record = AsyncMock()
    monkeypatch.setattr(security_event_notification, "record_security_event", record)

    result = await security_event_notification.handle(
        fake_cp,
        type="FirmwareUpdated",
        timestamp="not-a-real-timestamp",
    )

    assert isinstance(result, call_result.SecurityEventNotification)
    # The repo is still called — we don't drop the event on bad
    # timestamps. `reported_at` is a fallback datetime.
    record.assert_awaited_once()
    from datetime import datetime

    assert isinstance(record.await_args.kwargs["reported_at"], datetime)


# ---- Kafka emit -----------------------------------------------------------


@pytest.mark.asyncio
async def test_publishes_to_cp_security_event_topic_when_producer_attached(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When event_producer is wired, the handler publishes a
    `cp.security_event` envelope. SIEM consumers tail this for
    alerting; the type, charger_reported_at, and tech_info round-trip
    verbatim."""
    monkeypatch.setattr(security_event_notification, "record_security_event", AsyncMock())
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await security_event_notification.handle(
        fake_cp,
        type="InvalidSecurityEventCertificate",
        timestamp="2026-05-08T22:00:00+00:00",
        tech_info="cert_subject=CN=evil",
    )

    fake_producer.publish.assert_awaited_once()
    call_kwargs = fake_producer.publish.await_args.kwargs
    assert call_kwargs["topic"] == fake_cp.settings.kafka_topic_cp_security_event
    assert call_kwargs["key"] == "TEST_CP_001"

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(call_kwargs["value"])
    assert envelope.cp_id == "TEST_CP_001"
    assert envelope.HasField("cp_security_event")
    assert envelope.cp_security_event.type == "InvalidSecurityEventCertificate"
    assert envelope.cp_security_event.charger_reported_at == "2026-05-08T22:00:00+00:00"
    assert envelope.cp_security_event.tech_info == "cert_subject=CN=evil"


@pytest.mark.asyncio
async def test_no_publish_when_producer_is_none(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Kafka path is best-effort — when no producer is wired the
    handler still persists + returns the empty conf."""
    monkeypatch.setattr(security_event_notification, "record_security_event", AsyncMock())
    fake_cp.event_producer = None

    result = await security_event_notification.handle(
        fake_cp, type="FirmwareUpdated", timestamp="2026-05-08T22:00:00+00:00"
    )

    assert isinstance(result, call_result.SecurityEventNotification)


@pytest.mark.asyncio
async def test_handler_survives_kafka_publish_exception(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Broker drop must not crash the OCPP handler. The audit row is
    already in Postgres (above); Kafka is the SIEM-fanout path."""
    monkeypatch.setattr(security_event_notification, "record_security_event", AsyncMock())
    fake_producer = AsyncMock()
    fake_producer.publish = AsyncMock(side_effect=RuntimeError("broker is down"))
    fake_cp.event_producer = fake_producer

    result = await security_event_notification.handle(
        fake_cp, type="FirmwareUpdated", timestamp="2026-05-08T22:00:00+00:00"
    )

    assert isinstance(result, call_result.SecurityEventNotification)
    fake_producer.publish.assert_awaited_once()


# ---- Metric label ---------------------------------------------------------


@pytest.mark.asyncio
async def test_metric_label_carries_event_type(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`SECURITY_EVENTS_TOTAL` is labelled by event_type; the fleet
    dashboard's "security events by type" panel reads this. A future
    regression that hardcoded the label would silently empty the
    panel."""
    from eveys_ocpp.metrics import registry as metrics_registry

    monkeypatch.setattr(security_event_notification, "record_security_event", AsyncMock())

    before = metrics_registry.SECURITY_EVENTS_TOTAL.labels(
        event_type="InvalidFirmwareSignature"
    )._value.get()

    await security_event_notification.handle(
        fake_cp,
        type="InvalidFirmwareSignature",
        timestamp="2026-05-08T22:00:00+00:00",
    )

    after = metrics_registry.SECURITY_EVENTS_TOTAL.labels(
        event_type="InvalidFirmwareSignature"
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
        await security_event_notification.handle(
            fake_cp, type="FirmwareUpdated", timestamp="2026-07-02T16:00:00Z"
        )
