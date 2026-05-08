"""SecurityEventNotification handler (charger-initiated).

OCPP 1.6 reference: Security Whitepaper §4 (SecurityEventNotification.req
+ .conf). The charger emits this for any of the 18 spec-defined event
types — some CRITICAL (e.g. `InvalidFirmwareSignature`,
`InvalidSecurityEventCertificate`) which operator alerting must page
on, others NON-CRITICAL (e.g. `FirmwareUpdated`) which are routine.
The CSMS retains every event for audit (TC_077, TC_078).

Behaviour:
1. Insert a new row into `security_events` (append-only log; not
   latest-wins). Foreign key requires the BootNotification handler
   to have run first; an event before boot raises `ValueError` and
   metrics it as a handler error so the on-call team notices.
2. Publish a `cp.security_event` Kafka envelope so downstream SIEM
   consumers can tail it. Best-effort: a publish failure is logged
   and dropped, same as `meter_values` / `status_notification`.
3. Increment the per-event-type counter so the fleet dashboard's
   "security events by type" panel populates.
4. Reply with the empty conf the spec mandates.

The 18 spec-defined event types are stored as the charger-reported
string; column width allows for vendor extensions and future spec
additions without a migration. Operators classify CRITICAL vs
NON-CRITICAL downstream from the type field — keeping that logic out
of the gateway means a future spec amendment doesn't require a
gateway code change.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ocpp.v16 import call_result

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.metrics import record_handler_error, time_handler
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import record_security_event

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


def _build_envelope(
    *,
    cp_id: str,
    payload: events_pb2.CpSecurityEvent,
    occurred_at: datetime,
) -> bytes:
    envelope = events_pb2.EventEnvelope(
        event_id=str(uuid.uuid4()),
        occurred_at=occurred_at.isoformat(),
        cp_id=cp_id,
        schema_version="v1",
        cp_security_event=payload,
    )
    return envelope.SerializeToString()


def _parse_reported_at(timestamp: str) -> datetime:
    """The OCPP spec's `timestamp` is ISO-8601. Pydantic-style parse;
    fall back to "now" only if the charger sent something we can't
    interpret — log loud so a misbehaving vendor is visible.

    Charger clocks are untrusted (AGENTS rule 7), so a successful
    parse here does not mean the value is *correct* — it just means
    we have something to put in the column. Operators anchor on
    `received_at` for ordering."""
    try:
        return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        log.warning(
            "security_event.unparseable_timestamp",
            charger_reported=timestamp,
        )
        return datetime.now(UTC)


async def handle(
    cp: EveysChargePoint,
    *,
    type: str,
    timestamp: str,
    tech_info: str | None = None,
    **_: object,
) -> call_result.SecurityEventNotification:
    bind_contextvars(cp_id=cp.id, action="SecurityEventNotification", direction="rx")

    metrics_registry.SECURITY_EVENTS_TOTAL.labels(event_type=type).inc()
    with time_handler("SecurityEventNotification"):
        try:
            received_at = datetime.now(UTC)
            reported_at = _parse_reported_at(timestamp)

            async with session_scope(cp.session_factory) as session:
                await record_security_event(
                    session,
                    cp_id=cp.id,
                    event_type=type,
                    reported_at=reported_at,
                    tech_info=tech_info,
                )

            log.info(
                "security_event_notification",
                event_type=type,
                charger_reported_at=timestamp,
                tech_info_present=tech_info is not None,
            )

            if cp.event_producer is not None:
                payload = events_pb2.CpSecurityEvent(
                    type=type,
                    charger_reported_at=timestamp,
                    tech_info=tech_info or "",
                )
                envelope_bytes = _build_envelope(
                    cp_id=cp.id, payload=payload, occurred_at=received_at
                )
                # Best-effort: a Kafka publish failure must not crash
                # the OCPP handler. Same rationale as status_notification
                # / meter_values — the audit-grade row is already in
                # Postgres above; Kafka is the SIEM-fanout path.
                try:
                    await cp.event_producer.publish(
                        topic=cp.settings.kafka_topic_cp_security_event,
                        key=cp.id,
                        value=envelope_bytes,
                    )
                except Exception as exc:
                    log.warning("security_event.publish_failed", error=str(exc))

            return call_result.SecurityEventNotification()
        except Exception as exc:
            record_handler_error("SecurityEventNotification", exc)
            raise
