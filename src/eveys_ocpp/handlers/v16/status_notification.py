"""StatusNotification handler.

OCPP 1.6 reference: StatusNotification.req / StatusNotificationResponse.
Spec section: TODO (task C-1).

Charger reports state changes per connector: Available, Preparing,
Charging, SuspendedEV, SuspendedEVSE, Finishing, Reserved, Unavailable,
Faulted.

JSON Schemas: `ocpp.v16.schemas.StatusNotification` and
`StatusNotificationResponse`. The schema enforces the `status` and
`error_code` enums — payloads with unknown values are rejected by the
library before reaching us.

Behavior in v0:
1. Update `last_status` on the charger row (latest-wins).
2. Publish a `cp.status` event to Kafka (E2-8) so downstream consumers
   (mobile BFF, ops dashboards) see state transitions in near-real-time.
   Best-effort: a publish failure is logged and dropped.
3. Return an empty response (the spec defines no response body fields).

Per-connector history (one row per state transition) is intentionally
NOT stored in Postgres — it's high-volume and goes to ClickHouse via
Kafka (E2-8 + E2-14). The `last_status` row in Postgres is just a
latest-wins debug aid.

Deviations from the OCA spec to verify before W2 / OCTT
(see `docs/08-ocpp-conformance.md`):
- The optional `info`, `vendor_id`, `vendor_error_code`, `timestamp`
  fields go on the Kafka event but are not persisted to Postgres.
- No alerting on `Faulted` state. Surfacing faults to ops dashboards is
  scheduled for Phase 4.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ocpp.exceptions import SecurityError
from ocpp.v16 import call_result

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.metrics import record_handler_error, time_handler
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import update_status

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


def _build_envelope(*, cp_id: str, payload: events_pb2.CpStatus, occurred_at: datetime) -> bytes:
    envelope = events_pb2.EventEnvelope(
        event_id=str(uuid.uuid4()),
        occurred_at=occurred_at.isoformat(),
        cp_id=cp_id,
        schema_version="v1",
        cp_status=payload,
    )
    return envelope.SerializeToString()


async def handle(
    cp: EveysChargePoint,
    *,
    connector_id: int,
    status: str,
    error_code: str = "NoError",
    info: str | None = None,
    vendor_id: str | None = None,
    vendor_error_code: str | None = None,
    timestamp: str | None = None,
    **_: object,
) -> call_result.StatusNotification:
    bind_contextvars(cp_id=cp.id, action="StatusNotification", direction="rx")

    # Pending-authorization gate — only BootNotification is honoured
    # while the operator hasn't approved this cp_id.
    if cp.is_pending:
        raise SecurityError(
            details={"reason": "authorization pending; operator has not authorized this cp_id"}
        )

    metrics_registry.STATUS_NOTIFICATIONS_TOTAL.labels(status=status, error_code=error_code).inc()
    with time_handler("StatusNotification"):
        try:
            received_at = datetime.now(UTC)
            async with session_scope(cp.session_factory) as session:
                await update_status(session, cp_id=cp.id, status=status)

            log.info(
                "status_notification",
                connector_id=connector_id,
                status=status,
                error_code=error_code,
            )

            if cp.event_producer is not None:
                payload = events_pb2.CpStatus(
                    connector_id=connector_id,
                    status=status,
                    error_code=error_code,
                    info=info or "",
                    vendor_id=vendor_id or "",
                    vendor_error_code=vendor_error_code or "",
                    charger_reported_at=timestamp or "",
                )
                envelope_bytes = _build_envelope(
                    cp_id=cp.id, payload=payload, occurred_at=received_at
                )
                # Best-effort: a Kafka publish failure must not crash the OCPP
                # handler. Same rationale as meter_values.
                try:
                    await cp.event_producer.publish(
                        topic=cp.settings.kafka_topic_cp_status,
                        key=cp.id,
                        value=envelope_bytes,
                    )
                except Exception as exc:
                    log.warning("status_notification.publish_failed", error=str(exc))

            return call_result.StatusNotification()
        except Exception as exc:
            record_handler_error("StatusNotification", exc)
            raise
