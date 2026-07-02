"""DiagnosticsStatusNotification handler (charger-initiated).

OCPP 1.6 reference: DiagnosticsStatusNotification.req /
DiagnosticsStatusNotificationResponse, FirmwareManagement profile (E2-1F).

The charger emits this whenever the diagnostics-upload state machine
transitions: ``Idle`` → ``Uploading`` → (``Uploaded`` | ``UploadFailed``).
The CSMS uses it to know when a previously-issued ``GetDiagnostics``
finished, and to surface "diagnostics in flight" to ops dashboards
without polling.

Behaviour:
- Update ``charge_points.last_diagnostics_status`` (latest-wins) so the
  next ``GetChargerStatus`` / operator query reads the current state.
- Log at info level. Per-transition history is not written to Postgres
  (low volume; logs are enough until Phase 4 wires it through Kafka if
  ever needed).
- Reply with the empty conf the spec mandates.
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
from eveys_ocpp.persistence.repositories import update_diagnostics_status

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


async def handle(
    cp: EveysChargePoint, *, status: str, **_: object
) -> call_result.DiagnosticsStatusNotification:
    bind_contextvars(cp_id=cp.id, action="DiagnosticsStatusNotification", direction="rx")

    # Pending-authorization gate — only BootNotification is honoured
    # while the operator hasn't approved this cp_id.
    if cp.is_pending:
        raise SecurityError(
            details={"reason": "authorization pending; operator has not authorized this cp_id"}
        )

    metrics_registry.DIAGNOSTICS_STATUS_TOTAL.labels(status=status).inc()
    with time_handler("DiagnosticsStatusNotification"):
        try:
            async with session_scope(cp.session_factory) as session:
                await update_diagnostics_status(session, cp_id=cp.id, status=status)

            log.info("diagnostics_status_notification", status=status)

            # cp.diagnostics_status webhook source. Low volume (a few
            # per charger per upload lifecycle); best-effort publish
            # per the same pattern the other emitters use — broker
            # drop must not crash the OCPP handler.
            if cp.event_producer is not None:
                envelope = events_pb2.EventEnvelope(
                    event_id=str(uuid.uuid4()),
                    occurred_at=datetime.now(UTC).isoformat(),
                    cp_id=cp.id,
                    schema_version="v1",
                    cp_diagnostics_status_changed=events_pb2.CpDiagnosticsStatusChanged(
                        status=status,
                    ),
                )
                try:
                    await cp.event_producer.publish(
                        topic=cp.settings.kafka_topic_cp_diagnostics_status,
                        key=cp.id,
                        value=envelope.SerializeToString(),
                    )
                except Exception as exc:
                    log.warning("diagnostics_status.publish_failed", error=str(exc))

            return call_result.DiagnosticsStatusNotification()
        except Exception as exc:
            record_handler_error("DiagnosticsStatusNotification", exc)
            raise
