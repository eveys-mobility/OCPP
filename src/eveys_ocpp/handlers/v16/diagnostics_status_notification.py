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

from typing import TYPE_CHECKING

from ocpp.v16 import call_result

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

    metrics_registry.DIAGNOSTICS_STATUS_TOTAL.labels(status=status).inc()
    with time_handler("DiagnosticsStatusNotification"):
        try:
            async with session_scope(cp.session_factory) as session:
                await update_diagnostics_status(session, cp_id=cp.id, status=status)

            log.info("diagnostics_status_notification", status=status)
            return call_result.DiagnosticsStatusNotification()
        except Exception as exc:
            record_handler_error("DiagnosticsStatusNotification", exc)
            raise
