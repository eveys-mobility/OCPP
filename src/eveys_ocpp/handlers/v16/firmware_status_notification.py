"""FirmwareStatusNotification handler (charger-initiated).

OCPP 1.6 reference: FirmwareStatusNotification.req /
FirmwareStatusNotificationResponse, FirmwareManagement profile (E2-1F).

The charger emits this whenever the firmware-update state machine
transitions: ``Idle`` → ``Downloading`` → (``Downloaded`` | ``DownloadFailed``)
→ ``Installing`` → (``Installed`` | ``InstallationFailed``).
The CSMS uses it to know when a previously-issued ``UpdateFirmware``
landed (or failed), and to surface "firmware update in flight" to
ops dashboards without polling.

Behaviour mirrors ``DiagnosticsStatusNotification``:
- Update ``charge_points.last_firmware_status`` (latest-wins).
- Log at info level. The OCPP 1.6 Security profile (Phase 5 / E5-5)
  adds a wider enum (e.g. ``InvalidSignature``); the library lets it
  through and we persist whatever string the charger reports — the
  ``last_firmware_status`` column is ``String(32)`` precisely so the
  forward-compat is automatic.
- Reply with the empty conf the spec mandates.
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
from eveys_ocpp.persistence.repositories import update_firmware_status

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


async def handle(
    cp: EveysChargePoint, *, status: str, **_: object
) -> call_result.FirmwareStatusNotification:
    bind_contextvars(cp_id=cp.id, action="FirmwareStatusNotification", direction="rx")

    metrics_registry.FIRMWARE_STATUS_TOTAL.labels(status=status).inc()
    with time_handler("FirmwareStatusNotification"):
        try:
            async with session_scope(cp.session_factory) as session:
                await update_firmware_status(session, cp_id=cp.id, status=status)

            log.info("firmware_status_notification", status=status)

            # cp.firmware_status webhook source. Low volume (a few per
            # charger per update lifecycle); best-effort publish per the
            # same pattern the other emitters use — broker drop must
            # not crash the OCPP handler.
            if cp.event_producer is not None:
                envelope = events_pb2.EventEnvelope(
                    event_id=str(uuid.uuid4()),
                    occurred_at=datetime.now(UTC).isoformat(),
                    cp_id=cp.id,
                    schema_version="v1",
                    cp_firmware_status_changed=events_pb2.CpFirmwareStatusChanged(
                        status=status,
                    ),
                )
                try:
                    await cp.event_producer.publish(
                        topic=cp.settings.kafka_topic_cp_firmware_status,
                        key=cp.id,
                        value=envelope.SerializeToString(),
                    )
                except Exception as exc:
                    log.warning("firmware_status.publish_failed", error=str(exc))

            return call_result.FirmwareStatusNotification()
        except Exception as exc:
            record_handler_error("FirmwareStatusNotification", exc)
            raise
