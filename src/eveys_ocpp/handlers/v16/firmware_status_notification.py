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

from typing import TYPE_CHECKING

from ocpp.v16 import call_result

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

    async with session_scope(cp.session_factory) as session:
        await update_firmware_status(session, cp_id=cp.id, status=status)

    log.info("firmware_status_notification", status=status)
    return call_result.FirmwareStatusNotification()
