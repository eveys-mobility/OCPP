"""SignedFirmwareStatusNotification handler (charger-initiated).

OCPP 1.6 Security Whitepaper §4.4 / TC_080, TC_081. Sibling of the
plain ``FirmwareStatusNotification`` handler — same shape, wider
status enum (14 values vs 7). The two security-specific values
operators page on are:

- ``InvalidSignature`` — charger downloaded the firmware, attempted
  signature verification against the supplied signing cert, and the
  signature didn't match. TC_081 expects this exact value.
- ``InstallVerificationFailed`` — signature verified but the
  charger's post-install verification step failed.

Plus the affirmative success step ``SignatureVerified``, which a
firmware-update dashboard wants to show alongside ``Installed``.

Behaviour:
- Latest-wins update of ``charge_points.last_firmware_status``
  (same column as the plain firmware-status handler — they're
  alternative paths to the same operator-facing state).
- Increment ``eveys_ocpp_firmware_status_total{status}`` so the
  fleet-wide "firmware updates by status" panel populates with
  these security-specific values too. Same metric as the plain
  handler — that's intentional, operators read one panel.
- Reply with the empty conf the spec mandates.

The ``request_id`` field correlates the status with the original
SignedUpdateFirmware command. We log it but don't persist (no
per-request state on the charger row).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ocpp.exceptions import SecurityError
from ocpp.v16 import call_result

from eveys_ocpp.metrics import record_handler_error, time_handler
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import update_firmware_status

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


async def handle(
    cp: EveysChargePoint,
    *,
    status: str,
    request_id: int | None = None,
    **_: object,
) -> call_result.SignedFirmwareStatusNotification:
    bind_contextvars(cp_id=cp.id, action="SignedFirmwareStatusNotification", direction="rx")

    # Pending-authorization gate — only BootNotification is honoured
    # while the operator hasn't approved this cp_id. `SecurityError`
    # is untyped upstream (mobilityhouse/ocpp ships no `py.typed`).
    if cp.is_pending:
        raise SecurityError(  # type: ignore[no-untyped-call]
            details={"reason": "authorization pending; operator has not authorized this cp_id"}
        )

    metrics_registry.FIRMWARE_STATUS_TOTAL.labels(status=status).inc()
    with time_handler("SignedFirmwareStatusNotification"):
        try:
            async with session_scope(cp.session_factory) as session:
                await update_firmware_status(session, cp_id=cp.id, status=status)

            log.info(
                "signed_firmware_status_notification",
                status=status,
                request_id=request_id,
            )
            return call_result.SignedFirmwareStatusNotification()
        except Exception as exc:
            record_handler_error("SignedFirmwareStatusNotification", exc)
            raise
