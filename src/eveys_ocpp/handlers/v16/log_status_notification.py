"""LogStatusNotification handler (charger-initiated).

OCPP 1.6 Security Whitepaper §4.6 / TC_079. The charger emits this
to report progress on a previously-issued `GetLog` upload:
``Idle`` → ``Uploading`` → (``Uploaded`` | ``UploadFailure``).
Spec also defines ``BadMessage`` / ``NotSupportedOperation`` /
``PermissionDenied`` for charger-side rejections.

The CSMS uses it to know when a security-log retrieval finished
(or failed). Latest-wins update of `charge_points.last_log_status`
mirrors the existing `firmware_status_notification` /
`diagnostics_status_notification` pattern.

Per-event audit history is intentionally NOT stored here — for the
security-log audit trail specifically, the events themselves arrive
via `SecurityEventNotification` (TC_077/078, PR #109) and that
handler writes the audit-grade Postgres rows. This handler tracks
upload-progress only.

The `request_id` field correlates the status with the original
GetLog command. Operators issuing concurrent uploads need it; we
log it but don't persist (no per-request state on the charger row).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ocpp.exceptions import SecurityError
from ocpp.v16 import call_result

from eveys_ocpp.metrics import record_handler_error, time_handler
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import update_log_status

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


async def handle(
    cp: EveysChargePoint,
    *,
    status: str,
    request_id: int | None = None,
    **_: object,
) -> call_result.LogStatusNotification:
    bind_contextvars(cp_id=cp.id, action="LogStatusNotification", direction="rx")

    # Pending-authorization gate — only BootNotification is honoured
    # while the operator hasn't approved this cp_id. `SecurityError`
    # is untyped upstream (mobilityhouse/ocpp ships no `py.typed`).
    if cp.is_pending:
        raise SecurityError(  # type: ignore[no-untyped-call]
            details={"reason": "authorization pending; operator has not authorized this cp_id"}
        )

    metrics_registry.LOG_STATUS_TOTAL.labels(status=status).inc()
    with time_handler("LogStatusNotification"):
        try:
            async with session_scope(cp.session_factory) as session:
                await update_log_status(session, cp_id=cp.id, status=status)

            log.info(
                "log_status_notification",
                status=status,
                request_id=request_id,
            )
            return call_result.LogStatusNotification()
        except Exception as exc:
            record_handler_error("LogStatusNotification", exc)
            raise
