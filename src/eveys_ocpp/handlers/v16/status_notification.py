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
2. Return an empty response (the spec defines no response body fields).

Per-connector history (one row per state transition) is intentionally
NOT stored in Postgres — it's high-volume and goes to ClickHouse via
Kafka (tasks E2-8 + E2-14). Until those land we keep just the latest
status as a debug aid.

Deviations from the OCA spec to verify before W2 / OCTT
(see `docs/08-ocpp-conformance.md`):
- We accept and silently drop the optional `info`, `vendor_id`,
  `vendor_error_code`, `timestamp` fields. The spec allows that — we
  log them but don't persist.
- No alerting on `Faulted` state. Surfacing faults to ops dashboards is
  scheduled for Phase 4.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ocpp.v16 import call_result

from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import update_status

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


async def handle(
    cp: EveysChargePoint,
    *,
    connector_id: int,
    status: str,
    error_code: str = "NoError",
    **_: object,  # info, vendor_id, vendor_error_code, timestamp — not yet persisted
) -> call_result.StatusNotification:
    bind_contextvars(cp_id=cp.id, action="StatusNotification", direction="rx")

    async with session_scope(cp.session_factory) as session:
        await update_status(session, cp_id=cp.id, status=status)

    log.info(
        "status_notification",
        connector_id=connector_id,
        status=status,
        error_code=error_code,
    )

    return call_result.StatusNotification()
