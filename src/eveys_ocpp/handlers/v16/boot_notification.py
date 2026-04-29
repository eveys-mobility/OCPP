"""BootNotification handler.

OCPP 1.6 reference: BootNotification.req / BootNotificationResponse.
Spec section: TODO — fill in exact OCA OCPP 1.6 Edition 2 section once
task C-1 lands the spec PDF on the shared drive. Until then, cite the
action by name only.

JSON Schemas: `ocpp.v16.schemas.BootNotification` (request) and
`BootNotificationResponse` (response). Validated automatically by the
mobilityhouse/ocpp library (ADR-0002) — we cannot send a malformed payload.

Behavior in v0:
1. Upsert the charger row (idempotent on `cp_id` — AGENTS rule 3).
2. Reply with `RegistrationStatus.accepted` and the configured heartbeat
   interval (`interval` is in seconds, per the BootNotificationResponse
   schema).

The spec also defines `Pending` and `Rejected` for the response status:
- `Pending` is used when the CSMS wants to delay registration (e.g. firmware
  whitelist check). We don't gate v0 boots — add policy gating in a future
  ADR if needed.
- `Rejected` permanently refuses the charger. Same rationale: not in v0.

Deviations from the OCA spec to verify before W2 / OCTT (see
`docs/08-ocpp-conformance.md`):
- We always Accept; no charger blocklist.
- `interval` is read from `Settings.heartbeat_interval_seconds` (default 300s).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ocpp.v16 import call_result
from ocpp.v16.enums import RegistrationStatus

from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import upsert_charge_point_boot

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


async def handle(
    cp: EveysChargePoint,
    *,
    charge_point_vendor: str | None = None,
    charge_point_model: str | None = None,
    firmware_version: str | None = None,
    charge_point_serial_number: str | None = None,
    **_: object,  # OCPP 1.6 includes other optional fields we don't persist yet
) -> call_result.BootNotification:
    bind_contextvars(cp_id=cp.id, action="BootNotification", direction="rx")

    received_at = datetime.now(UTC)
    async with session_scope(cp.session_factory) as session:
        await upsert_charge_point_boot(
            session,
            cp_id=cp.id,
            vendor=charge_point_vendor,
            model=charge_point_model,
            firmware_version=firmware_version,
            serial_number=charge_point_serial_number,
            boot_at=received_at,
        )

    log.info(
        "boot_notification.accepted",
        vendor=charge_point_vendor,
        model=charge_point_model,
    )

    return call_result.BootNotification(
        current_time=received_at.isoformat(),
        interval=cp.settings.heartbeat_interval_seconds,
        status=RegistrationStatus.accepted,
    )
