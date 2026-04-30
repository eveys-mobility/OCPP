"""BootNotification handler.

OCPP 1.6 reference: BootNotification.req / BootNotificationResponse.
Spec section: TODO — fill in exact OCA OCPP 1.6 Edition 2 section once
task C-1 lands the spec PDF on the shared drive. Until then, cite the
action by name only.

JSON Schemas: `ocpp.v16.schemas.BootNotification` (request) and
`BootNotificationResponse` (response). Validated automatically by the
mobilityhouse/ocpp library (ADR-0002) — we cannot send a malformed payload.

Behavior in v0:
1. **Replay gate (E2-11, AGENTS rule 3).** Consult the idempotency
   cache keyed by ``(cp_id, message_id)``. Cache hit → this is a
   retry of a request we already handled; skip the DB upsert AND the
   Kafka emit, return the canonical Accepted response. Cache miss →
   proceed and the cache key is now recorded.
2. Upsert the charger row.
3. Reply with `RegistrationStatus.accepted` and the configured
   heartbeat interval (`interval` is in seconds, per the
   BootNotificationResponse schema).

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


def _accepted_response(received_at: datetime, interval: int) -> call_result.BootNotification:
    return call_result.BootNotification(
        current_time=received_at.isoformat(),
        interval=interval,
        status=RegistrationStatus.accepted,
    )


async def handle(
    cp: EveysChargePoint,
    *,
    message_id: str | None = None,
    charge_point_vendor: str | None = None,
    charge_point_model: str | None = None,
    firmware_version: str | None = None,
    charge_point_serial_number: str | None = None,
    **_: object,  # OCPP 1.6 includes other optional fields we don't persist yet
) -> call_result.BootNotification:
    bind_contextvars(cp_id=cp.id, action="BootNotification", direction="rx")

    received_at = datetime.now(UTC)

    # Replay gate (E2-11). If the cache says we've seen this exact
    # message_id within the TTL window, return the same response and
    # skip the DB write + Kafka emit. A Redis outage here falls
    # through to the normal path — better a rare double-write than a
    # wedged handler when the cache is the problem (ADR-0017).
    if cp.idempotency is not None and message_id:
        try:
            replay = await cp.idempotency.check_and_record(cp_id=cp.id, message_id=message_id)
        except Exception as exc:
            log.warning("boot_notification.idempotency_failed", error=str(exc))
            replay = False
        if replay:
            log.info("boot_notification.replay_ignored", message_id=message_id)
            return _accepted_response(received_at, cp.settings.heartbeat_interval_seconds)

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

    return _accepted_response(received_at, cp.settings.heartbeat_interval_seconds)
