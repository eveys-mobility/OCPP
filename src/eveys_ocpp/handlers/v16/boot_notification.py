"""BootNotification handler.

OCPP 1.6 reference: BootNotification.req / BootNotificationResponse.
Spec section: TODO — fill in exact OCA OCPP 1.6 Edition 2 section once
task C-1 lands the spec PDF on the shared drive. Until then, cite the
action by name only.

JSON Schemas: `ocpp.v16.schemas.BootNotification` (request) and
`BootNotificationResponse` (response). Validated automatically by the
mobilityhouse/ocpp library (ADR-0002) — we cannot send a malformed payload.

Behavior:
1. **Replay gate (E2-11, AGENTS rule 3).** Consult the idempotency
   cache keyed by ``(cp_id, message_id)``. Cache hit → this is a
   retry of a request we already handled; skip the DB upsert, backend
   call, AND the Kafka emit, return the canonical Accepted response.
   Cache miss → proceed and the cache key is now recorded.
2. Upsert the charger row.
3. Call the backend's `POST /api/eveys/charge-points/register` (E3-5)
   if a `backend_client` is wired. The backend's `registration_status`
   and `heartbeat_interval_seconds` flow through to the OCPP response
   verbatim. Backend unreachable → fall back per
   `settings.backend_register_fallback` (default `accept_offline`).
4. Publish a `cp.boot` event to Kafka (E2-8) — only on Accepted (we
   don't emit `cp.boot` for Pending/Rejected; downstream consumers
   wouldn't want to materialize a charger that's not actually booted).
   Best-effort: a publish failure is logged, dropped, never raised.
5. Reply with the resolved `RegistrationStatus` and heartbeat interval.

The contract permits `Accepted`/`Pending`/`Rejected`; the gateway
forwards verbatim per ADR-0023. An unrecognised string from the
backend (forward-compat) maps to `Pending` — safer default than
`Accepted` for an unknown shape (the spec says `Pending` triggers the
charger to re-send BootNotification later).

Deviations from the OCA spec to verify before W2 / OCTT (see
`docs/08-ocpp-conformance.md`):
- We always Accept when no backend is wired (W1/dev mode).
- `interval` falls back to `Settings.heartbeat_interval_seconds` only
  when the backend returns 0 or is unreachable.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ocpp.v16 import call_result
from ocpp.v16.enums import RegistrationStatus

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import upsert_charge_point_boot
from eveys_ocpp.platform import BackendBusinessError, BackendUnavailableError

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


# OCPP wire string → mobilityhouse/ocpp enum. Forward-compat: an
# unknown status from the backend maps to Pending — per the OCPP spec
# Pending tells the charger to re-send BootNotification, which is a
# safer default than Accepted for an unrecognised shape.
_REGISTRATION_STATUS_MAP: dict[str, RegistrationStatus] = {
    "Accepted": RegistrationStatus.accepted,
    "Pending": RegistrationStatus.pending,
    "Rejected": RegistrationStatus.rejected,
}


def _response(
    received_at: datetime, status: RegistrationStatus, interval: int
) -> call_result.BootNotification:
    return call_result.BootNotification(
        current_time=received_at.isoformat(),
        interval=interval,
        status=status,
    )


def _register_fallback(
    cp: EveysChargePoint, exc: BackendUnavailableError
) -> tuple[RegistrationStatus, int]:
    """Backend `/charge-points/register` unreachable past the retry
    budget. Apply the configured fallback policy. The local DB row was
    already upserted, so when the backend recovers it can reconcile."""
    policy = cp.settings.backend_register_fallback
    log.warning(
        "boot_notification.backend_unavailable",
        policy=policy,
        error=str(exc),
        error_code=exc.error_code,
    )
    if policy == "accept_offline":
        return RegistrationStatus.accepted, cp.settings.heartbeat_interval_seconds
    return RegistrationStatus.rejected, cp.settings.heartbeat_interval_seconds


def _build_envelope(*, cp_id: str, payload: events_pb2.CpBoot, occurred_at: datetime) -> bytes:
    envelope = events_pb2.EventEnvelope(
        event_id=str(uuid.uuid4()),
        occurred_at=occurred_at.isoformat(),
        cp_id=cp_id,
        schema_version="v1",
        cp_boot=payload,
    )
    return envelope.SerializeToString()


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
    # skip the DB write, backend call, and Kafka emit. A Redis outage
    # here falls through to the normal path — better a rare double-
    # write than a wedged handler when the cache is the problem
    # (ADR-0017). Replay returns canonical Accepted with the configured
    # default interval; we accepted the first time, we won't re-call
    # the backend just because the charger retried.
    if cp.idempotency is not None and message_id:
        try:
            replay = await cp.idempotency.check_and_record(cp_id=cp.id, message_id=message_id)
        except Exception as exc:
            log.warning("boot_notification.idempotency_failed", error=str(exc))
            replay = False
        if replay:
            log.info("boot_notification.replay_ignored", message_id=message_id)
            return _response(
                received_at,
                RegistrationStatus.accepted,
                cp.settings.heartbeat_interval_seconds,
            )

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

    # Backend round-trip (E3-5). If no backend is wired (W1/dev), accept
    # locally with the configured interval. Otherwise the backend is
    # the source of truth for both `registration_status` and
    # `heartbeat_interval_seconds`.
    status = RegistrationStatus.accepted
    interval = cp.settings.heartbeat_interval_seconds
    if cp.backend_client is not None:
        try:
            result = await cp.backend_client.register_charge_point(
                cp_id=cp.id,
                vendor=charge_point_vendor,
                model=charge_point_model,
                firmware_version=firmware_version,
                serial_number=charge_point_serial_number,
                boot_at=received_at.isoformat(),
                idempotency_key=f"ocpp-boot-{cp.id}-{message_id or 'no-msg-id'}",
            )
            status = _REGISTRATION_STATUS_MAP.get(
                result.registration_status, RegistrationStatus.pending
            )
            interval = result.heartbeat_interval_seconds or cp.settings.heartbeat_interval_seconds
        except BackendUnavailableError as exc:
            status, interval = _register_fallback(cp, exc)
        except BackendBusinessError as exc:
            log.warning(
                "boot_notification.backend_business_rejected",
                error_code=exc.error_code,
                message=str(exc),
            )
            status = RegistrationStatus.rejected

    log.info(
        "boot_notification.decided",
        decision=status.value,
        interval=interval,
        vendor=charge_point_vendor,
        model=charge_point_model,
    )

    # Only emit `cp.boot` on Accepted. Pending/Rejected mean the
    # charger isn't actually online for downstream consumers
    # (mobile BFF, fleet dashboards) to materialize.
    if status == RegistrationStatus.accepted and cp.event_producer is not None:
        payload = events_pb2.CpBoot(
            vendor=charge_point_vendor or "",
            model=charge_point_model or "",
            firmware_version=firmware_version or "",
            serial_number=charge_point_serial_number or "",
            status=events_pb2.CP_BOOT_STATUS_ACCEPTED,
        )
        envelope_bytes = _build_envelope(cp_id=cp.id, payload=payload, occurred_at=received_at)
        # Best-effort publish — a broker drop must not crash the OCPP
        # handler, otherwise a flaky broker DoSes the gateway as
        # chargers retry. Match the meter_values pattern.
        try:
            await cp.event_producer.publish(
                topic=cp.settings.kafka_topic_cp_boot,
                key=cp.id,
                value=envelope_bytes,
            )
        except Exception as exc:
            log.warning("boot_notification.publish_failed", error=str(exc))

    return _response(received_at, status, interval)
