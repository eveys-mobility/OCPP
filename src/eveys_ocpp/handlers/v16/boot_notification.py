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

import asyncio
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ocpp.exceptions import SecurityError
from ocpp.v16 import call, call_result
from ocpp.v16.enums import RegistrationStatus

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.metrics import record_handler_error, time_handler
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import upsert_charge_point_boot
from eveys_ocpp.platform import BackendBusinessError, BackendUnavailableError

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)

# Charger needs a moment to settle after BootNotification before we
# start pushing ChangeConfiguration calls — some firmwares ignore or
# error on commands during the boot handshake. 2s is the empirical
# settle window from the mobilityhouse/ocpp sim and a few production
# Schneider/ABB units. See eveys-mobility/OCPP#238.
_POST_BOOT_PUSH_DELAY_SECONDS = 2.0

# Per-CALL gap between consecutive ChangeConfigurations. Some firmwares
# serialize CALL processing internally and will queue-drop or reject the
# tail of a rapid burst. 250 ms is below the human-perceivable level
# but well above any single-CALL processing time we've observed.
_POST_BOOT_INTER_CALL_GAP_SECONDS = 0.25


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

    with time_handler("BootNotification") as _set_outcome:
        try:
            return await _handle_inner(
                cp,
                message_id=message_id,
                charge_point_vendor=charge_point_vendor,
                charge_point_model=charge_point_model,
                firmware_version=firmware_version,
                charge_point_serial_number=charge_point_serial_number,
                received_at=received_at,
                set_outcome=_set_outcome,
            )
        except Exception as exc:
            record_handler_error("BootNotification", exc)
            raise


async def _handle_inner(
    cp: EveysChargePoint,
    *,
    message_id: str | None,
    charge_point_vendor: str | None,
    charge_point_model: str | None,
    firmware_version: str | None,
    charge_point_serial_number: str | None,
    received_at: datetime,
    set_outcome: object,
) -> call_result.BootNotification:
    # Pending-authorization gate. When the WS was accepted for a device
    # the operator hasn't approved yet, BootNotification is the only
    # OCPP action allowed through — but its metadata is cached in the
    # Redis pending row (so the operator's Console shows vendor / model
    # / firmware for the decision), NOT upserted into `charge_points`,
    # and NOT emitted as `cp.boot` (downstream consumers must not see
    # boot events for a device that isn't part of the fleet yet).
    if cp.is_pending:
        return await _handle_pending_boot(
            cp,
            charge_point_vendor=charge_point_vendor,
            charge_point_model=charge_point_model,
            firmware_version=firmware_version,
            charge_point_serial_number=charge_point_serial_number,
            received_at=received_at,
            set_outcome=set_outcome,
        )

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
            metrics_registry.BOOT_REPLAYS_TOTAL.inc()
            metrics_registry.BOOT_NOTIFICATIONS_TOTAL.labels(decision="Accepted").inc()
            if callable(set_outcome):
                set_outcome("replay")
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
            ocpp_version=cp.ocpp_version,
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
    metrics_registry.BOOT_NOTIFICATIONS_TOTAL.labels(decision=status.value).inc()

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

    # Push the full post-boot ChangeConfiguration matrix on every
    # Accepted boot (eveys-mobility/OCPP#238). Fire-and-forget — we
    # don't want the boot reply to wait on a charger that's slow to
    # ack the ChangeConfiguration burst. The task sleeps briefly
    # first to let the charger settle (some firmwares reject
    # ChangeConfiguration during the boot handshake window) and then
    # pushes each key with a small gap between CALLs so firmwares
    # with serial CALL processing don't queue-drop the tail.
    if status == RegistrationStatus.accepted:
        _schedule_post_boot_configuration_push(cp)

    return _response(received_at, status, interval)


async def _handle_pending_boot(
    cp: EveysChargePoint,
    *,
    charge_point_vendor: str | None,
    charge_point_model: str | None,
    firmware_version: str | None,
    charge_point_serial_number: str | None,
    received_at: datetime,
    set_outcome: object,
) -> call_result.BootNotification:
    """BootNotification path for a device the operator hasn't authorised.

    Merges the vendor/model/firmware/serial into the Redis pending row
    so the operator's queue UI has real metadata to show. Does not
    touch Postgres, does not publish `cp.boot`, does not schedule the
    post-boot ChangeConfiguration burst.

    If the pending row's TTL expired between the WS upgrade and this
    Boot arriving, the pending device has effectively been forgotten —
    close the WS with 1008 and raise SecurityError, matching the
    contract that pending devices never reach the fleet without an
    operator decision.
    """
    if cp.pending_store is not None:
        row = await cp.pending_store.record_boot(
            cp_id=cp.id,
            vendor=charge_point_vendor,
            model=charge_point_model,
            firmware=firmware_version,
            serial_number=charge_point_serial_number,
            now=received_at,
        )
        if row is None:
            log.warning(
                "boot_notification.pending_expired",
                cp_id=cp.id,
            )
            await cp._connection.close(1008, "authorization pending expired")
            raise SecurityError(
                details={"reason": "authorization pending expired"},
            )

    log.info(
        "boot_notification.pending_recorded",
        cp_id=cp.id,
        vendor=charge_point_vendor,
        model=charge_point_model,
        firmware=firmware_version,
    )
    metrics_registry.BOOT_NOTIFICATIONS_TOTAL.labels(decision="PendingAuthorization").inc()
    if callable(set_outcome):
        set_outcome("pending_authorization")

    return _response(
        received_at,
        RegistrationStatus.accepted,
        cp.settings.heartbeat_interval_seconds,
    )


def _post_boot_keys(cp: EveysChargePoint) -> list[tuple[str, str]]:
    """Build the list of ``(key, value)`` pairs the gateway pushes after
    each Accepted boot. Values are read through the runtime-override
    layer so an operator's edit in the Console takes effect on the next
    boot without a gateway restart.

    Scope: only **type-agnostic** keys. The four measurand-list keys
    (`MeterValuesAlignedData`, `MeterValuesSampledData`,
    `StopTxnAlignedData`, `StopTxnSampledData`) are deliberately
    excluded — they differ between AC and DC sites and
    BootNotification doesn't carry a reliable AC/DC signal. Operators
    that need to set them per charger should use the existing
    `POST /api/v1/charge-points/{cp_id}/commands/change-configuration`.
    """
    from eveys_ocpp.runtime_overrides import get_override

    settings = cp.settings

    # Read each value with override fallthrough. ints stringified into
    # the OCPP wire form here.
    def _i(field: str, default: int) -> str:
        return str(get_override(field, default))

    # OCPP configuration values are always strings on the wire; bools
    # are lowercased per the ISO 15118 / OCA convention chargers
    # actually accept.
    def _b(field: str, default: bool) -> str:
        return "true" if bool(get_override(field, default)) else "false"

    return [
        (
            "MeterValueSampleInterval",
            _i("meter_value_sample_interval_seconds", settings.meter_value_sample_interval_seconds),
        ),
        (
            "HeartbeatInterval",
            _i(
                "ocpp_cfg_heartbeat_interval_seconds",
                settings.ocpp_cfg_heartbeat_interval_seconds,
            ),
        ),
        (
            "ConnectionTimeOut",
            _i(
                "ocpp_cfg_connection_time_out_seconds",
                settings.ocpp_cfg_connection_time_out_seconds,
            ),
        ),
        (
            "TransactionMessageAttempts",
            _i(
                "ocpp_cfg_transaction_message_attempts",
                settings.ocpp_cfg_transaction_message_attempts,
            ),
        ),
        (
            "TransactionMessageRetryInterval",
            _i(
                "ocpp_cfg_transaction_message_retry_interval_seconds",
                settings.ocpp_cfg_transaction_message_retry_interval_seconds,
            ),
        ),
        (
            "WebSocketPingInterval",
            _i(
                "ocpp_cfg_websocket_ping_interval_seconds",
                settings.ocpp_cfg_websocket_ping_interval_seconds,
            ),
        ),
        (
            "ISO15118PnCEnabled",
            _b("ocpp_cfg_iso15118_pnc_enabled", settings.ocpp_cfg_iso15118_pnc_enabled),
        ),
        (
            "PlugandChargeMode",
            _i("ocpp_cfg_plug_and_charge_mode", settings.ocpp_cfg_plug_and_charge_mode),
        ),
        (
            "ContractValidationOffline",
            _b(
                "ocpp_cfg_contract_validation_offline",
                settings.ocpp_cfg_contract_validation_offline,
            ),
        ),
    ]


def _schedule_post_boot_configuration_push(cp: EveysChargePoint) -> None:
    """Spawn the deferred ChangeConfiguration task.

    Pinned on `cp` so the asyncio loop doesn't garbage-collect it
    before it runs (the "lost task" footgun from PEP 3156-style
    fire-and-forget code).
    """
    task = asyncio.create_task(
        _push_post_boot_configuration(cp),
        name=f"boot_notification.push_post_boot_configuration.{cp.id}",
    )
    # Hold a reference via the cp object. The library doesn't expose
    # a hook for this so we tack a private attribute on; reassigning
    # on subsequent boots is fine — the prior task either ran to
    # completion (no leak) or is still pending (it'll keep running
    # because asyncio holds its own ref via the loop).
    cp._meter_value_push_task = task


async def _push_post_boot_configuration(cp: EveysChargePoint) -> None:
    """Wait briefly, then push every configured key with
    ChangeConfiguration. Failure of any single CALL is logged and the
    loop moves on — the operator can fix the value via the
    `/api/v1/admin/config` PATCH and the next boot retries.

    Failure modes (all logged at WARN, none re-raised):
    - Charger has disconnected by the time we wake up → first CALL
      raises; subsequent keys are skipped because the WS is gone.
    - Charger rejects a key (read-only, unknown on older firmwares,
      not in its vendor feature profile) → response.status is
      `Rejected` / `NotSupported`; we log and continue to the next
      key. Operators can patch per-charger via the existing
      `POST /api/v1/charge-points/{cp_id}/commands/change-configuration`.
    """
    try:
        await asyncio.sleep(_POST_BOOT_PUSH_DELAY_SECONDS)
    except asyncio.CancelledError:  # pragma: no cover — pod shutdown
        raise

    pairs = _post_boot_keys(cp)
    for idx, (key, value) in enumerate(pairs):
        try:
            request = call.ChangeConfiguration(key=key, value=value)
            response = await cp.call(request)
            response_status = getattr(response, "status", None)
            log.info(
                "boot_notification.change_configuration.pushed",
                cp_id=cp.id,
                key=key,
                value=value,
                response_status=str(response_status) if response_status is not None else None,
            )
        except Exception as exc:
            # Best-effort per-key — a single rejected key must not
            # abort the rest. The WS path is unaffected; an operator
            # diagnosing rejections reads the per-key log line.
            log.warning(
                "boot_notification.change_configuration.failed",
                cp_id=cp.id,
                key=key,
                value=value,
                error=str(exc),
            )
        # Pace the burst so firmwares with serial CALL processing don't
        # queue-drop the tail. Skip the gap after the last key — no
        # point delaying the task's exit.
        if idx < len(pairs) - 1:
            await asyncio.sleep(_POST_BOOT_INTER_CALL_GAP_SECONDS)
