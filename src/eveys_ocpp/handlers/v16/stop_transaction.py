"""StopTransaction handler.

OCPP 1.6 reference: StopTransaction.req / StopTransactionResponse.
Spec section: TODO (task C-1).

Charger reports the end of a transaction (the inverse of StartTransaction).
The response carries an optional `IdTagInfo` (omitted if no id_tag was
provided).

JSON Schemas: `ocpp.v16.schemas.StopTransaction` and `StopTransactionResponse`.

Behavior:
1. Mark the transaction stopped in Postgres, matching the owning station
   and physical ID. PG first-apply/dedup is authoritative.
2. Record an optional Redis hint only after the Stop commit. A cached
   marker cannot bypass PG: it may predate a failed write in older code.
3. **Backend round-trip (E3-6).** If a `backend_client` is wired AND
   the DB write was a real first-time stop (`applied=True`), call
   `POST /api/eveys/sessions/close`. The backend's `id_tag_info.status`
   flows through to the OCPP response. Non-applies skip the backend;
   chargers may retry an already committed Stop.
4. Reply with the resolved `IdTagInfo.status`.

PG dedup survives Redis outages, pod changes, and failed/cancelled commits.
The Stop records the charger-reported time and server receive time separately;
an existing server-rejection timestamp is preserved.

Backend failure policy for an already reported physical Stop:
- `BackendUnavailableError` → keep the local DB row (audit-of-record),
  reply `Accepted`. Post-commit publish/backend failure is not yet a
  durable outbox; recovery is not guaranteed by this handler.
- `BackendBusinessError` (e.g. billing-fraud `Blocked`) → keep the
  row, reply with whatever the backend dictated (typically `Blocked`).
  Forwards the operational decision to the charger.

Deviations from the OCA spec to verify before W2 / OCTT
(see `docs/08-ocpp-conformance.md`):
- `transactionData` (an array of MeterValues) is accepted by the
  handler but NOT forwarded to the backend in this version — the
  `cp.meter` Kafka topic is the canonical home for those samples.
- We don't validate that the inbound `transaction_id` actually exists —
  the repository's UPDATE simply matches zero rows and returns
  `applied=False`, which suppresses the backend call.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ocpp.exceptions import SecurityError
from ocpp.v16 import call_result
from ocpp.v16.datatypes import IdTagInfo
from ocpp.v16.enums import AuthorizationStatus

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.metrics import record_handler_error, time_handler
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import stop_transaction
from eveys_ocpp.platform import BackendBusinessError, BackendUnavailableError

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


# OCPP wire string → mobilityhouse/ocpp enum. Same shape as Authorize
# / StartTransaction maps; an unrecognised backend status maps to
# Invalid (forward-compat — the safe default for an unknown shape on
# the financial path).
_AUTH_STATUS_MAP: dict[str, AuthorizationStatus] = {
    "Accepted": AuthorizationStatus.accepted,
    "Blocked": AuthorizationStatus.blocked,
    "Expired": AuthorizationStatus.expired,
    "Invalid": AuthorizationStatus.invalid,
    "ConcurrentTx": AuthorizationStatus.concurrent_tx,
}


def _build_envelope(
    *,
    cp_id: str,
    payload: events_pb2.TxStopped,
    occurred_at: datetime,
) -> bytes:
    envelope = events_pb2.EventEnvelope(
        event_id=str(uuid.uuid4()),
        occurred_at=occurred_at.isoformat(),
        cp_id=cp_id,
        schema_version="v1",
        tx_stopped=payload,
    )
    return envelope.SerializeToString()


def _response(status: AuthorizationStatus) -> call_result.StopTransaction:
    return call_result.StopTransaction(id_tag_info=IdTagInfo(status=status))


def _local_status(id_tag: str | None) -> AuthorizationStatus:
    """Pre-backend default: the inbound id_tag's local-only verdict.
    Used for replays (we already accepted) and when no backend is
    wired (W1/dev). The `INVALID*` prefix mirrors the mock policy in
    `authorize.py` so unit tests stay coherent across handlers."""
    if id_tag and id_tag.upper().startswith("INVALID"):
        return AuthorizationStatus.invalid
    return AuthorizationStatus.accepted


async def handle(
    cp: EveysChargePoint,
    *,
    message_id: str | None = None,
    transaction_id: int,
    meter_stop: int,
    timestamp: str,
    reason: str | None = None,
    id_tag: str | None = None,
    **_: object,  # transaction_data (MeterValues subset) — ClickHouse-bound, not Postgres
) -> call_result.StopTransaction:
    bind_contextvars(
        cp_id=cp.id,
        action="StopTransaction",
        direction="rx",
        transaction_id=transaction_id,
    )

    # Pending-authorization gate — only BootNotification is honoured
    # while the operator hasn't approved this cp_id.
    if cp.is_pending:
        raise SecurityError(
            details={"reason": "authorization pending; operator has not authorized this cp_id"}
        )

    # Counted regardless of what happens next — SLO 4 (transaction
    # durability) needs the denominator to include stops that fail
    # to persist (those are precisely what the SLO flags). The
    # `_total` (persisted) counter is bumped after the DB commit
    # below so the SLO ratio is meaningful.
    metrics_registry.STOP_TRANSACTIONS_RECEIVED_TOTAL.inc()
    with time_handler("StopTransaction") as set_outcome:
        try:
            return await _stop_inner(
                cp,
                message_id=message_id,
                transaction_id=transaction_id,
                meter_stop=meter_stop,
                timestamp=timestamp,
                reason=reason,
                id_tag=id_tag,
                set_outcome=set_outcome,
            )
        except Exception as exc:
            record_handler_error("StopTransaction", exc)
            raise


async def _stop_inner(
    cp: EveysChargePoint,
    *,
    message_id: str | None,
    transaction_id: int,
    meter_stop: int,
    timestamp: str,
    reason: str | None,
    id_tag: str | None,
    set_outcome: object,
) -> call_result.StopTransaction:
    # Postgres owns durability/dedup. A Redis marker written before commit
    # could suppress the only retry after a failed or cancelled DB write.
    reported_at = datetime.fromisoformat(timestamp)
    received_at = datetime.now(UTC)

    # DB-layer dedup survives Redis failures and retries after rollback.
    idem_key = f"{cp.id}:{transaction_id}:{meter_stop}"

    async with session_scope(cp.session_factory) as session:
        meter_start_wh = await stop_transaction(
            session,
            cp_id=cp.id,
            transaction_id=transaction_id,
            meter_stop_wh=meter_stop,
            stopped_reported_at=reported_at,
            stopped_received_at=received_at,
            reason=reason,
            idempotency_key=idem_key,
        )

    if meter_start_wh is None:
        # Already applied, unknown, or owned by another station. None of
        # these outcomes permits a new event or backend close request.
        log.info("stop_transaction.not_applied")
        if callable(set_outcome):
            set_outcome("not_applied")
        return _response(_local_status(id_tag))

    # This cache is only a post-commit hint; it never substitutes for the
    # owner-scoped PG write. Failed hints must not affect the charger ACK.
    if cp.idempotency is not None and message_id:
        try:
            await cp.idempotency.check_and_record(cp_id=cp.id, message_id=message_id)
        except Exception as exc:
            log.warning("stop_transaction.idempotency_failed", error_class=type(exc).__name__)

    # Persisted — SLO 4 numerator. (Received counter at the top is
    # the denominator; the difference is what SLO 4 alerts on.)
    metrics_registry.STOP_TRANSACTIONS_TOTAL.labels(reason=reason or "unknown").inc()
    log.info("stop_transaction.applied", reason=reason, meter_stop=meter_stop)

    # Best-effort post-commit event: PG replay dedup cannot recover a
    # failed publish or a crash in this gap. The current backend closes
    # financial state through this event; /sessions/close is a no-op
    # acknowledgement, so it is not a durable fallback. Only real applies
    # publish. A durable ledger/outbox is still required for recovery.
    if cp.event_producer is not None:
        payload = events_pb2.TxStopped(
            transaction_id=transaction_id,
            id_tag=id_tag or "",
            meter_stop_wh=meter_stop,
            consumed_wh=meter_stop - meter_start_wh,
            stop_reason=reason or "",
            charger_reported_at=timestamp,
        )
        envelope_bytes = _build_envelope(
            cp_id=cp.id, payload=payload, occurred_at=datetime.now(UTC)
        )
        try:
            await cp.event_producer.publish(
                topic=cp.settings.kafka_topic_tx_stopped,
                key=cp.id,
                value=envelope_bytes,
            )
        except Exception as exc:
            log.warning(
                "stop_transaction.publish_failed",
                transaction_id=transaction_id,
                error=str(exc),
            )

    # Backend round-trip (E3-6). The DB row is now durably marked
    # stopped; whatever the backend says, we keep that state. On
    # `BackendUnavailableError` we reply Accepted so the charger
    # finalises cleanly. This call alone does not guarantee reconciliation.
    # On `BackendBusinessError` we forward the backend's verdict to
    # the charger (Blocked / Invalid / etc.) so billing-fraud cases
    # surface immediately.
    final_status = _local_status(id_tag)
    if cp.backend_client is not None:
        try:
            result = await cp.backend_client.close_session(
                transaction_id=transaction_id,
                cp_id=cp.id,
                # OCPP allows omitting id_tag (e.g. EVDisconnected stop reason).
                # Contract requires a string; pass empty so the backend can
                # decide what to do (typically: settle by transaction_id alone).
                id_tag=id_tag or "",
                meter_stop_wh=meter_stop,
                stopped_reported_at=timestamp,
                stop_reason=reason,
                idempotency_key=(
                    f"ocpp-session-close-{transaction_id}-{message_id or 'no-msg-id'}"
                ),
            )
            final_status = _AUTH_STATUS_MAP.get(
                result.id_tag_info.status, AuthorizationStatus.invalid
            )
        except BackendUnavailableError as exc:
            log.warning(
                "stop_transaction.backend_unavailable",
                transaction_id=transaction_id,
                error=str(exc),
                error_code=exc.error_code,
            )
        except BackendBusinessError as exc:
            log.warning(
                "stop_transaction.backend_business_rejected",
                transaction_id=transaction_id,
                error_code=exc.error_code,
                message=str(exc),
            )
            # Forward the backend's verdict; default to Invalid if the
            # business error doesn't carry an actionable status.
            final_status = AuthorizationStatus.invalid

    log.info(
        "stop_transaction.decided",
        transaction_id=transaction_id,
        decision=final_status.value,
    )
    return _response(final_status)
