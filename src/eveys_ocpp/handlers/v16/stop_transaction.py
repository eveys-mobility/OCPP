"""StopTransaction handler.

OCPP 1.6 reference: StopTransaction.req / StopTransactionResponse.
Spec section: TODO (task C-1).

Charger reports the end of a transaction (the inverse of StartTransaction).
The response carries an optional `IdTagInfo` (omitted if no id_tag was
provided).

JSON Schemas: `ocpp.v16.schemas.StopTransaction` and `StopTransactionResponse`.

Behavior:
1. **Replay gate (E2-11, AGENTS rule 3).** Consult the Redis-backed
   idempotency cache keyed by ``(cp_id, message_id)``. Cache hit →
   skip everything (DB, backend, response shape preserved).
2. Mark the transaction stopped at the DB layer (also idempotent —
   defense in depth via the ``idempotency_key`` natural key).
3. **Backend round-trip (E3-6).** If a `backend_client` is wired AND
   the DB write was a real first-time stop (`applied=True`), call
   `POST /api/eveys/sessions/close`. The backend's `id_tag_info.status`
   flows through to the OCPP response. Replays (cache hit OR DB
   `applied=False`) skip the backend — chargers retry aggressively and
   we never want to double-bill.
4. Reply with the resolved `IdTagInfo.status`.

Why two layers of dedup:
- Redis (E2-11) is fast and catches the common case (charger retries
  within seconds). Saves the DB round-trip + backend call.
- Postgres ``idempotency_key`` (legacy) survives Redis flushes and
  cross-pod retries that arrive after the cache TTL expires.

Backend failure policy (matches StartTransaction in E3-5):
- `BackendUnavailableError` → keep the local DB row (audit-of-record),
  reply `Accepted`. Reconciler heals when the backend recovers.
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

from datetime import datetime
from typing import TYPE_CHECKING

from ocpp.v16 import call_result
from ocpp.v16.datatypes import IdTagInfo
from ocpp.v16.enums import AuthorizationStatus

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
    # Redis replay gate (E2-11). Cache hit → return the canonical
    # response without DB work, AND skip the backend call (the first
    # request already settled the session backend-side). Falls through
    # to the DB path if Redis is misbehaving — the DB-layer dedup below
    # still catches replays.
    if cp.idempotency is not None and message_id:
        try:
            replay = await cp.idempotency.check_and_record(cp_id=cp.id, message_id=message_id)
        except Exception as exc:
            log.warning("stop_transaction.idempotency_failed", error=str(exc))
            replay = False
        if replay:
            log.info("stop_transaction.replay_ignored_cache", message_id=message_id)
            metrics_registry.STOP_TRANSACTION_REPLAYS_TOTAL.inc()
            if callable(set_outcome):
                set_outcome("replay")
            return _response(_local_status(id_tag))

    reported_at = datetime.fromisoformat(timestamp)

    # DB-layer dedup. Survives a Redis flush or a TTL-expired retry.
    # Key shape pre-dates E2-11; the Redis layer above catches the
    # hot path so this rarely fires now.
    idem_key = f"{cp.id}:{transaction_id}:{meter_stop}"

    async with session_scope(cp.session_factory) as session:
        applied = await stop_transaction(
            session,
            transaction_id=transaction_id,
            meter_stop_wh=meter_stop,
            stopped_reported_at=reported_at,
            reason=reason,
            idempotency_key=idem_key,
        )

    if not applied:
        # DB-layer dedup said "we've already stopped this transaction."
        # Don't double-bill the backend.
        log.info("stop_transaction.replay_ignored_db")
        metrics_registry.STOP_TRANSACTION_REPLAYS_TOTAL.inc()
        if callable(set_outcome):
            set_outcome("replay")
        return _response(_local_status(id_tag))

    # Persisted — SLO 4 numerator. (Received counter at the top is
    # the denominator; the difference is what SLO 4 alerts on.)
    metrics_registry.STOP_TRANSACTIONS_TOTAL.labels(reason=reason or "unknown").inc()
    log.info("stop_transaction.applied", reason=reason, meter_stop=meter_stop)

    # Backend round-trip (E3-6). The DB row is now durably marked
    # stopped; whatever the backend says, we keep that state. On
    # `BackendUnavailableError` we reply Accepted so the charger
    # finalises cleanly — the reconciler heals the backend later.
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
