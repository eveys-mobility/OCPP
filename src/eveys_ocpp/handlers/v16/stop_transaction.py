"""StopTransaction handler.

OCPP 1.6 reference: StopTransaction.req / StopTransactionResponse.
Spec section: TODO (task C-1).

Charger reports the end of a transaction (the inverse of StartTransaction).
The response carries an optional `IdTagInfo` (omitted if no id_tag was
provided).

JSON Schemas: `ocpp.v16.schemas.StopTransaction` and `StopTransactionResponse`.

Behavior in v0:
1. **Replay gate (E2-11, AGENTS rule 3).** Consult the Redis-backed
   idempotency cache keyed by ``(cp_id, message_id)``. Cache hit →
   skip the DB write, return the canonical Accepted response.
2. Mark the transaction stopped at the DB layer (also idempotent —
   defense in depth via the ``idempotency_key`` natural key).
3. Reply with `IdTagInfo.status` for the optional id_tag.

Why two layers of dedup:
- Redis (E2-11) is fast and catches the common case (charger retries
  within seconds). Saves the DB round-trip.
- Postgres ``idempotency_key`` (legacy) survives Redis flushes and
  cross-pod retries that arrive after the cache TTL expires.

Deviations from the OCA spec to verify before W2 / OCTT
(see `docs/08-ocpp-conformance.md`):
- `transactionData` (an array of MeterValues) is accepted but NOT
  persisted by this handler. MeterValues are ClickHouse-bound via Kafka
  (tasks E2-8 + E2-14).
- We don't validate that the inbound `transaction_id` actually exists —
  the repository's UPDATE simply matches zero rows and returns
  `applied=False`.
- Real `session-service.CloseSession` integration lands in E3-6.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from ocpp.v16 import call_result
from ocpp.v16.datatypes import IdTagInfo
from ocpp.v16.enums import AuthorizationStatus

from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import stop_transaction

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


def _response(id_tag: str | None) -> call_result.StopTransaction:
    info_status = (
        AuthorizationStatus.invalid
        if id_tag and id_tag.upper().startswith("INVALID")
        else AuthorizationStatus.accepted
    )
    return call_result.StopTransaction(id_tag_info=IdTagInfo(status=info_status))


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

    # Redis replay gate (E2-11). Cache hit → return the canonical
    # response without DB work. Falls through to the DB path if Redis
    # is misbehaving — the DB-layer dedup below still catches replays.
    if cp.idempotency is not None and message_id:
        try:
            replay = await cp.idempotency.check_and_record(cp_id=cp.id, message_id=message_id)
        except Exception as exc:
            log.warning("stop_transaction.idempotency_failed", error=str(exc))
            replay = False
        if replay:
            log.info("stop_transaction.replay_ignored_cache", message_id=message_id)
            return _response(id_tag)

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

    if applied:
        log.info("stop_transaction.applied", reason=reason, meter_stop=meter_stop)
    else:
        log.info("stop_transaction.replay_ignored_db")

    return _response(id_tag)
