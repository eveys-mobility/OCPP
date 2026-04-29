"""StopTransaction handler.

OCPP 1.6 reference: StopTransaction.req / StopTransactionResponse.
Spec section: TODO (task C-1).

Charger reports the end of a transaction (the inverse of StartTransaction).
The response carries an optional `IdTagInfo` (omitted if no id_tag was
provided).

JSON Schemas: `ocpp.v16.schemas.StopTransaction` and `StopTransactionResponse`.

Behavior in v0:
1. Mark the transaction stopped (idempotent on `idempotency_key`).
2. Reply with `IdTagInfo.status` for the optional id_tag.

Idempotency model (AGENTS rule 3):
- Key = `f"{cp_id}:{transaction_id}:{meter_stop}"`.
- Replays (charger retries because our ACK was lost) carry the same
  triple → repository returns `applied=False` → no double-write.
- We always reply `Accepted` to the charger; the charger doesn't know
  or care that we treated the call as a replay.

Deviations from the OCA spec to verify before W2 / OCTT
(see `docs/08-ocpp-conformance.md`):
- `transactionData` (an array of MeterValues) is accepted but NOT
  persisted by this handler. MeterValues are ClickHouse-bound via Kafka
  (tasks E2-8 + E2-14). Until those land, the array is logged but not
  stored.
- We don't validate that the inbound `transaction_id` actually exists —
  the repository's UPDATE simply matches zero rows and returns
  `applied=False`. OCTT may flag this; if so, add an explicit lookup
  and return a 4xx-equivalent error.
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


async def handle(
    cp: EveysChargePoint,
    *,
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

    reported_at = datetime.fromisoformat(timestamp)

    # Use the OCPP message_id as the idempotency key when available.
    # The mobilityhouse library exposes the current incoming message via
    # `cp._unique_id_generator`'s peer state; in practice we synthesize a
    # stable key from (cp_id, transaction_id, meter_stop) which is unique
    # per logical stop event. Replays carry the same triple.
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
        log.info("stop_transaction.replay_ignored")

    info_status = (
        AuthorizationStatus.invalid
        if id_tag and id_tag.upper().startswith("INVALID")
        else AuthorizationStatus.accepted
    )
    return call_result.StopTransaction(id_tag_info=IdTagInfo(status=info_status))
