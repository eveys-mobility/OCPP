"""StartTransaction handler.

OCPP 1.6 reference: StartTransaction.req / StartTransactionResponse.
Spec section: TODO (task C-1).

Charger reports the start of an energy-delivery session. The CSMS
assigns the `transactionId` and returns it along with an `IdTagInfo`
decision.

JSON Schemas: `ocpp.v16.schemas.StartTransaction` and `StartTransactionResponse`.

Behavior in v0:
1. Reject `INVALID*` id_tags up front (mock — same policy as authorize.py).
2. Resolve the charger's surrogate PK. Charger MUST be known (have sent
   BootNotification at some point). If unknown, reply with `ConcurrentTx`
   rather than auto-creating — silent auto-create would mask BootNotification
   bugs.
3. Insert a transaction row; surrogate `id` doubles as `transaction_id`.
4. Reply `Accepted`.

Deviations from the OCA spec to verify before W2 / OCTT
(see `docs/08-ocpp-conformance.md`):
- "Unknown charger → ConcurrentTx" is our policy choice. The spec doesn't
  prescribe a status for this case (the spec assumes the charger is known
  by the time it sends StartTransaction). The chosen status surfaces the
  problem to the charger logs without our needing a new status code.
- No `reservationId` honoring yet.
- Real `session-service.OpenSession` integration lands in E3-5.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from ocpp.v16 import call_result
from ocpp.v16.datatypes import IdTagInfo
from ocpp.v16.enums import AuthorizationStatus

from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import get_charge_point_pk, insert_transaction

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


async def handle(
    cp: EveysChargePoint,
    *,
    connector_id: int,
    id_tag: str,
    meter_start: int,
    timestamp: str,
    **_: object,
) -> call_result.StartTransaction:
    bind_contextvars(cp_id=cp.id, action="StartTransaction", direction="rx")

    if id_tag.upper().startswith("INVALID"):
        return call_result.StartTransaction(
            transaction_id=0,
            id_tag_info=IdTagInfo(status=AuthorizationStatus.invalid),
        )

    reported_at = datetime.fromisoformat(timestamp)

    async with session_scope(cp.session_factory) as session:
        cp_pk = await get_charge_point_pk(session, cp_id=cp.id)
        if cp_pk is None:
            log.warning("start_transaction.unknown_charger")
            return call_result.StartTransaction(
                transaction_id=0,
                id_tag_info=IdTagInfo(status=AuthorizationStatus.concurrent_tx),
            )

        transaction_id = await insert_transaction(
            session,
            charge_point_pk=cp_pk,
            connector_id=connector_id,
            id_tag=id_tag,
            meter_start_wh=meter_start,
            started_reported_at=reported_at,
        )

    log.info(
        "start_transaction.accepted",
        transaction_id=transaction_id,
        connector_id=connector_id,
        id_tag=id_tag,
    )

    return call_result.StartTransaction(
        transaction_id=transaction_id,
        id_tag_info=IdTagInfo(status=AuthorizationStatus.accepted),
    )
