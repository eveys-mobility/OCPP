"""StartTransaction handler.

OCPP 1.6 reference: StartTransaction.req / StartTransactionResponse.
Spec section: TODO (task C-1).

Charger reports the start of an energy-delivery session. The CSMS
assigns the `transactionId` and returns it along with an `IdTagInfo`
decision.

JSON Schemas: `ocpp.v16.schemas.StartTransaction` and `StartTransactionResponse`.

Behavior:
1. Reject `INVALID*` id_tags up front (mock — same policy as authorize.py).
2. Resolve the charger's surrogate PK. Charger MUST be known (have sent
   BootNotification at some point). If unknown, reply with `ConcurrentTx`
   rather than auto-creating — silent auto-create would mask BootNotification
   bugs.
3. Insert a transaction row; surrogate `id` doubles as `transaction_id`.
4. Call the backend's `POST /api/eveys/sessions/open` (E3-5) if a
   `backend_client` is wired. The backend's `id_tag_info.status` flows
   through to the OCPP response. On `BackendUnavailableError` we keep
   the local row and reply `Accepted` — the row is the audit-of-record
   and the reconciler heals when the backend recovers. On
   `BackendBusinessError` (4xx) we reply `Invalid` but keep the row
   for the same audit reason.
5. Publish a `tx.started` event to Kafka (E2-8). Best-effort — publish
   failure is logged and dropped, never raised, since the OCPP reply is
   already on its way back.
6. Reply with the resolved status + assigned `transaction_id`.

The publish-after-commit ordering is intentional: only events for
durably-recorded transactions are emitted. We don't emit on the
INVALID id_tag or unknown-charger branches.

Deviations from the OCA spec to verify before W2 / OCTT
(see `docs/08-ocpp-conformance.md`):
- "Unknown charger → ConcurrentTx" is our policy choice. The spec doesn't
  prescribe a status for this case (the spec assumes the charger is known
  by the time it sends StartTransaction). The chosen status surfaces the
  problem to the charger logs without our needing a new status code.
- No `reservationId` honoring yet.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ocpp.v16 import call_result
from ocpp.v16.datatypes import IdTagInfo
from ocpp.v16.enums import AuthorizationStatus

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.metrics import record_handler_error, time_handler
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import get_charge_point_pk, insert_transaction
from eveys_ocpp.platform import BackendBusinessError, BackendUnavailableError

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


# OCPP wire string → mobilityhouse/ocpp enum. Same shape as authorize's
# map; an unrecognised backend status maps to Invalid (forward-compat —
# the safe default for an unknown shape on the financial path).
_AUTH_STATUS_MAP: dict[str, AuthorizationStatus] = {
    "Accepted": AuthorizationStatus.accepted,
    "Blocked": AuthorizationStatus.blocked,
    "Expired": AuthorizationStatus.expired,
    "Invalid": AuthorizationStatus.invalid,
    "ConcurrentTx": AuthorizationStatus.concurrent_tx,
}


def _build_envelope(*, cp_id: str, payload: events_pb2.TxStarted, occurred_at: datetime) -> bytes:
    envelope = events_pb2.EventEnvelope(
        event_id=str(uuid.uuid4()),
        occurred_at=occurred_at.isoformat(),
        cp_id=cp_id,
        schema_version="v1",
        tx_started=payload,
    )
    return envelope.SerializeToString()


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

    with time_handler("StartTransaction"):
        try:
            response = await _start_inner(
                cp,
                connector_id=connector_id,
                id_tag=id_tag,
                meter_start=meter_start,
                timestamp=timestamp,
            )
            # `id_tag_info` is an IdTagInfo dataclass; `.status` is the
            # AuthorizationStatus enum. The OCPP enum values are the
            # canonical wire strings (Accepted / Blocked / Invalid /
            # Expired / ConcurrentTx).
            decision = getattr(getattr(response.id_tag_info, "status", None), "value", "Unknown")
            metrics_registry.START_TRANSACTIONS_TOTAL.labels(decision=decision).inc()
            return response
        except Exception as exc:
            record_handler_error("StartTransaction", exc)
            raise


async def _start_inner(
    cp: EveysChargePoint,
    *,
    connector_id: int,
    id_tag: str,
    meter_start: int,
    timestamp: str,
) -> call_result.StartTransaction:
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

    # Backend round-trip (E3-5). The local row is already committed;
    # whatever the backend says, we keep that row as the audit-of-
    # record. On a backend outage we still reply Accepted so the
    # charger keeps charging — the reconciler heals later. On a
    # business rejection we forward Invalid to the charger (it stops)
    # but keep the row so the operations team has a trace of the
    # decision.
    backend_status = AuthorizationStatus.accepted
    if cp.backend_client is not None:
        try:
            result = await cp.backend_client.open_session(
                transaction_id=transaction_id,
                cp_id=cp.id,
                connector_id=connector_id,
                id_tag=id_tag,
                meter_start_wh=meter_start,
                started_reported_at=timestamp,
                idempotency_key=f"ocpp-session-open-{transaction_id}",
            )
            backend_status = _AUTH_STATUS_MAP.get(
                result.id_tag_info.status, AuthorizationStatus.invalid
            )
        except BackendUnavailableError as exc:
            log.warning(
                "start_transaction.backend_unavailable",
                transaction_id=transaction_id,
                error=str(exc),
                error_code=exc.error_code,
            )
        except BackendBusinessError as exc:
            log.warning(
                "start_transaction.backend_business_rejected",
                transaction_id=transaction_id,
                error_code=exc.error_code,
                message=str(exc),
            )
            backend_status = AuthorizationStatus.invalid

    log.info(
        "start_transaction.decided",
        transaction_id=transaction_id,
        connector_id=connector_id,
        id_tag=id_tag,
        decision=backend_status.value,
    )

    if cp.event_producer is not None:
        payload = events_pb2.TxStarted(
            transaction_id=transaction_id,
            connector_id=connector_id,
            id_tag=id_tag,
            meter_start_wh=meter_start,
            charger_reported_at=timestamp,
        )
        envelope_bytes = _build_envelope(
            cp_id=cp.id, payload=payload, occurred_at=datetime.now(UTC)
        )
        # Best-effort publish — broker drop must not crash the OCPP
        # handler. Same rationale as meter_values + boot_notification.
        try:
            await cp.event_producer.publish(
                topic=cp.settings.kafka_topic_tx_started,
                key=cp.id,
                value=envelope_bytes,
            )
        except Exception as exc:
            log.warning(
                "start_transaction.publish_failed",
                transaction_id=transaction_id,
                error=str(exc),
            )

    return call_result.StartTransaction(
        transaction_id=transaction_id,
        id_tag_info=IdTagInfo(status=backend_status),
    )
