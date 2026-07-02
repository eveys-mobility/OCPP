"""Heartbeat handler.

OCPP 1.6 reference: Heartbeat.req / HeartbeatResponse.
Spec section: TODO (task C-1).

Charger pings every `interval` seconds (configured in BootNotificationResponse).
The response carries the CSMS's current UTC time, which the charger uses
to discipline its own clock.

JSON Schemas: `ocpp.v16.schemas.Heartbeat` and `HeartbeatResponse`.

Behavior:
1. Refresh `last_heartbeat_at` for the charger row in Postgres.
2. Refresh the Redis online-registry TTL on `cp:online:{cp_id}`. If
   the key is gone (TTL expired since the last heartbeat — possible if
   the network was choppy), re-mark as online.
3. Return server-current UTC time. Per AGENTS rule 7 the charger's clock
   is untrusted; the Heartbeat response is the canonical clock signal.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ocpp.exceptions import SecurityError
from ocpp.v16 import call_result

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.metrics import record_handler_error, time_handler
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import update_heartbeat

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


def _build_envelope(*, cp_id: str, payload: events_pb2.CpHeartbeat, occurred_at: datetime) -> bytes:
    envelope = events_pb2.EventEnvelope(
        event_id=str(uuid.uuid4()),
        occurred_at=occurred_at.isoformat(),
        cp_id=cp_id,
        schema_version="v1",
        cp_heartbeat=payload,
    )
    return envelope.SerializeToString()


async def handle(cp: EveysChargePoint) -> call_result.Heartbeat:
    bind_contextvars(cp_id=cp.id, action="Heartbeat", direction="rx")

    # Pending-authorization gate — only BootNotification is honoured
    # while the operator hasn't approved this cp_id.
    if cp.is_pending:
        raise SecurityError(
            details={"reason": "authorization pending; operator has not authorized this cp_id"}
        )

    metrics_registry.HEARTBEATS_TOTAL.inc()
    with time_handler("Heartbeat"):
        try:
            now = datetime.now(UTC)
            async with session_scope(cp.session_factory) as session:
                await update_heartbeat(session, cp_id=cp.id, at=now)

            if cp.registry is not None:
                refreshed = await cp.registry.refresh(cp.id)
                if not refreshed:
                    # Key expired between heartbeats. Re-claim ownership.
                    await cp.registry.mark_online(cp.id)
                    metrics_registry.HEARTBEAT_REGISTRY_RECLAIMS_TOTAL.inc()
                    log.info("heartbeat.registry_reclaimed")

            # Best-effort publish so the backend can refresh
            # `last_online`. A broker drop must not block the Heartbeat
            # response — same pattern boot/meter_values use.
            if cp.event_producer is not None:
                envelope_bytes = _build_envelope(
                    cp_id=cp.id,
                    payload=events_pb2.CpHeartbeat(pod_id=cp.settings.pod_id),
                    occurred_at=now,
                )
                try:
                    await cp.event_producer.publish(
                        topic=cp.settings.kafka_topic_cp_heartbeat,
                        key=cp.id,
                        value=envelope_bytes,
                    )
                except Exception as exc:
                    log.warning("heartbeat.publish_failed", error=str(exc))

            log.debug("heartbeat.tick")
            return call_result.Heartbeat(current_time=now.isoformat())
        except Exception as exc:
            record_handler_error("Heartbeat", exc)
            raise
