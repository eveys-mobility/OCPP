"""Heartbeat handler.

OCPP 1.6 reference: Heartbeat.req / HeartbeatResponse.
Spec section: TODO (task C-1).

Charger pings every `interval` seconds (configured in BootNotificationResponse).
The response carries the CSMS's current UTC time, which the charger uses
to discipline its own clock.

JSON Schemas: `ocpp.v16.schemas.Heartbeat` and `HeartbeatResponse`.

Behavior in v0:
1. Refresh `last_heartbeat_at` for the charger row.
2. Return server-current UTC time. Per AGENTS rule 7 the charger's clock
   is untrusted; the Heartbeat response is the canonical clock signal.

Deviations from the OCA spec to verify before W2 / OCTT
(see `docs/08-ocpp-conformance.md`):
- Redis online-registry TTL refresh is NOT done here — that lands with
  task E2-9.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ocpp.v16 import call_result

from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import update_heartbeat

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


async def handle(cp: EveysChargePoint) -> call_result.Heartbeat:
    bind_contextvars(cp_id=cp.id, action="Heartbeat", direction="rx")

    now = datetime.now(UTC)
    async with session_scope(cp.session_factory) as session:
        await update_heartbeat(session, cp_id=cp.id, at=now)

    log.debug("heartbeat.tick")

    return call_result.Heartbeat(current_time=now.isoformat())
