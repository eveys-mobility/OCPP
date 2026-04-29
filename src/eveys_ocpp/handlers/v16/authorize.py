"""Authorize handler.

OCPP 1.6 reference: Authorize.req / AuthorizeResponse.
Spec section: TODO (task C-1).

The charger asks the CSMS whether an `idTag` (RFID/NFC token) is
permitted. The response carries an `IdTagInfo` with one of: Accepted,
Blocked, Expired, Invalid, ConcurrentTx.

JSON Schemas: `ocpp.v16.schemas.Authorize` and `AuthorizeResponse`.

Behavior in v0 (mock policy):
- Tags whose UPPER-CASED form starts with `INVALID` → `Invalid`.
- Everything else → `Accepted`.

This is a deliberate stub — the real call to `auth-service.CheckAuthorization`
lands in task E3-3 (Phase 3, W4). The mock keeps the protocol layer
testable end-to-end without a wallet/auth dependency.

Deviations from the OCA spec to verify before W2 / OCTT
(see `docs/08-ocpp-conformance.md`):
- We never return `Blocked`, `Expired`, or `ConcurrentTx` from this
  handler. Real auth-service integration must produce these where
  appropriate.
- We don't populate `IdTagInfo.expiry_date` or `parent_id_tag`.
- No caching yet (E3-4 adds Redis caching with 30s TTL).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ocpp.v16 import call_result
from ocpp.v16.datatypes import IdTagInfo
from ocpp.v16.enums import AuthorizationStatus

from eveys_ocpp.observability import bind_contextvars, get_logger

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


async def handle(cp: EveysChargePoint, *, id_tag: str, **_: object) -> call_result.Authorize:
    bind_contextvars(cp_id=cp.id, action="Authorize", direction="rx")

    # Mock policy: accept everything except a hard-coded INVALID prefix.
    # Real policy lives behind auth-service in E3-3.
    if id_tag.upper().startswith("INVALID"):
        status = AuthorizationStatus.invalid
    else:
        status = AuthorizationStatus.accepted

    log.info("authorize.decided", id_tag=id_tag, decision=status.value)

    return call_result.Authorize(id_tag_info=IdTagInfo(status=status))
