"""Authorize handler.

OCPP 1.6 reference: Authorize.req / AuthorizeResponse.
Spec section: TODO (task C-1).

The charger asks the CSMS whether an `idTag` (RFID/NFC token) is
permitted. The response carries an `IdTagInfo` with one of: Accepted,
Blocked, Expired, Invalid, ConcurrentTx.

JSON Schemas: `ocpp.v16.schemas.Authorize` and `AuthorizeResponse`.

Behaviour
---------

- If a backend client is wired (production), the handler calls
  `POST /api/eveys/authorize` and forwards the resulting
  `IdTagInfo.status` to the charger verbatim. Per ADR-0023 the
  backend is the source of truth for authorization; the gateway
  doesn't second-guess it.
- If the backend client is not wired (W1 dev stack, unit tests
  without ``backend_base_url``), the handler returns ``Accepted``
  for any id_tag — useful for end-to-end protocol testing without
  a backend dependency. The gating in `__main__.py` makes this
  explicit (logs ``backend_client.disabled``).

Fallback policy when the backend is unreachable past the retry
budget (ADR-0023 §"Fallback policy"):

- ``settings.backend_authorize_fallback="reject"`` (default): return
  ``Invalid`` to the charger. Loud, safe — a charger that can't
  authorize won't start a session, which is the right behaviour
  for a billing-relevant gate.
- ``settings.backend_authorize_fallback="accept_offline"``: return
  ``Accepted`` with a 5-minute ``expiry_date``. Use only when the
  operator accepts the risk of un-billable sessions.

Idempotency-Key shape: ``ocpp-auth-{cp_id}-{id_tag}-{message_id}``.
The OCPP message-id is forwarded by the library as ``call_unique_id``.
This ensures a charger replay (same message_id) hits the backend's
idempotency cache instead of double-consulting.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from ocpp.v16 import call_result
from ocpp.v16.datatypes import IdTagInfo
from ocpp.v16.enums import AuthorizationStatus

from eveys_ocpp.observability import bind_contextvars, get_logger
from eveys_ocpp.platform import (
    AuthorizeResult,
    BackendBusinessError,
    BackendUnavailableError,
)

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


# OCPP wire string → mobilityhouse/ocpp enum value. Any unrecognised
# string from the backend (forward-compat) maps to Invalid — safer
# default than Accepted for an unknown shape.
_STATUS_MAP: dict[str, AuthorizationStatus] = {
    "Accepted": AuthorizationStatus.accepted,
    "Blocked": AuthorizationStatus.blocked,
    "Expired": AuthorizationStatus.expired,
    "Invalid": AuthorizationStatus.invalid,
    "ConcurrentTx": AuthorizationStatus.concurrent_tx,
}

# Length of the offline-Accepted window when the operator opts for
# the `accept_offline` fallback. Five minutes balances "give the
# user a chance to charge during a brief backend hiccup" against
# "don't issue a too-long offline grant if the backend's outage
# turns into hours."
_OFFLINE_ACCEPT_WINDOW_SECONDS = 300


async def handle(
    cp: EveysChargePoint,
    *,
    id_tag: str,
    message_id: str | None = None,
    **_: object,
) -> call_result.Authorize:
    bind_contextvars(cp_id=cp.id, action="Authorize", direction="rx")

    # No backend client — stub `Accepted` for dev / W1.
    if cp.backend_client is None:
        log.info("authorize.no_backend_client", id_tag=id_tag, decision="Accepted")
        return call_result.Authorize(id_tag_info=IdTagInfo(status=AuthorizationStatus.accepted))

    idempotency_key = f"ocpp-auth-{cp.id}-{id_tag}-{message_id or 'no-msg-id'}"

    try:
        result = await cp.backend_client.authorize(
            id_tag=id_tag,
            cp_id=cp.id,
            idempotency_key=idempotency_key,
        )
    except BackendUnavailableError as exc:
        return _fallback(cp, id_tag, exc)
    except BackendBusinessError as exc:
        # Backend understood the request and refused (e.g.
        # `UNKNOWN_ID_TAG`). Pass that through as Invalid — the
        # charger doesn't need the error_code, just the OCPP-level
        # outcome.
        log.warning(
            "authorize.business_rejected",
            id_tag=id_tag,
            error_code=exc.error_code,
            message=str(exc),
        )
        return call_result.Authorize(id_tag_info=IdTagInfo(status=AuthorizationStatus.invalid))

    return _result_to_response(result)


def _result_to_response(result: AuthorizeResult) -> call_result.Authorize:
    """Translate the typed `AuthorizeResult` to an OCPP `IdTagInfo`."""
    status = _STATUS_MAP.get(result.id_tag_info.status)
    if status is None:
        log.warning(
            "authorize.unknown_status_from_backend",
            id_tag=result.id_tag,
            backend_status=result.id_tag_info.status,
        )
        status = AuthorizationStatus.invalid

    log.info(
        "authorize.decided",
        id_tag=result.id_tag,
        decision=status.value,
        backend_request_id=result.request_id,
    )

    return call_result.Authorize(
        id_tag_info=IdTagInfo(
            status=status,
            parent_id_tag=result.id_tag_info.parent_id_tag,
            expiry_date=result.id_tag_info.expiry_date,
        )
    )


def _fallback(
    cp: EveysChargePoint, id_tag: str, exc: BackendUnavailableError
) -> call_result.Authorize:
    """Backend unreachable past the retry budget. Apply the configured
    fallback policy and log it loudly so ops dashboards can surface
    "backend authorize degraded" runs."""
    policy = cp.settings.backend_authorize_fallback
    log.warning(
        "authorize.backend_unavailable",
        id_tag=id_tag,
        policy=policy,
        error=str(exc),
        error_code=exc.error_code,
    )

    if policy == "accept_offline":
        expiry = datetime.now(UTC) + timedelta(seconds=_OFFLINE_ACCEPT_WINDOW_SECONDS)
        return call_result.Authorize(
            id_tag_info=IdTagInfo(
                status=AuthorizationStatus.accepted,
                expiry_date=expiry.isoformat(),
            )
        )

    # Default `reject` — return Invalid; charger refuses the session.
    return call_result.Authorize(id_tag_info=IdTagInfo(status=AuthorizationStatus.invalid))
