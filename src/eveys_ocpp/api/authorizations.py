"""`/api/v1/authorizations` routes.

Operator surface for the Redis-backed pending queue:

    GET    /authorizations                           list currently-pending devices
    POST   /authorizations/{cp_id}/authorize         admit the device to the fleet
    POST   /authorizations/{cp_id}/reject            drop the pending row
    POST   /authorizations/{cp_id}/revoke            remove an already-authorized device

`authorize` copies the pending row's metadata into `charge_points` so
future WS upgrades follow the authorized branch of `_authorization.py`.
`reject` drops the pending row; the next Boot from the same cp_id
restarts the pending flow. `revoke` deletes the `charge_points` row so
the device is no longer authorized — same lockout effect as reject,
but for a device that was previously in the fleet.

All three operator actions also force-close any live WS this pod is
hosting for the cp_id, so the charger reconnects and takes the correct
branch immediately. Cross-pod revocation relies on the next reconnect
being rejected — the registry doesn't carry a "kick this cp_id"
channel today, and adding one is its own scope.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request

from eveys_ocpp.api._errors import (
    ERR_BAD_REQUEST,
    ERR_UNKNOWN_CP_ID,
    ApiError,
)
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import (
    delete_charge_point,
    upsert_charge_point_boot,
)

if TYPE_CHECKING:
    from eveys_ocpp.pending_authorizations import PendingAuthorizations

router = APIRouter(tags=["authorizations"])

log = get_logger(__name__)


# Bounded outcomes for the admin counter — kept in sync with what the
# routes emit. Deliberately distinct from `WS_AUTHORIZATION_TOTAL`,
# which tracks the WS-edge decisions (a machine event) rather than
# operator actions (a human event).
_ADMIN_OUTCOME_AUTHORIZED = "authorized"
_ADMIN_OUTCOME_REJECTED = "rejected"
_ADMIN_OUTCOME_REVOKED = "revoked"
_ADMIN_OUTCOME_NOT_FOUND = "not_found"


def _validate_cp_id(cp_id: str) -> None:
    """Bounds match the `charge_points.cp_id` column (64 chars). Reject
    malformed URLs early rather than let them ripple into Redis /
    Postgres."""
    if not cp_id or len(cp_id) > 64:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="cp_id must be 1..64 chars",
        )


def _pending_store(request: Request) -> PendingAuthorizations:
    """Fetch the process-wide pending store off `app.state`.

    Wired in `__main__.py` (production) and in the REST test fixtures.
    Missing means the app was constructed without the store — surface
    the mistake as a 500 instead of silently 404'ing every request."""
    store: PendingAuthorizations | None = getattr(request.app.state, "pending_store", None)
    if store is None:
        # Defensive: an app.state misconfiguration should not look like
        # an empty queue to the operator. Fail loud.
        raise ApiError(
            status_code=500,
            error_code="INTERNAL_ERROR",
            message="pending authorization store is not configured",
        )
    return store


async def _close_live_ws(request: Request, cp_id: str, reason: str) -> None:
    """Force-close any WS this pod holds for `cp_id`.

    Uses close code 1008 (Policy Violation) — the charger interprets it
    as "don't come back on this credential" and the reconnect attempt
    hits the WS auth gate fresh, which is what all three operator
    actions want. The underlying WebSocket lives at the OCPP base
    class's `_connection` attribute; a `connection` alias is a common
    fake-CP shape in tests, so we try both."""
    connections = getattr(request.app.state, "connections", None)
    if connections is None:
        return
    cp = connections.get(cp_id)
    if cp is None:
        return
    ws = getattr(cp, "connection", None) or getattr(cp, "_connection", None)
    if ws is None:
        return
    try:
        await ws.close(1008, reason)
        log.info("authorization.ws_closed", cp_id=cp_id, reason=reason)
    except Exception as exc:
        log.warning(
            "authorization.ws_close_failed",
            cp_id=cp_id,
            reason=reason,
            error=str(exc),
        )


@router.get(
    "/authorizations",
    summary="List devices currently pending operator authorization",
)
async def list_authorizations_route(request: Request) -> dict[str, Any]:
    """Return the Redis pending queue. Only pending devices are
    tracked; authorized fleet lives in `/api/v1/charge-points` and
    rejected / revoked devices have no persistent state (rejected drops
    the Redis row; revoked drops the `charge_points` row)."""
    store = _pending_store(request)
    rows = await store.list_pending()
    return {"items": rows}


@router.post(
    "/authorizations/{cp_id}/authorize",
    summary="Authorize a pending device so it can join the fleet",
)
async def authorize_route(request: Request, cp_id: str) -> dict[str, Any]:
    """Move a pending device from Redis into `charge_points`.

    The pending row's Boot metadata seeds the new `charge_points` row so
    the operator UI has vendor / model / firmware / serial without
    waiting for the next BootNotification. Any live WS is force-closed
    so the charger reconnects and takes the authorized branch
    (`_authorization.py`) on its next upgrade — no half-state where
    the DB says authorized but the connection is still flagged pending.
    """
    _validate_cp_id(cp_id)
    store = _pending_store(request)
    row = await store.pop(cp_id)
    if row is None:
        metrics_registry.AUTHORIZATION_ADMIN_TOTAL.labels(outcome=_ADMIN_OUTCOME_NOT_FOUND).inc()
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=f"no pending authorization for cp_id: {cp_id}",
        )

    now = datetime.now(UTC)
    async with session_scope(request.app.state.session_factory) as session:
        await upsert_charge_point_boot(
            session,
            cp_id=cp_id,
            vendor=row.get("vendor"),
            model=row.get("model"),
            firmware_version=row.get("firmware"),
            serial_number=row.get("serial_number"),
            boot_at=now,
        )

    await _close_live_ws(request, cp_id, "authorization granted; reconnect")
    metrics_registry.AUTHORIZATION_ADMIN_TOTAL.labels(outcome=_ADMIN_OUTCOME_AUTHORIZED).inc()
    log.info("authorization.authorized", cp_id=cp_id)
    return {"cp_id": cp_id, "status": "authorized", "authorized_at": now.isoformat()}


@router.post(
    "/authorizations/{cp_id}/reject",
    summary="Reject a pending device; TTL restarts on next Boot",
)
async def reject_route(request: Request, cp_id: str) -> dict[str, Any]:
    """Drop the pending row for `cp_id`. Not sticky — a fresh WS
    upgrade from the same cp_id creates a new pending row (with a fresh
    TTL). Operators use reject to clear the queue of an obviously-wrong
    entry; recurring rejects live in the IP rate limiter's ban list."""
    _validate_cp_id(cp_id)
    store = _pending_store(request)
    removed = await store.remove(cp_id)
    if not removed:
        metrics_registry.AUTHORIZATION_ADMIN_TOTAL.labels(outcome=_ADMIN_OUTCOME_NOT_FOUND).inc()
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=f"no pending authorization for cp_id: {cp_id}",
        )

    await _close_live_ws(request, cp_id, "authorization rejected")
    metrics_registry.AUTHORIZATION_ADMIN_TOTAL.labels(outcome=_ADMIN_OUTCOME_REJECTED).inc()
    log.info("authorization.rejected", cp_id=cp_id)
    return {"cp_id": cp_id, "status": "rejected"}


@router.post(
    "/authorizations/{cp_id}/revoke",
    summary="Revoke an already-authorized device; force-closes any live WS",
)
async def revoke_route(request: Request, cp_id: str) -> dict[str, Any]:
    """Remove an authorized device from the fleet.

    Deletes the `charge_points` row so the WS auth gate no longer
    recognises the cp_id, then force-closes any live WS this pod is
    hosting. On the next reconnect the device follows the unknown /
    pending branch as if it were a brand-new charger.
    """
    _validate_cp_id(cp_id)
    async with session_scope(request.app.state.session_factory) as session:
        deleted = await delete_charge_point(session, cp_id=cp_id)
    if not deleted:
        metrics_registry.AUTHORIZATION_ADMIN_TOTAL.labels(outcome=_ADMIN_OUTCOME_NOT_FOUND).inc()
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=f"unknown cp_id: {cp_id}",
        )

    await _close_live_ws(request, cp_id, "authorization revoked")
    metrics_registry.AUTHORIZATION_ADMIN_TOTAL.labels(outcome=_ADMIN_OUTCOME_REVOKED).inc()
    log.info("authorization.revoked", cp_id=cp_id)
    return {"cp_id": cp_id, "status": "revoked"}
