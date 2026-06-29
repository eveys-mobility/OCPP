"""`/api/v1/authorizations` routes (#0013).

Operator surface for the device-authorization allowlist:

    GET    /authorizations                           list (filter by ?status=)
    POST   /authorizations/{cp_id}/approve
    POST   /authorizations/{cp_id}/reject
    POST   /authorizations/{cp_id}/revoke

`approve` transitions a `pending` (or previously `rejected`) row to
`approved`. `reject` is for "no, this isn't one of ours" — the
charger is locked out until an operator changes its mind. `revoke`
is for "was ours, no longer trusted" — same lockout effect, plus the
gateway force-closes any live WS the charger has open.

The actor (`decided_by`) defaults to the bearer-token subject — the
common middleware (`make_bearer_auth_middleware`) already validates
the token and the same identity flows through every operator route.
Tests bypass auth via `EVEYS_OCPP_REST_AUTH_DISABLED=true`; in that
mode the route accepts `?actor=` as a fallback so the audit trail
still has something to record.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Query, Request

from eveys_ocpp.api._errors import (
    ERR_BAD_REQUEST,
    ERR_UNKNOWN_CP_ID,
    ApiError,
)
from eveys_ocpp.observability import get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.models import (
    AUTH_STATUS_APPROVED,
    AUTH_STATUS_REJECTED,
    AUTH_STATUS_REVOKED,
    AUTH_STATUSES,
)
from eveys_ocpp.persistence.repositories import (
    decide_authorization,
    list_authorizations,
)

router = APIRouter(tags=["authorizations"])

log = get_logger(__name__)


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _to_response(row: dict[str, Any]) -> dict[str, Any]:
    """Project the repository's dict shape to the JSON wire shape.

    Wire format is stable — adding new columns later means adding new
    keys, not renaming existing ones. The console's API types follow
    the same shape one-for-one.
    """
    return {
        "cp_id": row["cp_id"],
        "status": row["status"],
        "requested_at": _isoformat(row["requested_at"]),
        "decided_at": _isoformat(row["decided_at"]),
        "decided_by": row["decided_by"],
        "last_attempt_ip": row["last_attempt_ip"],
        "last_attempt_user_agent": row["last_attempt_user_agent"],
        "last_attempt_at": _isoformat(row["last_attempt_at"]),
    }


def _actor(request: Request) -> str:
    """Identity to write into `decided_by`.

    1. Bearer-auth middleware stashes the token subject at
       `request.state.token_subject` when auth is on.
    2. With auth disabled (dev / e2e), accept `?actor=` as a hint.
    3. Default to "unknown" so the column is never null on a decided
       row.
    """
    subject = getattr(request.state, "token_subject", None)
    if isinstance(subject, str) and subject:
        return subject
    explicit = request.query_params.get("actor")
    if explicit:
        return explicit
    return "unknown"


def _validate_cp_id(cp_id: str) -> None:
    """Same bounds as the rest of the API — the path parameter is
    `cp_id` per OCPP convention. Reject empty / overly-long inputs so
    a malformed URL can't poison the table."""
    if not cp_id or len(cp_id) > 64:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="cp_id must be 1..64 chars",
        )


@router.get(
    "/authorizations",
    summary="List device authorizations",
)
async def list_authorizations_route(
    request: Request,
    status: str | None = Query(
        default=None,
        description=(
            "Filter by status. One of `pending`, `approved`, "
            "`rejected`, `revoked`. Omit to list all (capped at the "
            "server's page size)."
        ),
    ),
    limit: int = Query(default=200, ge=1, le=500),
) -> dict[str, Any]:
    if status is not None and status not in AUTH_STATUSES:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"status must be one of {sorted(AUTH_STATUSES)}",
        )
    async with session_scope(request.app.state.session_factory) as session:
        rows = await list_authorizations(session, status=status, limit=limit)
    return {"items": [_to_response(r) for r in rows]}


async def _decide(request: Request, *, cp_id: str, new_status: str) -> dict[str, Any]:
    _validate_cp_id(cp_id)
    actor = _actor(request)
    async with session_scope(request.app.state.session_factory) as session:
        row = await decide_authorization(
            session,
            cp_id=cp_id,
            new_status=new_status,
            decided_by=actor,
            now=datetime.now(UTC),
        )
    if row is None:
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=f"unknown cp_id: {cp_id}",
        )
    log.info(
        "authorization.decided",
        cp_id=cp_id,
        new_status=new_status,
        actor=actor,
    )
    return _to_response(row)


@router.post(
    "/authorizations/{cp_id}/approve",
    summary="Approve a charger so it can connect",
)
async def approve_route(request: Request, cp_id: str) -> dict[str, Any]:
    return await _decide(request, cp_id=cp_id, new_status=AUTH_STATUS_APPROVED)


@router.post(
    "/authorizations/{cp_id}/reject",
    summary="Reject a charger; future upgrades return 401",
)
async def reject_route(request: Request, cp_id: str) -> dict[str, Any]:
    return await _decide(request, cp_id=cp_id, new_status=AUTH_STATUS_REJECTED)


@router.post(
    "/authorizations/{cp_id}/revoke",
    summary=(
        "Revoke an approved charger; force-disconnects any live WS and rejects future upgrades"
    ),
)
async def revoke_route(request: Request, cp_id: str) -> dict[str, Any]:
    result = await _decide(request, cp_id=cp_id, new_status=AUTH_STATUS_REVOKED)
    # Force-disconnect any live WS this pod is hosting. Cross-pod
    # revocation relies on the next reconnect being rejected — the
    # registry doesn't carry a "kick this cp_id" channel today, and
    # adding one is its own scope. Loud log so operators can spot a
    # cross-pod revoke that didn't immediately disconnect.
    connections = getattr(request.app.state, "connections", None)
    if connections is not None:
        cp = connections.get(cp_id)
        if cp is not None and hasattr(cp, "connection"):
            try:
                await cp.connection.close(1008, "authorization revoked")
                log.info("authorization.revoked_disconnected", cp_id=cp_id)
            except Exception as exc:
                log.warning(
                    "authorization.revoke_close_failed",
                    cp_id=cp_id,
                    error=str(exc),
                )
    return result
