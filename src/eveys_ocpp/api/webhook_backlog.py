"""`/api/v1/webhook-backlog` admin routes (E3-9 tail).

Operator surface for the durable webhook backlog. Reads / mutates
rows in ``webhook_delivery_backlog`` — the buffer the drainer keeps
retrying after the in-loop dispatcher has given up. Five routes:

    GET    /webhook-backlog                     list (filter by dead=?, event_type=?)
    GET    /webhook-backlog/{id}                single row (includes body_b64)
    POST   /webhook-backlog/{id}/replay         resurrect one dead row
    DELETE /webhook-backlog/{id}                purge one dead row
    POST   /webhook-backlog/replay-dead         bulk resurrect (optional event_type)

Mutations are dead-only — replay refuses live rows (409), purge
refuses live rows (409). The dispatcher's own retry loop owns the
live subset; the admin surface exists exclusively for dead-letter
recovery.

Auth: bearer-token middleware (``make_bearer_auth_middleware``) —
same as ``/api/v1/authorizations``. Every mutation logs the token
subject (or ``?actor=`` fallback when auth is disabled in dev) so
the operator audit trail survives.
"""

from __future__ import annotations

import base64
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field

from eveys_ocpp.api._errors import (
    ERR_BAD_REQUEST,
    ApiError,
)
from eveys_ocpp.api._pagination import (
    clamp_limit,
    decode_cursor,
    encode_cursor,
)
from eveys_ocpp.observability import get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import (
    get_webhook_backlog_by_id,
    list_webhook_backlog,
    purge_dead_webhook_backlog,
    purge_webhook_backlog,
    resurrect_dead_webhook_backlog,
    resurrect_webhook_backlog,
)

router = APIRouter(tags=["webhook-backlog"])

log = get_logger(__name__)


# Backlog list caps. Lower defaults than /authorizations because the
# body column can be several KB per row and paginating too aggressively
# would blow up the console's memory when there's a real dead-letter
# problem to look at.
_DEFAULT_LIMIT = 50
_MAX_LIMIT = 500

# Module-level Query() singletons — B008 flags Query(...) as a mutable
# default when the parameter type is a mutable container (list[str] |
# None). Hoisting to a constant keeps the pattern the rest of the API
# uses (Query() inline) without tripping the lint.
_EVENT_TYPE_QUERY = Query(
    default=None,
    description=(
        "Repeatable filter. Restricts the list to the given event "
        "types (`cp.boot`, `tx.stopped`, ...)."
    ),
)


def _row_to_response(row: dict[str, Any]) -> dict[str, Any]:
    """Wire shape for a single backlog row. Frozen — Console UI +
    external consumers depend on these keys.

    ``body`` is intentionally excluded from the list projection; it's
    served separately by the single-row endpoint as base64."""
    return {
        "id": str(row["id"]),
        "event_id": str(row["event_id"]),
        "event_type": row["event_type"],
        "url": row["url"],
        "signature": row["signature"],
        "created_at": _isoformat(row.get("created_at")),
        "next_attempt_at": _isoformat(row.get("next_attempt_at")),
        "attempts": row["attempts"],
        "last_error": row.get("last_error"),
        "dead": bool(row["dead"]),
    }


def _isoformat(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _actor(request: Request) -> str:
    """Same identity resolver used by /authorizations. Bearer subject
    when auth is on; ``?actor=`` fallback when disabled; ``unknown``
    when neither is present."""
    subject = getattr(request.state, "token_subject", None)
    if isinstance(subject, str) and subject:
        return subject
    explicit = request.query_params.get("actor")
    if explicit:
        return explicit
    return "unknown"


def _parse_uuid(value: str, *, field: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as exc:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"{field} must be a UUID",
        ) from exc


@router.get(
    "/webhook-backlog",
    summary="List rows in the webhook delivery backlog",
)
async def list_backlog_route(
    request: Request,
    dead: bool | None = Query(
        default=None,
        description=(
            "Filter by dead flag: `true` for dead-letter rows only, "
            "`false` for live (still-retrying) rows only, omit for both."
        ),
    ),
    event_type: list[str] | None = _EVENT_TYPE_QUERY,
    cursor: str | None = Query(default=None),
    limit: int = Query(default=_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT),
) -> dict[str, Any]:
    limit = clamp_limit(limit, default=_DEFAULT_LIMIT, maximum=_MAX_LIMIT)
    payload = decode_cursor(cursor)
    after_id: UUID | None = None
    if payload and "id" in payload:
        after_id = _parse_uuid(str(payload["id"]), field="cursor.id")
    async with session_scope(request.app.state.session_factory) as session:
        rows = await list_webhook_backlog(
            session,
            dead=dead,
            event_types=event_type,
            after_id=after_id,
            limit=limit,
        )
    next_cursor: str | None = None
    if len(rows) > limit:
        # The last row is the sentinel — drop it, encode its id as the
        # next-page cursor. Same shape as authorizations / transactions
        # (see api/_pagination.py docstring).
        overflow = rows[-1]
        rows = rows[:limit]
        next_cursor = encode_cursor({"id": str(overflow["id"])})
    return {
        "rows": [_row_to_response(r) for r in rows],
        "next_cursor": next_cursor,
    }


@router.get(
    "/webhook-backlog/{backlog_id}",
    summary="Fetch a single backlog row including the pending body",
)
async def get_backlog_route(
    request: Request,
    backlog_id: str,
) -> dict[str, Any]:
    backlog_uuid = _parse_uuid(backlog_id, field="backlog_id")
    async with session_scope(request.app.state.session_factory) as session:
        found = await get_webhook_backlog_by_id(session, backlog_id=backlog_uuid)
    if found is None:
        raise ApiError(
            status_code=404,
            error_code=ERR_BAD_REQUEST,
            message=f"unknown backlog id: {backlog_id}",
        )
    row, body = found
    payload = _row_to_response(row)
    # Base64 rather than an escaped JSON string so operators can round-
    # trip the bytes without worrying about the JSON parser mangling
    # them. The Console UI decodes to render the JSON body in a pre.
    payload["body_b64"] = base64.b64encode(body).decode("ascii")
    return payload


@router.post(
    "/webhook-backlog/{backlog_id}/replay",
    summary="Resurrect a dead row: re-arm for immediate delivery",
)
async def replay_backlog_route(
    request: Request,
    backlog_id: str,
) -> dict[str, Any]:
    backlog_uuid = _parse_uuid(backlog_id, field="backlog_id")
    actor = _actor(request)
    async with session_scope(request.app.state.session_factory) as session:
        # Peek first so we can produce a distinct 404 (missing) vs 409
        # (present but not dead) — the update helper's WHERE dead=true
        # can't tell those apart on its own.
        found = await get_webhook_backlog_by_id(session, backlog_id=backlog_uuid)
        if found is None:
            raise ApiError(
                status_code=404,
                error_code=ERR_BAD_REQUEST,
                message=f"unknown backlog id: {backlog_id}",
            )
        row, _body = found
        if not row["dead"]:
            raise ApiError(
                status_code=409,
                error_code=ERR_BAD_REQUEST,
                message="row is not dead; only dead rows can be replayed",
            )
        updated = await resurrect_webhook_backlog(session, backlog_id=backlog_uuid)
    if updated is None:  # pragma: no cover — race after the peek above
        raise ApiError(
            status_code=409,
            error_code=ERR_BAD_REQUEST,
            message="row is not dead; only dead rows can be replayed",
        )
    log.info(
        "webhook_backlog.replay_requested",
        backlog_id=backlog_id,
        event_id=str(updated["event_id"]),
        event_type=updated["event_type"],
        actor=actor,
    )
    return _row_to_response(updated)


@router.delete(
    "/webhook-backlog/{backlog_id}",
    summary="Purge a dead row from the backlog",
)
async def purge_backlog_route(
    request: Request,
    backlog_id: str,
) -> dict[str, Any]:
    backlog_uuid = _parse_uuid(backlog_id, field="backlog_id")
    actor = _actor(request)
    async with session_scope(request.app.state.session_factory) as session:
        found = await get_webhook_backlog_by_id(session, backlog_id=backlog_uuid)
        if found is None:
            raise ApiError(
                status_code=404,
                error_code=ERR_BAD_REQUEST,
                message=f"unknown backlog id: {backlog_id}",
            )
        row, _body = found
        if not row["dead"]:
            raise ApiError(
                status_code=409,
                error_code=ERR_BAD_REQUEST,
                message="row is not dead; only dead rows can be purged",
            )
        deleted = await purge_webhook_backlog(session, backlog_id=backlog_uuid)
    if not deleted:  # pragma: no cover — race after the peek above
        raise ApiError(
            status_code=409,
            error_code=ERR_BAD_REQUEST,
            message="row is not dead; only dead rows can be purged",
        )
    log.info(
        "webhook_backlog.purge_requested",
        backlog_id=backlog_id,
        event_id=str(row["event_id"]),
        event_type=row["event_type"],
        actor=actor,
    )
    return {"deleted": True, "id": backlog_id}


class ReplayDeadBody(BaseModel):
    """Body for bulk-resurrect. Optional filter narrows the scope to
    one or more event types; omit to replay every dead row."""

    event_type: list[str] | None = Field(
        default=None,
        description=(
            "Optional list of event_type values to resurrect. Omit to "
            "replay every dead row across all event types."
        ),
    )


@router.post(
    "/webhook-backlog/replay-dead",
    summary="Bulk-resurrect every dead row (optionally filtered by event_type)",
)
async def replay_dead_backlog_route(
    request: Request,
    body: ReplayDeadBody | None = None,
) -> dict[str, Any]:
    actor = _actor(request)
    event_types = body.event_type if body else None
    async with session_scope(request.app.state.session_factory) as session:
        count = await resurrect_dead_webhook_backlog(session, event_types=event_types)
    log.info(
        "webhook_backlog.bulk_replay_requested",
        count=count,
        event_types=event_types,
        actor=actor,
    )
    return {"count": count}


class PurgeDeadBody(BaseModel):
    """Body for bulk-purge. Same shape as ``ReplayDeadBody`` — optional
    ``event_type`` filter, empty body purges every dead row across every
    type."""

    event_type: list[str] | None = Field(
        default=None,
        description=(
            "Optional list of event_type values to purge. Omit to delete "
            "every dead row across all event types. Live rows are never "
            "touched — the ``WHERE dead=true`` guard is enforced in the "
            "repository layer."
        ),
    )


@router.post(
    "/webhook-backlog/purge-dead",
    summary="Bulk-delete every dead row (optionally filtered by event_type)",
)
async def purge_dead_backlog_route(
    request: Request,
    body: PurgeDeadBody | None = None,
) -> dict[str, Any]:
    actor = _actor(request)
    event_types = body.event_type if body else None
    async with session_scope(request.app.state.session_factory) as session:
        count = await purge_dead_webhook_backlog(session, event_types=event_types)
    log.info(
        "webhook_backlog.bulk_purge_requested",
        count=count,
        event_types=event_types,
        actor=actor,
    )
    return {"count": count}
