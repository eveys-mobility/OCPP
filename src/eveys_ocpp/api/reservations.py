"""`GET /api/v1/charge-points/{cp_id}/reservations` route (E3-7c).

Cursor-paginated list of a charger's reservations. The shape mirrors
the existing `/charge-points/{cp_id}/transactions` endpoint:
`UNKNOWN_CP_ID` for an unknown charger, opaque keyset cursor, optional
filters as query params.

The active reservation set inlined in `GET /charge-points/{cp_id}` is
filtered to `status='Active'` only; this endpoint exposes the full
table including `Pending` and `Cancelled` rows, which operators need
for audit trails. Pass `active=true` to narrow to live reservations
(status=Active AND expiry_date > now()).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Query, Request

from eveys_ocpp.api._errors import (
    ERR_BAD_REQUEST,
    ERR_UNKNOWN_CP_ID,
    ApiError,
)
from eveys_ocpp.api._pagination import clamp_limit, decode_cursor, encode_cursor
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import list_reservations_by_cp

router = APIRouter(tags=["reservations"])


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _to_response(r: dict[str, Any]) -> dict[str, Any]:
    """Drop internal `id`, format datetimes."""
    return {
        "reservation_id": r["reservation_id"],
        "connector_id": r["connector_id"],
        "id_tag": r["id_tag"],
        "parent_id_tag": r["parent_id_tag"],
        "expiry_date": _isoformat(r["expiry_date"]),
        "status": r["status"],
        "created_at": _isoformat(r["created_at"]),
        "updated_at": _isoformat(r["updated_at"]),
    }


@router.get("/charge-points/{cp_id}/reservations")
async def list_reservations_route(
    request: Request,
    cp_id: str,
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=10_000),
    status: str | None = Query(default=None),
    active: bool | None = Query(default=None),
    id_tag: str | None = Query(default=None),
) -> dict[str, Any]:
    settings = request.app.state.settings
    page_size = clamp_limit(
        limit,
        default=settings.rest_default_page_size,
        maximum=settings.rest_max_page_size,
    )

    cursor_payload = decode_cursor(cursor)
    after_id: int | None = None
    if cursor_payload is not None:
        raw_id = cursor_payload.get("id")
        if not isinstance(raw_id, int):
            raise ApiError(
                status_code=400,
                error_code=ERR_BAD_REQUEST,
                message="malformed cursor: missing 'id'",
            )
        after_id = raw_id

    async with session_scope(request.app.state.session_factory) as session:
        rows = await list_reservations_by_cp(
            session,
            cp_id=cp_id,
            after_id=after_id,
            limit=page_size,
            status=status,
            active=active,
            id_tag=id_tag,
        )

    if rows is None:
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=f"unknown cp_id: {cp_id}",
        )

    has_more = len(rows) > page_size
    page = rows[:page_size]
    next_cursor: str | None = None
    if has_more and page:
        next_cursor = encode_cursor({"id": page[-1]["id"]})

    return {
        "reservations": [{**_to_response(r), "cp_id": cp_id} for r in page],
        "next_cursor": next_cursor,
        "request_id": request.state.request_id,
    }
