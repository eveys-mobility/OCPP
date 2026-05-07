"""`GET /api/v1/charge-points/{cp_id}/charging-profiles` route (E3-7c).

Cursor-paginated list of charging profiles installed on a charger,
with each profile's schedule periods inlined. Same pattern as the
reservations and transactions routes — `UNKNOWN_CP_ID` for an unknown
charger, opaque keyset cursor.

This is the gateway-side mirror per ADR-0022 — it shows the *input*
the operator pushed, not the resolved composite schedule. To read the
resolved schedule, callers must round-trip the charger via
`POST /charge-points/{cp_id}/commands/get-composite-schedule`.
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
from eveys_ocpp.api._schemas import ChargingProfileListResponse
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import list_charging_profiles_by_cp

router = APIRouter(tags=["charging_profiles"])


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _to_response(p: dict[str, Any]) -> dict[str, Any]:
    """Drop internal `id`, format datetimes."""
    return {
        "charging_profile_id": p["charging_profile_id"],
        "connector_id": p["connector_id"],
        "stack_level": p["stack_level"],
        "purpose": p["purpose"],
        "kind": p["kind"],
        "recurrency_kind": p["recurrency_kind"],
        "valid_from": _isoformat(p["valid_from"]),
        "valid_to": _isoformat(p["valid_to"]),
        "transaction_id": p["transaction_id"],
        "charging_rate_unit": p["charging_rate_unit"],
        "min_charging_rate": p["min_charging_rate"],
        "schedule_duration": p["schedule_duration"],
        "start_schedule": _isoformat(p["start_schedule"]),
        "status": p["status"],
        "schedule_periods": p["schedule_periods"],
        "created_at": _isoformat(p["created_at"]),
        "updated_at": _isoformat(p["updated_at"]),
    }


@router.get(
    "/charge-points/{cp_id}/charging-profiles",
    summary="List active charging profiles for a charge point (cursor-paginated)",
    responses={200: {"model": ChargingProfileListResponse}},
)
async def list_charging_profiles_route(
    request: Request,
    cp_id: str,
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=10_000),
    purpose: str | None = Query(default=None),
    stack_level: int | None = Query(default=None),
    connector_id: int | None = Query(default=None),
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
        rows = await list_charging_profiles_by_cp(
            session,
            cp_id=cp_id,
            after_id=after_id,
            limit=page_size,
            purpose=purpose,
            stack_level=stack_level,
            connector_id=connector_id,
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
        "charging_profiles": [{**_to_response(p), "cp_id": cp_id} for p in page],
        "next_cursor": next_cursor,
        "request_id": request.state.request_id,
    }
