"""Timeseries read endpoints (E3-7d).

Two ClickHouse-backed GET routes under `/api/v1/charge-points/{cp_id}/`:

- `/meter-values` — paginated samples from the `cp_meter` table.
- `/status-history` — paginated transitions from the `cp_status` table.

Both require a `[from, to]` window on `occurred_at` (the trustworthy
server-receive timestamp; charger-reported timestamps are surfaced
verbatim but never queried). Windows wider than 7 days return 400
`WINDOW_TOO_LARGE` so a sloppy query can't scan the whole partition
range.

This is the gateway's interface to the time-series store. ClickHouse
itself is the backend per ADR-0004 / ADR-0020; the read client wrapper
lives at `clickhouse/read_client.py`.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Query, Request

from eveys_ocpp.api._errors import (
    ERR_BAD_REQUEST,
    ERR_INTERNAL_ERROR,
    ERR_UNKNOWN_CP_ID,
    ERR_WINDOW_TOO_LARGE,
    ApiError,
)
from eveys_ocpp.api._pagination import clamp_limit
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import get_charge_point_pk

router = APIRouter(tags=["timeseries"])

_MAX_WINDOW = timedelta(days=7)


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_iso8601(value: str, *, field_name: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"invalid {field_name}: not ISO-8601",
        ) from exc


def _validate_window(started_from: datetime, started_to: datetime) -> None:
    if started_to < started_from:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="`to` must be >= `from`",
        )
    if started_to - started_from > _MAX_WINDOW:
        raise ApiError(
            status_code=400,
            error_code=ERR_WINDOW_TOO_LARGE,
            message=f"window cannot exceed {_MAX_WINDOW.days} days",
        )


async def _ensure_cp_exists(request: Request, cp_id: str) -> None:
    """404 if the charger has never sent a BootNotification.

    Same shape as the Postgres-backed routes — we look up the surrogate
    PK; absence means UNKNOWN_CP_ID. Avoids ClickHouse round-trips for
    bogus cp_ids (which would just return empty rows misleadingly).
    """
    async with session_scope(request.app.state.session_factory) as session:
        pk = await get_charge_point_pk(session, cp_id=cp_id)
    if pk is None:
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=f"unknown cp_id: {cp_id}",
        )


def _ch_client(request: Request) -> Any:
    client = getattr(request.app.state, "ch_client", None)
    if client is None:
        raise ApiError(
            status_code=500,
            error_code=ERR_INTERNAL_ERROR,
            message="ClickHouse read client not configured on this gateway",
        )
    return client


@router.get("/charge-points/{cp_id}/meter-values")
async def list_meter_values(
    request: Request,
    cp_id: str,
    from_: str = Query(alias="from"),
    to: str = Query(...),
    connector_id: int | None = Query(default=None),
    measurand: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=10_000),
) -> dict[str, Any]:
    settings = request.app.state.settings
    page_size = clamp_limit(
        limit,
        default=settings.rest_default_page_size,
        maximum=settings.rest_max_page_size,
    )

    started_from = _parse_iso8601(from_, field_name="from")
    started_to = _parse_iso8601(to, field_name="to")
    _validate_window(started_from, started_to)

    await _ensure_cp_exists(request, cp_id)

    rows = await _ch_client(request).fetch_meter_values(
        cp_id=cp_id,
        started_from=started_from,
        started_to=started_to,
        connector_id=connector_id,
        measurand=measurand,
        limit=page_size,
    )

    return {
        "meter_values": [_meter_to_response(r) for r in rows],
        "request_id": request.state.request_id,
    }


@router.get("/charge-points/{cp_id}/status-history")
async def list_status_history(
    request: Request,
    cp_id: str,
    from_: str = Query(alias="from"),
    to: str = Query(...),
    connector_id: int | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=10_000),
) -> dict[str, Any]:
    settings = request.app.state.settings
    page_size = clamp_limit(
        limit,
        default=settings.rest_default_page_size,
        maximum=settings.rest_max_page_size,
    )

    started_from = _parse_iso8601(from_, field_name="from")
    started_to = _parse_iso8601(to, field_name="to")
    _validate_window(started_from, started_to)

    await _ensure_cp_exists(request, cp_id)

    rows = await _ch_client(request).fetch_status_history(
        cp_id=cp_id,
        started_from=started_from,
        started_to=started_to,
        connector_id=connector_id,
        limit=page_size,
    )

    return {
        "status_history": [_status_to_response(r) for r in rows],
        "request_id": request.state.request_id,
    }


def _meter_to_response(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "occurred_at": _isoformat(row["occurred_at"]),
        "cp_id": row["cp_id"],
        "connector_id": row["connector_id"],
        "transaction_id": row["transaction_id"] or None,
        "charger_reported_at": row["charger_reported_at"] or None,
        "sample": {
            "value": row["value"],
            "context": row["context"] or None,
            "format": row["format"] or None,
            "measurand": row["measurand"] or None,
            "phase": row["phase"] or None,
            "location": row["location"] or None,
            "unit": row["unit"] or None,
        },
    }


def _status_to_response(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "occurred_at": _isoformat(row["occurred_at"]),
        "cp_id": row["cp_id"],
        "connector_id": row["connector_id"],
        "status": row["status"],
        "error_code": row["error_code"] or None,
        "info": row["info"] or None,
        "vendor_id": row["vendor_id"] or None,
        "vendor_error_code": row["vendor_error_code"] or None,
        "charger_reported_at": row["charger_reported_at"] or None,
    }
