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
from typing import Annotated, Any

from fastapi import APIRouter, Query, Request

from eveys_ocpp._ocpp_enums import (
    ocpp_string_for,
    proto_enum_name_for_measurand,
)
from eveys_ocpp.api._errors import (
    ERR_BAD_REQUEST,
    ERR_INTERNAL_ERROR,
    ERR_UNKNOWN_CP_ID,
    ERR_WINDOW_TOO_LARGE,
    ApiError,
)
from eveys_ocpp.api._pagination import (
    clamp_limit,
    decode_cursor,
    encode_cursor,
    offset_for_page,
    pagination_block,
    reject_mixed_pagination,
)
from eveys_ocpp.api._schemas import (
    ErrorEnvelope,
    FleetStatusResponse,
    MeterValuesResponse,
    OcppFramesByCpResponse,
    OfflineHistoryResponse,
    StatusHistoryResponse,
    UptimeResponse,
)
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import get_charge_point_pk

router = APIRouter(tags=["timeseries"])

_MAX_WINDOW = timedelta(days=7)
# Uptime is an aggregation, not a time-series stream — operators
# routinely want quarterly / monthly answers, and a single aggregate
# row out of ClickHouse is cheap even over 90 days. Loosen the
# window cap accordingly while keeping a hard upper bound so a
# typo can't fan the scan out across all partitions.
_UPTIME_MAX_WINDOW = timedelta(days=90)


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


@router.get(
    "/charge-points/{cp_id}/meter-values",
    summary="ClickHouse-backed MeterValues time-series for a charge point",
    responses={
        200: {"model": MeterValuesResponse},
        400: {"model": ErrorEnvelope, "description": "Bad from/to or window too large."},
        404: {"model": ErrorEnvelope, "description": "Unknown cp_id."},
    },
)
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

    # Storage-form translation: API takes the OCPP wire form
    # (`?measurand=Voltage`); ClickHouse rows store the proto enum name
    # (`MEASURAND_VOLTAGE`). Translate before the SQL goes out so an
    # unknown wire string returns an empty page rather than silently
    # matching every UNSPECIFIED row.
    storage_measurand = measurand
    if measurand is not None:
        storage_measurand = proto_enum_name_for_measurand(measurand)
        if storage_measurand is None:
            return {
                "meter_values": [],
                "request_id": request.state.request_id,
            }

    rows = await _ch_client(request).fetch_meter_values(
        cp_id=cp_id,
        started_from=started_from,
        started_to=started_to,
        connector_id=connector_id,
        measurand=storage_measurand,
        limit=page_size,
    )

    return {
        "meter_values": [_meter_to_response(r) for r in rows],
        "request_id": request.state.request_id,
    }


@router.get(
    "/charge-points/{cp_id}/status-history",
    summary="ClickHouse-backed StatusNotification history for a charge point",
    responses={
        200: {"model": StatusHistoryResponse},
        400: {"model": ErrorEnvelope, "description": "Bad from/to or window too large."},
        404: {"model": ErrorEnvelope, "description": "Unknown cp_id."},
    },
)
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


@router.get(
    "/status-history",
    summary="Fleet-wide StatusNotification history (e.g. all Faulted statuses this week)",
    responses={
        200: {"model": FleetStatusResponse},
        400: {"model": ErrorEnvelope, "description": "Bad from/to or window too large."},
    },
)
async def list_fleet_status_history(
    request: Request,
    from_: str = Query(alias="from"),
    to: str = Query(...),
    status: Annotated[list[str] | None, Query()] = None,
    cp_id: Annotated[list[str] | None, Query()] = None,
    limit: int | None = Query(default=None, ge=1, le=10_000),
) -> dict[str, Any]:
    """Cross-charger variant of the per-cp ``status-history`` route.

    Common use: an operator filtering for ``status=Faulted`` over the
    last week to surface every charger that flipped to a fault state.

    ``status`` and ``cp_id`` are both repeatable query params: e.g.
    ``?status=Faulted&status=Unavailable`` to get either, or
    ``?cp_id=A&cp_id=B`` to restrict to two chargers without paying
    the cost of one round-trip per cp_id."""
    settings = request.app.state.settings
    page_size = clamp_limit(
        limit,
        default=settings.rest_default_page_size,
        maximum=settings.rest_max_page_size,
    )

    started_from = _parse_iso8601(from_, field_name="from")
    started_to = _parse_iso8601(to, field_name="to")
    _validate_window(started_from, started_to)

    rows = await _ch_client(request).fetch_fleet_status_history(
        started_from=started_from,
        started_to=started_to,
        statuses=status,
        cp_ids=cp_id,
        limit=page_size,
    )

    return {
        "events": [_status_to_response(r) for r in rows],
        "request_id": request.state.request_id,
    }


@router.get(
    "/charge-points/{cp_id}/frames",
    summary="ClickHouse-backed OCPP frame audit for a charge point (both directions)",
    responses={
        200: {"model": OcppFramesByCpResponse},
        400: {"model": ErrorEnvelope, "description": "Bad from/to or window too large."},
        404: {"model": ErrorEnvelope, "description": "Unknown cp_id."},
    },
)
async def list_frames_by_cp(
    request: Request,
    cp_id: str,
    from_: str = Query(alias="from"),
    to: str = Query(...),
    direction: str | None = Query(default=None),
    action: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=10_000),
) -> dict[str, Any]:
    """Verbatim OCPP frame trail for one charger. Each row is the
    exact JSON the gateway received (``direction=inbound``) or wrote
    on the wire (``direction=outbound``).

    Optional ``direction`` scopes to one side; optional ``action``
    filters on the OCPP action name (``BootNotification``,
    ``MeterValues``, …). Same 7-day window cap as the meter +
    status routes."""
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

    rows = await _ch_client(request).fetch_frames_by_cp(
        cp_id=cp_id,
        started_from=started_from,
        started_to=started_to,
        direction=direction,
        action=action,
        limit=page_size,
    )

    return {
        "cp_id": cp_id,
        "frames": [_frame_to_response(r) for r in rows],
        "request_id": request.state.request_id,
    }


def _frame_to_response(row: dict[str, Any]) -> dict[str, Any]:
    """Project a ClickHouse `cp_ocpp_frames` row into the API shape.
    Empty strings normalised to None for nullable-feeling fields so
    consumers don't have to special-case both forms."""
    return {
        "event_id": row["event_id"],
        "occurred_at": _isoformat(row["occurred_at"]),
        "cp_id": row["cp_id"],
        "direction": row["direction"],
        "action": row["action"] or "",
        "message_type": row["message_type"],
        "message_id": row["message_id"] or "",
        "ocpp_version": row["ocpp_version"] or "",
        "transaction_id": row.get("transaction_id"),
        "raw_payload": row["raw_payload"],
    }


def _meter_to_response(row: dict[str, Any]) -> dict[str, Any]:
    """Project a ClickHouse `cp_meter` row into the API shape.

    Storage form is the proto enum name (`"MEASURAND_VOLTAGE"`,
    `"PHASE_L1"`, …); the API exposes the OCPP wire form
    (`"Voltage"`, `"L1"`). `*_UNSPECIFIED`, vendor extensions, and
    empty strings all surface as `null` rather than leak the proto
    sentinel to clients."""
    return {
        "event_id": row["event_id"],
        "occurred_at": _isoformat(row["occurred_at"]),
        "cp_id": row["cp_id"],
        "connector_id": row["connector_id"],
        "transaction_id": row["transaction_id"] or None,
        "charger_reported_at": row["charger_reported_at"] or None,
        "sample": {
            "value": row["value"],
            "context": ocpp_string_for("context", row["context"]),
            "format": ocpp_string_for("format", row["format"]),
            "measurand": ocpp_string_for("measurand", row["measurand"]),
            "phase": ocpp_string_for("phase", row["phase"]),
            "location": ocpp_string_for("location", row["location"]),
            "unit": ocpp_string_for("unit", row["unit"]),
        },
    }


@router.get(
    "/charge-points/{cp_id}/offline-history",
    summary="ClickHouse-backed offline-duration history for a charge point",
    responses={
        200: {"model": OfflineHistoryResponse},
        400: {"model": ErrorEnvelope, "description": "Bad since/until or pagination."},
        404: {"model": ErrorEnvelope, "description": "Unknown cp_id."},
    },
)
async def list_offline_history(
    request: Request,
    cp_id: str,
    since: str | None = Query(default=None),
    until: str | None = Query(default=None),
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=10_000),
    page: int | None = Query(default=None, ge=1),
    page_size: int | None = Query(default=None, ge=1, le=10_000),
) -> dict[str, Any]:
    settings = request.app.state.settings
    reject_mixed_pagination(cursor=cursor, page=page)

    since_dt = _parse_iso8601(since, field_name="since") if since else None
    until_dt = _parse_iso8601(until, field_name="until") if until else None
    if since_dt is not None and until_dt is not None and until_dt < since_dt:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="`until` must be >= `since`",
        )

    await _ensure_cp_exists(request, cp_id)

    client = _ch_client(request)

    if page is not None:
        effective_size = clamp_limit(
            page_size if page_size is not None else limit,
            default=settings.rest_default_page_size,
            maximum=settings.rest_max_page_size,
        )
        offset = offset_for_page(page, effective_size)
        rows, total = await client.fetch_offline_history(
            cp_id=cp_id,
            since=since_dt,
            until=until_dt,
            limit=effective_size,
            offset=offset,
        )
        return {
            "cp_id": cp_id,
            "offline_windows": [_offline_to_response(r) for r in rows],
            "pagination": pagination_block(page=page, page_size=effective_size, total=total),
            "request_id": request.state.request_id,
        }

    effective_size = clamp_limit(
        limit,
        default=settings.rest_default_page_size,
        maximum=settings.rest_max_page_size,
    )

    # Cursor mode keys on `offset` — same opaque-base64 contract as the
    # /charge-points cursor, just paged by row count instead of a
    # surrogate id (ClickHouse has no stable PK to keyset on; ordering
    # is by came_online_at DESC). Acceptable for a per-charger feed
    # that's bounded to one CP's outages.
    cursor_payload = decode_cursor(cursor)
    cursor_offset = 0
    if cursor_payload is not None:
        raw_offset = cursor_payload.get("offset")
        if not isinstance(raw_offset, int) or raw_offset < 0:
            raise ApiError(
                status_code=400,
                error_code=ERR_BAD_REQUEST,
                message="malformed cursor: missing or negative 'offset'",
            )
        cursor_offset = raw_offset

    rows, total = await client.fetch_offline_history(
        cp_id=cp_id,
        since=since_dt,
        until=until_dt,
        limit=effective_size + 1,
        offset=cursor_offset,
    )
    has_more = len(rows) > effective_size
    page_rows = rows[:effective_size]
    next_cursor: str | None = None
    if has_more:
        next_cursor = encode_cursor({"offset": cursor_offset + effective_size})

    return {
        "cp_id": cp_id,
        "offline_windows": [_offline_to_response(r) for r in page_rows],
        "next_cursor": next_cursor,
        "request_id": request.state.request_id,
    }


def _offline_to_response(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "occurred_at": _isoformat(row["occurred_at"]),
        "went_offline_at": _isoformat(row["went_offline_at"]),
        "came_online_at": _isoformat(row["came_online_at"]),
        "offline_seconds": int(row["offline_seconds"]),
        "prior_pod_id": row["prior_pod_id"] or None,
        "prior_reason": row["prior_reason"] or None,
    }


@router.get(
    "/charge-points/{cp_id}/uptime",
    summary="Uptime % for a charge point over a date range",
    responses={
        200: {"model": UptimeResponse},
        400: {"model": ErrorEnvelope, "description": "Bad from/to or window too large."},
        404: {"model": ErrorEnvelope, "description": "Unknown cp_id."},
    },
)
async def get_uptime_for_cp(
    request: Request,
    cp_id: str,
    from_: str = Query(alias="from"),
    to: str = Query(...),
) -> dict[str, Any]:
    """Computed from completed offline intervals in
    ``cp_offline_duration``. Intervals overlapping the
    ``[from, to]`` window are clipped at the boundaries and summed.

    Returns ``uptime_pct``, the total offline + online seconds inside
    the window, and the contributing intervals. **In-flight outages
    are not counted** — a charger that's currently offline has no
    ``came_online_at`` row yet. The detail route's ``online`` flag
    is the right place to read live state from.

    The window cap is 90 days (vs 7 for the time-series streams) —
    aggregations are cheap and operators want quarterly answers."""
    started_from = _parse_iso8601(from_, field_name="from")
    started_to = _parse_iso8601(to, field_name="to")
    if started_to < started_from:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="`to` must be >= `from`",
        )
    if started_to - started_from > _UPTIME_MAX_WINDOW:
        raise ApiError(
            status_code=400,
            error_code=ERR_WINDOW_TOO_LARGE,
            message=f"window cannot exceed {_UPTIME_MAX_WINDOW.days} days",
        )

    await _ensure_cp_exists(request, cp_id)

    offline_total, raw_intervals = await _ch_client(request).fetch_uptime_for_cp(
        cp_id=cp_id,
        window_from=started_from,
        window_to=started_to,
    )

    window_seconds = int((started_to - started_from).total_seconds())
    # Clamp: floating-point or row-edge weirdness shouldn't surface
    # offline > window. The clip math in fetch_uptime_for_cp already
    # bounds each interval, but belt-and-braces.
    offline_total = max(0, min(offline_total, window_seconds))
    online_total = window_seconds - offline_total
    uptime_pct = (online_total / window_seconds * 100.0) if window_seconds > 0 else 100.0

    return {
        "cp_id": cp_id,
        "uptime_pct": round(uptime_pct, 4),
        "offline_seconds_total": offline_total,
        "online_seconds_total": online_total,
        "intervals": [_uptime_interval_to_response(i) for i in raw_intervals],
        "window": {
            "from": _isoformat(started_from),
            "to": _isoformat(started_to),
            "seconds": window_seconds,
        },
        "request_id": request.state.request_id,
    }


def _uptime_interval_to_response(interval: dict[str, Any]) -> dict[str, Any]:
    return {
        "went_offline_at": _isoformat(interval["went_offline_at"]),
        "came_online_at": _isoformat(interval["came_online_at"]),
        "offline_seconds": int(interval["offline_seconds"]),
        "prior_reason": interval.get("prior_reason") or None,
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
