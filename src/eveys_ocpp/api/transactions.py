"""`GET /api/v1/transactions/{transaction_id}` and
`GET /api/v1/charge-points/{cp_id}/transactions` routes (E3-7 commit 2).

Per the contract `docs/integration/02-gateway-rest-api.md`:
- Single-transaction detail keys off the OCPP-visible `transaction_id`,
  not the surrogate PK.
- The list endpoint supports `from`/`to` (window on
  `started_reported_at`), `id_tag` exact match, and `open` (currently
  charging vs. stopped). Cursor-paginated.
- 404 `UNKNOWN_TRANSACTION_ID` and `UNKNOWN_CP_ID` are the only error
  cases beyond `BAD_REQUEST` (cursor malformed, time window not
  parseable).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Query, Request

from eveys_ocpp.api._errors import (
    ERR_BAD_REQUEST,
    ERR_UNKNOWN_CP_ID,
    ERR_UNKNOWN_TRANSACTION_ID,
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
    OcppFramesByTransactionResponse,
    TransactionDetail,
    TransactionListResponse,
    TransactionsAggregateResponse,
)
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import (
    aggregate_transactions,
    count_transactions,
    count_transactions_by_cp,
    get_transaction_by_id,
    list_transactions,
    list_transactions_by_cp,
)

router = APIRouter(tags=["transactions"])

# Sort + direction enums for `GET /transactions`. The Console UI is the
# only intended consumer today; expand here when a new use case lands.
_TX_SORT_VALUES = {"id", "started_at", "stopped_at", "consumed_wh"}
_TX_DIR_VALUES = {"asc", "desc"}


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_iso8601(value: str | None, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"invalid {field_name}: not ISO-8601",
        ) from exc


def _transaction_to_response(tx: dict[str, Any]) -> dict[str, Any]:
    """Wire shape — drop the internal surrogate `id`, format datetimes.

    Emits both the precise `*_reported_at` / `*_received_at` pair and the
    convenience aliases `started_at` / `stopped_at` (pointing at the
    `_reported_at` value — the charger's wall clock, which is what UIs
    render). The `open` flag is `True` while `stopped_reported_at` is
    null so callers don't have to recompute it.
    """
    started_reported = _isoformat(tx["started_reported_at"])
    stopped_reported = _isoformat(tx["stopped_reported_at"])
    return {
        "transaction_id": tx["transaction_id"],
        "cp_id": tx.get("cp_id"),  # filled by route when needed
        "connector_id": tx["connector_id"],
        "id_tag": tx["id_tag"],
        "meter_start_wh": tx["meter_start_wh"],
        "meter_stop_wh": tx["meter_stop_wh"],
        "consumed_wh": tx["consumed_wh"],
        "started_reported_at": started_reported,
        "started_received_at": _isoformat(tx["started_received_at"]),
        "stopped_reported_at": stopped_reported,
        "stopped_received_at": _isoformat(tx["stopped_received_at"]),
        "stop_reason": tx["stop_reason"],
        # Convenience aliases for UI consumers — `started_at` /
        # `stopped_at` are the charger-reported timestamps a human-
        # facing view actually wants, and `open` is the boolean form
        # of "this session hasn't stopped yet."
        "started_at": started_reported,
        "stopped_at": stopped_reported,
        "open": tx["stopped_reported_at"] is None,
    }


@router.get(
    "/charge-points/{cp_id}/transactions",
    summary="List transactions for a charge point (cursor-paginated)",
    responses={
        200: {"model": TransactionListResponse},
        400: {
            "model": ErrorEnvelope,
            "description": "Bad cursor or unparseable from/to timestamp.",
        },
    },
)
async def list_transactions_route(
    request: Request,
    cp_id: str,
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=10_000),
    page: int | None = Query(default=None, ge=1),
    page_size: int | None = Query(default=None, ge=1, le=10_000),
    id_tag: str | None = Query(default=None),
    open: bool | None = Query(default=None),
    from_: str | None = Query(default=None, alias="from"),
    to: str | None = Query(default=None),
    connector_id: int | None = Query(default=None),
    stop_reason: str | None = Query(default=None),
    stopped_after: str | None = Query(default=None),
    stopped_before: str | None = Query(default=None),
    min_consumed_wh: int | None = Query(default=None, ge=0),
    max_consumed_wh: int | None = Query(default=None, ge=0),
) -> dict[str, Any]:
    settings = request.app.state.settings
    reject_mixed_pagination(cursor=cursor, page=page)

    filter_kwargs: dict[str, Any] = {
        "id_tag": id_tag,
        "open_only": open,
        "started_from": _parse_iso8601(from_, field_name="from"),
        "started_to": _parse_iso8601(to, field_name="to"),
        "connector_id": connector_id,
        "stop_reason": stop_reason,
        "stopped_from": _parse_iso8601(stopped_after, field_name="stopped_after"),
        "stopped_to": _parse_iso8601(stopped_before, field_name="stopped_before"),
        "min_consumed_wh": min_consumed_wh,
        "max_consumed_wh": max_consumed_wh,
    }

    if page is not None:
        effective_size = clamp_limit(
            page_size if page_size is not None else limit,
            default=settings.rest_default_page_size,
            maximum=settings.rest_max_page_size,
        )
        offset = offset_for_page(page, effective_size)
        async with session_scope(request.app.state.session_factory) as session:
            rows = await list_transactions_by_cp(
                session,
                cp_id=cp_id,
                after_id=None,
                limit=effective_size,
                offset=offset,
                **filter_kwargs,
            )
            total = await count_transactions_by_cp(
                session,
                cp_id=cp_id,
                **filter_kwargs,
            )
        if rows is None or total is None:
            raise ApiError(
                status_code=404,
                error_code=ERR_UNKNOWN_CP_ID,
                message=f"unknown cp_id: {cp_id}",
            )
        return {
            "transactions": [{**_transaction_to_response(tx), "cp_id": cp_id} for tx in rows],
            "pagination": pagination_block(page=page, page_size=effective_size, total=total),
            "request_id": request.state.request_id,
        }

    effective_size = clamp_limit(
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
        rows = await list_transactions_by_cp(
            session,
            cp_id=cp_id,
            after_id=after_id,
            limit=effective_size,
            **filter_kwargs,
        )

    if rows is None:
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=f"unknown cp_id: {cp_id}",
        )

    has_more = len(rows) > effective_size
    page_rows = rows[:effective_size]
    next_cursor: str | None = None
    if has_more and page_rows:
        next_cursor = encode_cursor({"id": page_rows[-1]["id"]})

    return {
        "transactions": [{**_transaction_to_response(tx), "cp_id": cp_id} for tx in page_rows],
        "next_cursor": next_cursor,
        "request_id": request.state.request_id,
    }


@router.get(
    "/transactions",
    summary="List transactions (cursor-paginated, global)",
    responses={
        200: {"model": TransactionListResponse},
        400: {
            "model": ErrorEnvelope,
            "description": "Bad cursor, sort/dir, or unparseable from/to timestamp.",
        },
    },
)
async def list_transactions_global_route(
    request: Request,
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=10_000),
    page: int | None = Query(default=None, ge=1),
    page_size: int | None = Query(default=None, ge=1, le=10_000),
    cp_id: str | None = Query(default=None),
    id_tag: str | None = Query(default=None),
    active: bool | None = Query(default=None),
    from_: str | None = Query(default=None, alias="from"),
    to: str | None = Query(default=None),
    connector_id: int | None = Query(default=None),
    stop_reason: str | None = Query(default=None),
    stopped_after: str | None = Query(default=None),
    stopped_before: str | None = Query(default=None),
    min_consumed_wh: int | None = Query(default=None, ge=0),
    max_consumed_wh: int | None = Query(default=None, ge=0),
    sort: str | None = Query(default=None),
    dir: str | None = Query(default=None),
) -> dict[str, Any]:
    settings = request.app.state.settings
    reject_mixed_pagination(cursor=cursor, page=page)

    if sort is not None and sort not in _TX_SORT_VALUES:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"unknown sort: {sort!r}; expected one of {sorted(_TX_SORT_VALUES)}",
        )
    if dir is not None and dir not in _TX_DIR_VALUES:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"unknown dir: {dir!r}; expected 'asc' or 'desc'",
        )
    # Cursor pagination is only keyset-stable when sorting on the
    # surrogate id. For any other sort the route requires offset
    # (page) mode — otherwise `next_cursor` would skip rows whose
    # sort_value tied with the last row on the previous page.
    if sort is not None and sort != "id" and cursor is not None:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=(
                "cursor pagination is not supported with a non-default sort; "
                "use `page` + `page_size` instead"
            ),
        )

    filter_kwargs: dict[str, Any] = {
        "cp_id": cp_id,
        "id_tag": id_tag,
        "active": active,
        "started_from": _parse_iso8601(from_, field_name="from"),
        "started_to": _parse_iso8601(to, field_name="to"),
        "connector_id": connector_id,
        "stop_reason": stop_reason,
        "stopped_from": _parse_iso8601(stopped_after, field_name="stopped_after"),
        "stopped_to": _parse_iso8601(stopped_before, field_name="stopped_before"),
        "min_consumed_wh": min_consumed_wh,
        "max_consumed_wh": max_consumed_wh,
        "sort": sort,
        "direction": dir or "desc",
    }

    if page is not None:
        effective_size = clamp_limit(
            page_size if page_size is not None else limit,
            default=settings.rest_default_page_size,
            maximum=settings.rest_max_page_size,
        )
        offset = offset_for_page(page, effective_size)
        # `count_transactions` doesn't care about ordering — strip the
        # sort kwargs before the count call to avoid forwarding kwargs
        # that aren't in its signature.
        count_kwargs = {k: v for k, v in filter_kwargs.items() if k not in {"sort", "direction"}}
        async with session_scope(request.app.state.session_factory) as session:
            rows = await list_transactions(
                session,
                after_id=None,
                limit=effective_size,
                offset=offset,
                **filter_kwargs,
            )
            total = await count_transactions(session, **count_kwargs)
        return {
            "transactions": [_transaction_to_response(tx) for tx in rows],
            "pagination": pagination_block(page=page, page_size=effective_size, total=total),
            "request_id": request.state.request_id,
        }

    effective_size = clamp_limit(
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
        rows = await list_transactions(
            session,
            after_id=after_id,
            limit=effective_size,
            **filter_kwargs,
        )

    has_more = len(rows) > effective_size
    page_rows = rows[:effective_size]
    next_cursor: str | None = None
    if has_more and page_rows:
        next_cursor = encode_cursor({"id": page_rows[-1]["id"]})

    return {
        "transactions": [_transaction_to_response(tx) for tx in page_rows],
        "next_cursor": next_cursor,
        "request_id": request.state.request_id,
    }


_AGGREGATE_MAX_WINDOW = timedelta(days=90)
_BUCKETS = {"hour", "day"}
_GROUP_BY = {"none", "cp_id", "id_tag"}


@router.get(
    "/transactions/aggregate",
    summary="Bucketed analytics over completed transactions",
    responses={
        200: {"model": TransactionsAggregateResponse},
        400: {
            "model": ErrorEnvelope,
            "description": "Bad from/to, unknown bucket / group_by, or window too large.",
        },
    },
)
async def aggregate_transactions_route(
    request: Request,
    from_: str = Query(alias="from"),
    to: str = Query(...),
    bucket: str = Query(default="day"),
    group_by: str = Query(default="none"),
) -> dict[str, Any]:
    """Bucketed energy + duration totals over completed sessions.

    Window cap is 90 days; the aggregation is one SQL pass over
    ``transactions`` with ``date_trunc`` for the time bucket and
    optional ``cp_id`` / ``id_tag`` split. Active sessions are
    excluded (no ``stopped_reported_at`` → no totals to add).

    Buckets are UTC. The optional ``group`` field is set on every
    row when ``group_by != 'none'`` and omitted otherwise."""
    started_from = _parse_iso8601(from_, field_name="from")
    started_to = _parse_iso8601(to, field_name="to")
    if started_from is None or started_to is None:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="`from` and `to` are required",
        )
    if started_to < started_from:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="`to` must be >= `from`",
        )
    if started_to - started_from > _AGGREGATE_MAX_WINDOW:
        raise ApiError(
            status_code=400,
            error_code=ERR_WINDOW_TOO_LARGE,
            message=f"window cannot exceed {_AGGREGATE_MAX_WINDOW.days} days",
        )
    if bucket not in _BUCKETS:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"unknown bucket: {bucket!r}",
        )
    if group_by not in _GROUP_BY:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"unknown group_by: {group_by!r}",
        )

    async with session_scope(request.app.state.session_factory) as session:
        rows = await aggregate_transactions(
            session,
            window_from=started_from,
            window_to=started_to,
            bucket=bucket,
            group_by=group_by,
        )

    return {
        "buckets": [_aggregate_to_response(r) for r in rows],
        "window": {
            "from": _isoformat(started_from),
            "to": _isoformat(started_to),
            "seconds": int((started_to - started_from).total_seconds()),
            "bucket": bucket,
            "group_by": group_by,
        },
        "request_id": request.state.request_id,
    }


def _aggregate_to_response(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "bucket_at": _isoformat(row["bucket_at"]),
        "session_count": int(row["session_count"]),
        "consumed_wh_total": int(row["consumed_wh_total"]),
        "duration_seconds_total": int(row["duration_seconds_total"]),
    }
    if "group" in row:
        out["group"] = row["group"]
    return out


@router.get(
    "/transactions/{transaction_id}",
    summary="Single-transaction detail",
    responses={
        200: {"model": TransactionDetail},
        404: {"model": ErrorEnvelope, "description": "Unknown transaction_id."},
    },
)
async def get_transaction_route(request: Request, transaction_id: int) -> dict[str, Any]:
    async with session_scope(request.app.state.session_factory) as session:
        tx = await get_transaction_by_id(session, transaction_id=transaction_id)
    if tx is None:
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_TRANSACTION_ID,
            message=f"unknown transaction_id: {transaction_id}",
        )
    response = _transaction_to_response(tx)
    response["telemetry"] = await _telemetry_for(request, tx)
    response["request_id"] = request.state.request_id
    return response


@router.get(
    "/transactions/{transaction_id}/frames",
    summary="OCPP frame audit trail for one transaction",
    responses={
        200: {"model": OcppFramesByTransactionResponse},
        404: {"model": ErrorEnvelope, "description": "Unknown transaction_id."},
    },
)
async def get_transaction_frames_route(
    request: Request,
    transaction_id: int,
    limit: int | None = Query(default=None, ge=1, le=10_000),
) -> dict[str, Any]:
    """Every OCPP frame stamped with the given ``transaction_id`` —
    both directions, ordered by ``occurred_at``. No time window
    required: transactions are already bounded.

    404 when the transaction_id doesn't exist in the OLTP store. An
    existing transaction with zero frames returns ``frames: []`` (a
    fresh tx that hasn't received MeterValues yet)."""
    async with session_scope(request.app.state.session_factory) as session:
        tx = await get_transaction_by_id(session, transaction_id=transaction_id)
    if tx is None:
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_TRANSACTION_ID,
            message=f"unknown transaction_id: {transaction_id}",
        )

    settings = request.app.state.settings
    page_size = clamp_limit(
        limit,
        default=settings.rest_default_page_size,
        maximum=settings.rest_max_page_size,
    )

    ch_client = getattr(request.app.state, "ch_client", None)
    if ch_client is None:
        # Same posture as get_transaction_route's telemetry: tests and
        # compose-smoke run without ClickHouse — surface frames=[]
        # instead of 500ing.
        rows: list[dict[str, Any]] = []
    else:
        rows = await ch_client.fetch_frames_by_transaction(
            transaction_id=transaction_id,
            limit=page_size,
        )

    return {
        "transaction_id": transaction_id,
        "frames": [_frame_to_response(r) for r in rows],
        "request_id": request.state.request_id,
    }


def _frame_to_response(row: dict[str, Any]) -> dict[str, Any]:
    """Project a `cp_ocpp_frames` row into the API shape. Mirrors the
    same helper in `api/timeseries.py`; duplicated here so the two
    routers stay independent (the import direction would otherwise
    be backwards)."""
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


async def _telemetry_for(request: Request, tx: dict[str, Any]) -> dict[str, Any] | None:
    """Fetch the per-transaction telemetry snapshot, or `None` when the
    gateway has no ClickHouse read client wired in.

    Compose-smoke and unit-test apps run without ClickHouse; surface
    `telemetry: null` in those environments rather than 500ing on a
    detail call.

    Shapes the CH client's internal field names into the wire form
    consumed by the UI:
      `soc.start_pct` → `soc.start`, `soc.last_pct` → `soc.last`,
      plus a derived `soc.delta = last - start`.
      Per-phase `last_at` → `occurred_at`, plus a null `power_factor`
      column so the UI's PhaseSnapshot type has every field present.
    """
    ch_client = getattr(request.app.state, "ch_client", None)
    if ch_client is None:
        return None
    raw: dict[str, Any] = await ch_client.fetch_transaction_telemetry(
        cp_id=tx["cp_id"],
        transaction_id=tx["transaction_id"],
    )
    raw_soc = raw.get("soc") or {}
    start = raw_soc.get("start_pct")
    last = raw_soc.get("last_pct")
    delta: float | None = (last - start) if start is not None and last is not None else None
    soc = {"start": start, "last": last, "delta": delta}

    phases: dict[str, dict[str, Any]] = {}
    for phase, snap in (raw.get("phases") or {}).items():
        phases[phase] = {
            "voltage_v": snap.get("voltage_v"),
            "current_a": snap.get("current_a"),
            "power_w": snap.get("power_w"),
            "power_factor": snap.get("power_factor"),
            "occurred_at": snap.get("last_at"),
        }
    return {"soc": soc, "phases": phases}
