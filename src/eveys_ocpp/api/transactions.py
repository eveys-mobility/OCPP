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

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Query, Request

from eveys_ocpp.api._errors import (
    ERR_BAD_REQUEST,
    ERR_UNKNOWN_CP_ID,
    ERR_UNKNOWN_TRANSACTION_ID,
    ApiError,
)
from eveys_ocpp.api._pagination import clamp_limit, decode_cursor, encode_cursor
from eveys_ocpp.api._schemas import (
    ErrorEnvelope,
    TransactionDetail,
    TransactionListResponse,
)
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import (
    get_transaction_by_id,
    list_transactions,
    list_transactions_by_cp,
)

router = APIRouter(tags=["transactions"])


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
    """Wire shape — drop the internal surrogate `id`, format datetimes."""
    return {
        "transaction_id": tx["transaction_id"],
        "cp_id": tx.get("cp_id"),  # filled by route when needed
        "connector_id": tx["connector_id"],
        "id_tag": tx["id_tag"],
        "meter_start_wh": tx["meter_start_wh"],
        "meter_stop_wh": tx["meter_stop_wh"],
        "consumed_wh": tx["consumed_wh"],
        "started_reported_at": _isoformat(tx["started_reported_at"]),
        "started_received_at": _isoformat(tx["started_received_at"]),
        "stopped_reported_at": _isoformat(tx["stopped_reported_at"]),
        "stopped_received_at": _isoformat(tx["stopped_received_at"]),
        "stop_reason": tx["stop_reason"],
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
    id_tag: str | None = Query(default=None),
    open: bool | None = Query(default=None),
    from_: str | None = Query(default=None, alias="from"),
    to: str | None = Query(default=None),
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

    started_from = _parse_iso8601(from_, field_name="from")
    started_to = _parse_iso8601(to, field_name="to")

    async with session_scope(request.app.state.session_factory) as session:
        rows = await list_transactions_by_cp(
            session,
            cp_id=cp_id,
            after_id=after_id,
            limit=page_size,
            id_tag=id_tag,
            open_only=open,
            started_from=started_from,
            started_to=started_to,
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
        "transactions": [{**_transaction_to_response(tx), "cp_id": cp_id} for tx in page],
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
            "description": "Bad cursor or unparseable from/to timestamp.",
        },
    },
)
async def list_transactions_global_route(
    request: Request,
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=10_000),
    cp_id: str | None = Query(default=None),
    id_tag: str | None = Query(default=None),
    active: bool | None = Query(default=None),
    from_: str | None = Query(default=None, alias="from"),
    to: str | None = Query(default=None),
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

    started_from = _parse_iso8601(from_, field_name="from")
    started_to = _parse_iso8601(to, field_name="to")

    async with session_scope(request.app.state.session_factory) as session:
        rows = await list_transactions(
            session,
            after_id=after_id,
            limit=page_size,
            cp_id=cp_id,
            id_tag=id_tag,
            active=active,
            started_from=started_from,
            started_to=started_to,
        )

    has_more = len(rows) > page_size
    page = rows[:page_size]
    next_cursor: str | None = None
    if has_more and page:
        next_cursor = encode_cursor({"id": page[-1]["id"]})

    return {
        "transactions": [_transaction_to_response(tx) for tx in page],
        "next_cursor": next_cursor,
        "request_id": request.state.request_id,
    }


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
    response["request_id"] = request.state.request_id
    return response
