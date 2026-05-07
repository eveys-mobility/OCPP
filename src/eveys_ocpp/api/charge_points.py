"""`GET /api/v1/charge-points*` routes (E3-7 commit 2).

Endpoints:
- `GET /charge-points` — cursor-paginated list with `online`/`vendor`
  filters. Postgres backs metadata; Redis backs presence.
- `GET /charge-points/{cp_id}` — single-charger detail with active
  reservations + charging profiles inlined.

Per the contract `docs/integration/02-gateway-rest-api.md`:
- The response is **raw** (top-level *is* the resource), not enveloped.
- `online` and `pod_id` come from the Redis online registry.
- `last_*` fields come from Postgres (charge_points table).
- `connectors[]` carries the most recent StatusNotification per
  connector (sourced from ClickHouse `cp_status`). Empty when no
  StatusNotifications have been recorded yet, or when the gateway
  is running without a ClickHouse client wired (tests, dev laptops).
- `last_status` is kept as a single-string convenience for callers
  that don't need per-connector resolution. For multi-connector
  chargers it is **last-write-wins** across connectors and should
  not be read as the device's current state — use `connectors[]`.
- 404 with `error_code=UNKNOWN_CP_ID` when the charger has never sent
  a BootNotification.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Query, Request

from eveys_ocpp.api._errors import ERR_UNKNOWN_CP_ID, ApiError
from eveys_ocpp.api._pagination import clamp_limit, decode_cursor, encode_cursor
from eveys_ocpp.api._schemas import (
    ChargePointDetail,
    ChargePointListResponse,
    ErrorEnvelope,
)
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import (
    get_charge_point_detail,
    list_charge_points,
)

router = APIRouter(tags=["charge_points"])


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


async def _enrich_with_presence(request: Request, cp_dict: dict[str, Any]) -> dict[str, Any]:
    """Attach `online` + `pod_id` from the Redis registry.

    The Redis-less local stack runs without a registry (tests, dev
    laptops); in that case we report `online=False` and `pod_id=None`.
    Operators see the same shape, just without the presence fields
    populated."""
    registry = request.app.state.registry
    cp_id = cp_dict["cp_id"]
    if registry is None:
        cp_dict["online"] = False
        cp_dict["pod_id"] = None
        return cp_dict
    pod_id = await registry.get_pod(cp_id)
    cp_dict["online"] = pod_id is not None
    cp_dict["pod_id"] = pod_id
    return cp_dict


def _to_response(cp: dict[str, Any]) -> dict[str, Any]:
    """Project the repo-level dict to the wire shape (drop internal
    `id`, format datetimes)."""
    return {
        "cp_id": cp["cp_id"],
        "online": cp["online"],
        "pod_id": cp["pod_id"],
        "vendor": cp["vendor"],
        "model": cp["model"],
        "firmware_version": cp["firmware_version"],
        "serial_number": cp["serial_number"],
        "last_boot_at": _isoformat(cp["last_boot_at"]),
        "last_heartbeat_at": _isoformat(cp["last_heartbeat_at"]),
        "last_status": cp["last_status"],
        "last_diagnostics_status": cp["last_diagnostics_status"],
        "last_firmware_status": cp["last_firmware_status"],
        "connectors": cp.get("connectors", []),
    }


async def _enrich_with_connectors(
    request: Request, cp_dicts: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Attach per-connector status from ClickHouse to every dict.

    Batches all charger ids in one query so a list of N chargers stays
    a single ClickHouse round-trip. Falls back to an empty list per
    charger when the read client isn't wired (dev / unit tests) or the
    query fails — the route still serves the metadata correctly.
    """
    if not cp_dicts:
        return cp_dicts

    client = getattr(request.app.state, "ch_client", None)
    if client is None:
        for cp in cp_dicts:
            cp.setdefault("connectors", [])
        return cp_dicts

    cp_ids = [cp["cp_id"] for cp in cp_dicts]
    try:
        latest_by_cp = await client.fetch_latest_connector_statuses(cp_ids=cp_ids)
    except Exception:  # ClickHouse hiccup must not 500 the metadata path
        latest_by_cp = {}

    for cp in cp_dicts:
        rows = latest_by_cp.get(cp["cp_id"], [])
        cp["connectors"] = [
            {
                "connector_id": r["connector_id"],
                "status": r["status"],
                "error_code": r["error_code"] or None,
                "last_changed_at": _isoformat(r["last_changed_at"]),
            }
            for r in rows
        ]
    return cp_dicts


@router.get(
    "/charge-points",
    summary="List charge points (cursor-paginated)",
    responses={200: {"model": ChargePointListResponse}},
)
async def list_charge_points_route(
    request: Request,
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=10_000),
    online: bool | None = Query(default=None),
    vendor: str | None = Query(default=None),
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
                error_code="BAD_REQUEST",
                message="malformed cursor: missing 'id'",
            )
        after_id = raw_id

    async with session_scope(request.app.state.session_factory) as session:
        rows = await list_charge_points(
            session,
            after_id=after_id,
            limit=page_size,
            vendor=vendor,
        )

    # Detect next page: we asked for limit+1; trim the extra row and
    # set the cursor to the last row's surrogate id.
    has_more = len(rows) > page_size
    page = rows[:page_size]
    next_cursor: str | None = None
    if has_more and page:
        next_cursor = encode_cursor({"id": page[-1]["id"]})

    enriched = [await _enrich_with_presence(request, cp) for cp in page]

    # `online` filter is post-Postgres because presence lives in Redis.
    # This means the page may shrink below `limit` after filtering — a
    # known trade-off documented as acceptable in the spec ("limit is
    # a hint; pages may be shorter").
    if online is not None:
        enriched = [cp for cp in enriched if cp["online"] == online]

    enriched = await _enrich_with_connectors(request, enriched)

    return {
        "charge_points": [_to_response(cp) for cp in enriched],
        "next_cursor": next_cursor,
        "request_id": request.state.request_id,
    }


@router.get(
    "/charge-points/{cp_id}",
    summary="Charge-point detail with active reservations + charging profiles",
    responses={
        200: {"model": ChargePointDetail},
        404: {
            "model": ErrorEnvelope,
            "description": "Unknown cp_id (never sent BootNotification).",
        },
    },
)
async def get_charge_point_route(request: Request, cp_id: str) -> dict[str, Any]:
    async with session_scope(request.app.state.session_factory) as session:
        detail = await get_charge_point_detail(session, cp_id=cp_id)
    if detail is None:
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=f"unknown cp_id: {cp_id}",
        )

    enriched = await _enrich_with_presence(request, detail)
    [enriched] = await _enrich_with_connectors(request, [enriched])
    response = _to_response(enriched)
    response["active_reservations"] = [
        {
            "reservation_id": r["reservation_id"],
            "connector_id": r["connector_id"],
            "id_tag": r["id_tag"],
            "expiry_date": _isoformat(r["expiry_date"]),
            "status": r["status"],
        }
        for r in detail["active_reservations"]
    ]
    response["active_charging_profiles"] = detail["active_charging_profiles"]
    response["request_id"] = request.state.request_id
    return response
