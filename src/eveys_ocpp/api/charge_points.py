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
  is running without a ClickHouse client wired (tests, dev workstations).
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

from eveys_ocpp.api._errors import ERR_BAD_REQUEST, ERR_UNKNOWN_CP_ID, ApiError
from eveys_ocpp.api._pagination import (
    clamp_limit,
    decode_cursor,
    encode_cursor,
    offset_for_page,
    pagination_block,
    reject_mixed_pagination,
)
from eveys_ocpp.api._schemas import (
    ChargePointDetail,
    ChargePointListResponse,
    ErrorEnvelope,
)
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import (
    count_charge_points,
    get_charge_point_detail,
    list_charge_points,
    list_transactions_by_cp,
)

router = APIRouter(tags=["charge_points"])


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


async def _enrich_with_presence(request: Request, cp_dict: dict[str, Any]) -> dict[str, Any]:
    """Attach `online` + `pod_id` from the Redis registry.

    The Redis-less local stack runs without a registry (tests, dev
    workstations); in that case we report `online=False` and `pod_id=None`.
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
        "ocpp_version": cp.get("ocpp_version"),
        "last_boot_at": _isoformat(cp["last_boot_at"]),
        "last_heartbeat_at": _isoformat(cp["last_heartbeat_at"]),
        "last_status": cp["last_status"],
        "last_diagnostics_status": cp["last_diagnostics_status"],
        "last_firmware_status": cp["last_firmware_status"],
        "connectors": cp.get("connectors", []),
        "last_offline_seconds": cp.get("last_offline_seconds"),
        "last_offline_ended_at": _isoformat(cp.get("last_offline_ended_at")),
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


async def _enrich_with_last_offline(
    request: Request, cp_dicts: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Attach `last_offline_seconds` + `last_offline_ended_at` from
    ClickHouse `cp_offline_duration`.

    Same batched read pattern as `_enrich_with_connectors` — one
    round-trip for the whole page. Chargers without any recorded
    outage are left with `last_offline_*` absent (which becomes
    `None` in the response). ClickHouse failure falls back the same
    way: no enrichment, metadata path still serves.
    """
    if not cp_dicts:
        return cp_dicts

    client = getattr(request.app.state, "ch_client", None)
    if client is None:
        return cp_dicts

    cp_ids = [cp["cp_id"] for cp in cp_dicts]
    try:
        latest_by_cp = await client.fetch_latest_offline_durations(cp_ids=cp_ids)
    except Exception:
        latest_by_cp = {}

    for cp in cp_dicts:
        row = latest_by_cp.get(cp["cp_id"])
        if row is None:
            continue
        cp["last_offline_seconds"] = int(row["offline_seconds"])
        cp["last_offline_ended_at"] = row["last_offline_ended_at"]
    return cp_dicts


def _parse_iso8601_or_400(value: str | None, *, field_name: str) -> datetime | None:
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


@router.get(
    "/charge-points",
    summary="List charge points (cursor- or page-paginated)",
    responses={200: {"model": ChargePointListResponse}},
)
async def list_charge_points_route(
    request: Request,
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=10_000),
    page: int | None = Query(default=None, ge=1),
    page_size: int | None = Query(default=None, ge=1, le=10_000),
    online: bool | None = Query(default=None),
    vendor: str | None = Query(default=None),
    model: str | None = Query(default=None),
    firmware_version: str | None = Query(default=None),
    ocpp_version: str | None = Query(default=None),
    last_status: str | None = Query(default=None),
    last_firmware_status: str | None = Query(default=None),
    last_diagnostics_status: str | None = Query(default=None),
    last_log_status: str | None = Query(default=None),
    last_boot_after: str | None = Query(default=None),
    last_boot_before: str | None = Query(default=None),
    last_heartbeat_after: str | None = Query(default=None),
    last_heartbeat_before: str | None = Query(default=None),
    created_after: str | None = Query(default=None),
    created_before: str | None = Query(default=None),
    cp_id_prefix: str | None = Query(default=None),
    cp_id_contains: str | None = Query(default=None),
) -> dict[str, Any]:
    settings = request.app.state.settings
    reject_mixed_pagination(cursor=cursor, page=page)

    # Resolve the online filter against Redis BEFORE the SQL queries
    # so the count + page math stay consistent. `online=true` becomes
    # `cp_id IN (<online_ids>)`; `online=false` becomes the NOT IN.
    # When the registry isn't wired (tests / no-Redis dev), treat
    # every charger as offline (mirroring `_enrich_with_presence`).
    cp_ids_in: list[str] | None = None
    cp_ids_not_in: list[str] | None = None
    if online is not None:
        registry = request.app.state.registry
        if registry is None:
            online_ids: list[str] = []
        else:
            try:
                online_ids = await registry.list_online_ids()
            except Exception:
                online_ids = []
        if online:
            cp_ids_in = online_ids
        else:
            cp_ids_not_in = online_ids

    # Parse every time-window param once. Each may 400 individually.
    filter_kwargs: dict[str, Any] = {
        "vendor": vendor,
        "model": model,
        "firmware_version": firmware_version,
        "ocpp_version": ocpp_version,
        "last_status": last_status,
        "last_firmware_status": last_firmware_status,
        "last_diagnostics_status": last_diagnostics_status,
        "last_log_status": last_log_status,
        "last_boot_after": _parse_iso8601_or_400(last_boot_after, field_name="last_boot_after"),
        "last_boot_before": _parse_iso8601_or_400(last_boot_before, field_name="last_boot_before"),
        "last_heartbeat_after": _parse_iso8601_or_400(
            last_heartbeat_after, field_name="last_heartbeat_after"
        ),
        "last_heartbeat_before": _parse_iso8601_or_400(
            last_heartbeat_before, field_name="last_heartbeat_before"
        ),
        "created_after": _parse_iso8601_or_400(created_after, field_name="created_after"),
        "created_before": _parse_iso8601_or_400(created_before, field_name="created_before"),
        "cp_id_prefix": cp_id_prefix,
        "cp_id_contains": cp_id_contains,
        "cp_ids_in": cp_ids_in,
        "cp_ids_not_in": cp_ids_not_in,
    }

    # Two pagination paths, never both.
    if page is not None:
        effective_size = clamp_limit(
            page_size if page_size is not None else limit,
            default=settings.rest_default_page_size,
            maximum=settings.rest_max_page_size,
        )
        offset = offset_for_page(page, effective_size)
        async with session_scope(request.app.state.session_factory) as session:
            rows = await list_charge_points(
                session,
                after_id=None,
                limit=effective_size,
                offset=offset,
                **filter_kwargs,
            )
            total = await count_charge_points(session, **filter_kwargs)
        enriched = [await _enrich_with_presence(request, cp) for cp in rows]
        # `online` was already pushed into the SQL filter above — no
        # post-page trimming needed.
        enriched = await _enrich_with_connectors(request, enriched)
        enriched = await _enrich_with_last_offline(request, enriched)
        return {
            "charge_points": [_to_response(cp) for cp in enriched],
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
                error_code="BAD_REQUEST",
                message="malformed cursor: missing 'id'",
            )
        after_id = raw_id

    async with session_scope(request.app.state.session_factory) as session:
        rows = await list_charge_points(
            session,
            after_id=after_id,
            limit=effective_size,
            **filter_kwargs,
        )

    # Detect next page: we asked for limit+1; trim the extra row and
    # set the cursor to the last row's surrogate id.
    has_more = len(rows) > effective_size
    page_rows = rows[:effective_size]
    next_cursor: str | None = None
    if has_more and page_rows:
        next_cursor = encode_cursor({"id": page_rows[-1]["id"]})

    enriched = [await _enrich_with_presence(request, cp) for cp in page_rows]

    # `online` was pushed into the SQL filter via cp_ids_in /
    # cp_ids_not_in earlier in this handler, so the page contents
    # already honour the filter — no post-page trim.

    enriched = await _enrich_with_connectors(request, enriched)
    enriched = await _enrich_with_last_offline(request, enriched)

    return {
        "charge_points": [_to_response(cp) for cp in enriched],
        "next_cursor": next_cursor,
        "request_id": request.state.request_id,
    }


async def _fetch_active_sessions(
    request: Request, cp_id: str
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Pull active sessions + latest meter for one charger, both
    best-effort.

    Returns `(active_sessions, latest_meter)`. Either side may be
    empty / None when:
    - no `transactions` rows have `stopped_received_at IS NULL`
      (charger idle);
    - the ClickHouse client isn't wired (dev / tests);
    - the ClickHouse query failed (broker hiccup) — degrade rather
      than 500.

    Active sessions are enriched with the latest meter on their
    connector and an SoC + power snapshot from
    `fetch_transaction_telemetry`. Worst case is N+1 CH queries
    where N = active session count, which is bounded by connector
    count (typically 1, max ~4 for a multi-connector station)."""
    async with session_scope(request.app.state.session_factory) as session:
        rows = await list_transactions_by_cp(
            session,
            cp_id=cp_id,
            after_id=None,
            limit=10,
            open_only=True,
        )
    open_txns = rows or []

    client = getattr(request.app.state, "ch_client", None)
    latest_by_connector: dict[int, dict[str, Any]] = {}
    if client is not None:
        try:
            latest_by_connector = await client.fetch_latest_meter_per_connector(cp_id=cp_id)
        except Exception:
            latest_by_connector = {}

    latest_meter: dict[str, Any] | None = None
    if latest_by_connector:
        best_connector, best_row = max(
            latest_by_connector.items(),
            key=lambda kv: kv[1]["occurred_at"],
        )
        latest_meter = {
            "connector_id": best_connector,
            "energy_wh": best_row["value_wh"],
            "occurred_at": _isoformat(best_row["occurred_at"]),
        }

    sessions: list[dict[str, Any]] = []
    for tx in open_txns:
        connector_id = tx["connector_id"]
        meter = latest_by_connector.get(connector_id)
        energy_consumed: int | None = None
        last_meter_at: str | None = None
        if meter is not None and meter["value_wh"] is not None:
            energy_consumed = int(meter["value_wh"] - tx["meter_start_wh"])
            last_meter_at = _isoformat(meter["occurred_at"])

        soc_pct: float | None = None
        power_w: float | None = None
        if client is not None:
            try:
                telemetry = await client.fetch_transaction_telemetry(
                    cp_id=cp_id,
                    transaction_id=tx["transaction_id"],
                )
            except Exception:
                telemetry = None
            if telemetry is not None:
                soc_pct = telemetry.get("soc", {}).get("last_pct")
                # Power readout: prefer the per-phase snapshot's
                # power_w. Sum across phases that report it — a
                # three-phase charger publishes per phase; a DC
                # charger publishes once on PHASE_UNSPECIFIED which
                # the telemetry fetch already buckets out. Null
                # when no phase reported.
                phase_powers = [
                    snap["power_w"]
                    for snap in telemetry.get("phases", {}).values()
                    if snap.get("power_w") is not None
                ]
                if phase_powers:
                    power_w = float(sum(phase_powers))

        sessions.append(
            {
                "transaction_id": tx["transaction_id"],
                "connector_id": connector_id,
                "id_tag": tx["id_tag"],
                "started_at": _isoformat(tx["started_received_at"]),
                "meter_start_wh": tx["meter_start_wh"],
                "energy_consumed_wh": energy_consumed,
                "last_meter_at": last_meter_at,
                "soc_pct": soc_pct,
                "power_w": power_w,
            }
        )

    return sessions, latest_meter


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
    [enriched] = await _enrich_with_last_offline(request, [enriched])
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
    active_sessions, latest_meter = await _fetch_active_sessions(request, cp_id)
    response["active_sessions"] = active_sessions
    response["latest_meter"] = latest_meter
    response["request_id"] = request.state.request_id
    return response
