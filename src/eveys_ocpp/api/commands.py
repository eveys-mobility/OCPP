"""`POST /api/v1/charge-points/{cp_id}/commands/*` routes (E3-8).

Each route is a thin HTTP wrapper around the same OCPP dispatcher the
gRPC service uses. The dispatcher (see `_commands.py`) handles charger
lookup, cross-pod routing, and the 30-second OCPP round-trip timeout;
these routes own only:

- request body parsing (JSON → OCPP dataclass),
- response shape (OCPP dataclass → JSON),
- side-effect ordering for the five mutating commands (SendLocalList,
  ReserveNow, CancelReservation, SetChargingProfile, ClearChargingProfile)
  whose Postgres mirror writes must follow charger ACCEPTED.

The contract for paths, request bodies, response bodies, and error
codes is `docs/integration/02-gateway-rest-api.md` § "Command endpoints".
ADR-0026 records the framing decisions.

The five mutating commands intentionally duplicate a small amount of
mirror-write logic from `transport/grpc_server.py`. We keep that
duplication local-and-visible rather than extracting a shared
"MirrorWriter" service: the duplication is ~80 lines total and a
future incident report will show a missed mirror write more readily
when it lives next to the route that triggers it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request
from ocpp.v16 import call as ocpp_call
from ocpp.v16 import enums as ocpp_enums

from eveys_ocpp.api._commands import dispatch_ocpp_call
from eveys_ocpp.api._errors import (
    ERR_BAD_REQUEST,
    ERR_UNKNOWN_CP_ID,
    ApiError,
)
from eveys_ocpp.api._schemas import (
    CommandAcceptedResponse,
    ErrorEnvelope,
    RemoteStartRequest,
    RemoteStopRequest,
    ResetRequest,
)
from eveys_ocpp.observability import get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import (
    activate_reservation,
    apply_local_auth_list_differential,
    clear_charging_profiles,
    delete_reservation,
    get_charge_point_status,
    insert_pending_reservation,
    replace_local_auth_list,
    upsert_charging_profile,
)
from eveys_ocpp.persistence.repositories import (
    cancel_reservation as repo_cancel_reservation,
)

if TYPE_CHECKING:
    pass

log = get_logger(__name__)

router = APIRouter(tags=["commands"])

# Path prefix for every command route. Spelled out as a constant so a
# rename (e.g. moving to /api/v1/charge-points/{cp_id}/cmd/*) is a
# single-line edit.
_BASE = "/charge-points/{cp_id}/commands"


# ---- helpers ---------------------------------------------------------------


async def _body(request: Request) -> dict[str, Any]:
    """Parse the JSON body or 400. An empty body is accepted (some
    commands have no required fields)."""
    if int(request.headers.get("content-length") or 0) == 0:
        return {}
    try:
        data = await request.json()
    except ValueError as exc:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"invalid JSON body: {exc}",
        ) from exc
    if not isinstance(data, dict):
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="body must be a JSON object",
        )
    return data


def _require(body: dict[str, Any], key: str) -> Any:
    if key not in body or body[key] in (None, ""):
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"{key} is required",
        )
    return body[key]


def _as_int(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"{field} must be an integer",
        )
    return int(value)


def _ok(request: Request, status: str, **extra: Any) -> dict[str, Any]:
    """Standard response envelope: status + request_id + any per-RPC
    extras the contract calls for (`reservation_id`, `file_name`, etc.)."""
    out: dict[str, Any] = {"status": status, "request_id": request.state.request_id}
    out.update(extra)
    return out


# ---- Core remote control ---------------------------------------------------

# OpenAPI annotation note: the routes below parse the raw JSON body
# themselves (via `_body(request)`) rather than declaring a typed body
# parameter, so FastAPI can't auto-derive the request schema. We attach
# a `responses=` map and an `openapi_extra={"requestBody": ...}` blob
# pointing at the matching Pydantic model. That populates Swagger UI
# with both the example and the schema without changing the runtime
# parsing path. (Switching to typed body params is a future-PR cleanup
# — it'd also tighten validation, but it's a larger surface change.)


def _request_body_schema(model: type[Any]) -> dict[str, Any]:
    """Build an OpenAPI `requestBody` blob that references the given
    Pydantic model by name. FastAPI emits the model into `components/
    schemas` as a side effect of the `responses=` reference, so the
    `$ref` resolves on the generated OpenAPI document."""
    return {
        "content": {
            "application/json": {
                "schema": {"$ref": f"#/components/schemas/{model.__name__}"},
            }
        },
        "required": True,
    }


_COMMAND_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {"model": CommandAcceptedResponse},
    404: {"model": ErrorEnvelope, "description": "Unknown cp_id."},
    503: {"model": ErrorEnvelope, "description": "Charger offline."},
    504: {
        "model": ErrorEnvelope,
        "description": "Charger did not reply within the per-RPC timeout.",
    },
}


@router.post(
    _BASE + "/remote-start",
    summary="Tell a charger to start a session for an RFID tag",
    responses=_COMMAND_RESPONSES,
    openapi_extra={"requestBody": _request_body_schema(RemoteStartRequest)},
)
async def remote_start(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    id_tag = _require(body, "id_tag")
    connector_id = body.get("connector_id")
    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="RemoteStart",
        cp_id=cp_id,
        ocpp_request=ocpp_call.RemoteStartTransaction(
            id_tag=str(id_tag),
            connector_id=int(connector_id) if connector_id else None,
        ),
    )
    return _ok(request, ocpp_response.status)


@router.post(
    _BASE + "/remote-stop",
    summary="Tell a charger to stop an in-progress session",
    responses=_COMMAND_RESPONSES,
    openapi_extra={"requestBody": _request_body_schema(RemoteStopRequest)},
)
async def remote_stop(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    transaction_id = _as_int(_require(body, "transaction_id"), field="transaction_id")
    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="RemoteStop",
        cp_id=cp_id,
        ocpp_request=ocpp_call.RemoteStopTransaction(transaction_id=transaction_id),
    )
    return _ok(request, ocpp_response.status)


@router.post(
    _BASE + "/reset",
    summary="Soft or Hard reset of the charger",
    responses=_COMMAND_RESPONSES,
    openapi_extra={"requestBody": _request_body_schema(ResetRequest)},
)
async def reset(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    raw_type = str(_require(body, "type"))
    if raw_type not in ("Soft", "Hard"):
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="type must be 'Soft' or 'Hard'",
        )
    ocpp_type = ocpp_enums.ResetType.soft if raw_type == "Soft" else ocpp_enums.ResetType.hard
    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="Reset",
        cp_id=cp_id,
        ocpp_request=ocpp_call.Reset(type=ocpp_type),
    )
    return _ok(request, ocpp_response.status)


# ---- Configuration ---------------------------------------------------------


@router.post(_BASE + "/change-configuration")
async def change_configuration(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    key = str(_require(body, "key"))
    value = str(body.get("value") or "")
    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="ChangeConfiguration",
        cp_id=cp_id,
        ocpp_request=ocpp_call.ChangeConfiguration(key=key, value=value),
    )
    return _ok(request, ocpp_response.status)


@router.post(_BASE + "/get-configuration")
async def get_configuration(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    raw_keys = body.get("keys")
    keys: list[str] | None
    if raw_keys is None:
        keys = None
    elif isinstance(raw_keys, list):
        keys = [str(k) for k in raw_keys] if raw_keys else None
    else:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="keys must be a list of strings (or omitted)",
        )
    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="GetConfiguration",
        cp_id=cp_id,
        ocpp_request=ocpp_call.GetConfiguration(key=keys),
    )
    configuration_key = list(getattr(ocpp_response, "configuration_key", None) or [])
    unknown_key = list(getattr(ocpp_response, "unknown_key", None) or [])
    return {
        "configuration_key": configuration_key,
        "unknown_key": unknown_key,
        "request_id": request.state.request_id,
    }


@router.post(_BASE + "/clear-cache")
async def clear_cache(request: Request, cp_id: str) -> dict[str, Any]:
    await _body(request)  # accepts {} or empty body
    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="ClearCache",
        cp_id=cp_id,
        ocpp_request=ocpp_call.ClearCache(),
    )
    return _ok(request, ocpp_response.status)


@router.post(_BASE + "/trigger-message")
async def trigger_message(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    raw_msg = str(_require(body, "requested_message"))
    try:
        requested = ocpp_enums.MessageTrigger(raw_msg)
    except ValueError as exc:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=(
                "requested_message must be one of: BootNotification, "
                "DiagnosticsStatusNotification, FirmwareStatusNotification, "
                "Heartbeat, MeterValues, StatusNotification"
            ),
        ) from exc
    connector_id = body.get("connector_id")
    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="TriggerMessage",
        cp_id=cp_id,
        ocpp_request=ocpp_call.TriggerMessage(
            requested_message=requested,
            connector_id=int(connector_id) if connector_id else None,
        ),
    )
    return _ok(request, ocpp_response.status)


@router.post(_BASE + "/unlock-connector")
async def unlock_connector(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    connector_id = _as_int(_require(body, "connector_id"), field="connector_id")
    if connector_id <= 0:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="connector_id must be > 0 (UnlockConnector targets a specific connector)",
        )
    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="UnlockConnector",
        cp_id=cp_id,
        ocpp_request=ocpp_call.UnlockConnector(connector_id=connector_id),
    )
    return _ok(request, ocpp_response.status)


# ---- Vendor extension -------------------------------------------------------


@router.post(_BASE + "/data-transfer")
async def data_transfer(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    vendor_id = str(_require(body, "vendor_id"))
    message_id = body.get("message_id")
    data = body.get("data")
    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="DataTransfer",
        cp_id=cp_id,
        ocpp_request=ocpp_call.DataTransfer(
            vendor_id=vendor_id,
            message_id=str(message_id) if message_id else None,
            data=str(data) if data else None,
        ),
    )
    # OCPP DataTransfer's response carries an optional `data` echo back
    # alongside `status`. Surface both verbatim — vendor-specific
    # callers depend on the data field being in the response.
    return {
        "status": ocpp_response.status,
        "data": getattr(ocpp_response, "data", None),
        "request_id": request.state.request_id,
    }


# ---- Local Authorization List (E2-1B) --------------------------------------


@router.post(_BASE + "/get-local-list-version")
async def get_local_list_version(request: Request, cp_id: str) -> dict[str, Any]:
    await _body(request)
    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="GetLocalListVersion",
        cp_id=cp_id,
        ocpp_request=ocpp_call.GetLocalListVersion(),
    )
    return {
        "list_version": int(getattr(ocpp_response, "list_version", -1)),
        "request_id": request.state.request_id,
    }


@router.post(_BASE + "/send-local-list")
async def send_local_list(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    list_version = _as_int(_require(body, "list_version"), field="list_version")
    raw_update_type = str(_require(body, "update_type"))
    if raw_update_type not in ("Full", "Differential"):
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="update_type must be 'Full' or 'Differential'",
        )
    update_type = (
        ocpp_enums.UpdateType.full
        if raw_update_type == "Full"
        else ocpp_enums.UpdateType.differential
    )
    raw_entries = body.get("local_authorization_list") or []
    if not isinstance(raw_entries, list):
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="local_authorization_list must be a list",
        )
    ocpp_entries: list[dict[str, Any]] = []
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, dict) or not raw_entry.get("id_tag"):
            raise ApiError(
                status_code=400,
                error_code=ERR_BAD_REQUEST,
                message="every local_authorization_list entry must have an id_tag",
            )
        info = raw_entry.get("id_tag_info")
        ocpp_entries.append(
            {
                "id_tag": str(raw_entry["id_tag"]),
                "id_tag_info": dict(info) if isinstance(info, dict) else None,
            }
        )

    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="SendLocalList",
        cp_id=cp_id,
        ocpp_request=ocpp_call.SendLocalList(
            list_version=list_version,
            update_type=update_type,
            local_authorization_list=ocpp_entries,
        ),
    )

    # Mirror writes mirror grpc_server.py:387-417: only on Accepted, and
    # never promote a persistence failure to a request error (the
    # charger now has the list; rejecting would mislead the caller).
    if ocpp_response.status == "Accepted":
        try:
            session_factory = request.app.state.session_factory
            async with session_scope(session_factory) as session:
                if raw_update_type == "Full":
                    await replace_local_auth_list(
                        session,
                        cp_id=cp_id,
                        list_version=list_version,
                        entries=ocpp_entries,
                        full_replace_at=datetime.now(UTC),
                    )
                else:
                    await apply_local_auth_list_differential(
                        session,
                        cp_id=cp_id,
                        list_version=list_version,
                        entries=ocpp_entries,
                    )
        except Exception as exc:
            log.exception(
                "rest.send_local_list.persist_failed",
                cp_id=cp_id,
                error=str(exc),
            )

    return _ok(request, ocpp_response.status)


# ---- Reservations (E2-1C, ADR-0021) ----------------------------------------


@router.post(_BASE + "/reserve-now")
async def reserve_now(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    connector_id = _as_int(_require(body, "connector_id"), field="connector_id")
    id_tag = str(_require(body, "id_tag"))
    expiry_date_raw = str(_require(body, "expiry_date"))
    parent_id_tag = body.get("parent_id_tag")

    try:
        expiry_dt = datetime.fromisoformat(expiry_date_raw)
    except ValueError as exc:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"expiry_date must be ISO-8601: {exc}",
        ) from exc
    if expiry_dt.tzinfo is None:
        expiry_dt = expiry_dt.replace(tzinfo=UTC)

    # Allocate the reservation_id by inserting a Pending row (ADR-0021).
    # Mirror of grpc_server.py:454-463.
    session_factory = request.app.state.session_factory
    async with session_scope(session_factory) as session:
        reservation_id = await insert_pending_reservation(
            session,
            cp_id=cp_id,
            connector_id=connector_id,
            id_tag=id_tag,
            parent_id_tag=str(parent_id_tag) if parent_id_tag else None,
            expiry_date=expiry_dt,
        )

    try:
        ocpp_response = await dispatch_ocpp_call(
            request,
            rpc="ReserveNow",
            cp_id=cp_id,
            ocpp_request=ocpp_call.ReserveNow(
                connector_id=connector_id,
                expiry_date=expiry_date_raw,
                id_tag=id_tag,
                reservation_id=reservation_id,
                parent_id_tag=str(parent_id_tag) if parent_id_tag else None,
            ),
        )
    except BaseException:
        # Charger never replied — roll back the Pending row so it
        # doesn't pollute the operator's view (mirror of
        # grpc_server.py:477-490).
        try:
            async with session_scope(session_factory) as session:
                await delete_reservation(session, reservation_id=reservation_id)
        except Exception as cleanup_exc:
            log.exception(
                "rest.reserve_now.rollback_failed",
                reservation_id=reservation_id,
                error=str(cleanup_exc),
            )
        raise

    if ocpp_response.status == "Accepted":
        try:
            async with session_scope(session_factory) as session:
                await activate_reservation(session, reservation_id=reservation_id)
        except Exception as exc:
            log.exception(
                "rest.reserve_now.activate_failed",
                reservation_id=reservation_id,
                error=str(exc),
            )
    else:
        # Charger refused — drop the Pending row.
        try:
            async with session_scope(session_factory) as session:
                await delete_reservation(session, reservation_id=reservation_id)
        except Exception as exc:
            log.exception(
                "rest.reserve_now.cleanup_failed",
                reservation_id=reservation_id,
                error=str(exc),
            )

    return _ok(request, ocpp_response.status, reservation_id=reservation_id)


@router.post(_BASE + "/cancel-reservation")
async def cancel_reservation_route(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    reservation_id = _as_int(_require(body, "reservation_id"), field="reservation_id")
    if reservation_id <= 0:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="reservation_id must be > 0",
        )
    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="CancelReservation",
        cp_id=cp_id,
        ocpp_request=ocpp_call.CancelReservation(reservation_id=reservation_id),
    )

    if ocpp_response.status == "Accepted":
        try:
            session_factory = request.app.state.session_factory
            async with session_scope(session_factory) as session:
                await repo_cancel_reservation(session, reservation_id=reservation_id)
        except Exception as exc:
            log.exception(
                "rest.cancel_reservation.persist_failed",
                reservation_id=reservation_id,
                error=str(exc),
            )

    return _ok(request, ocpp_response.status)


# ---- FirmwareManagement (E2-1F) --------------------------------------------


@router.post(_BASE + "/get-diagnostics")
async def get_diagnostics(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    location = str(_require(body, "location"))
    retries = body.get("retries")
    retry_interval = body.get("retry_interval")
    start_time = body.get("start_time")
    stop_time = body.get("stop_time")
    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="GetDiagnostics",
        cp_id=cp_id,
        ocpp_request=ocpp_call.GetDiagnostics(
            location=location,
            retries=int(retries) if retries else None,
            retry_interval=int(retry_interval) if retry_interval else None,
            start_time=str(start_time) if start_time else None,
            stop_time=str(stop_time) if stop_time else None,
        ),
    )
    return {
        "file_name": getattr(ocpp_response, "file_name", None) or "",
        "request_id": request.state.request_id,
    }


@router.post(_BASE + "/update-firmware")
async def update_firmware(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    location = str(_require(body, "location"))
    retrieve_date = str(_require(body, "retrieve_date"))
    retries = body.get("retries")
    retry_interval = body.get("retry_interval")
    await dispatch_ocpp_call(
        request,
        rpc="UpdateFirmware",
        cp_id=cp_id,
        ocpp_request=ocpp_call.UpdateFirmware(
            location=location,
            retrieve_date=retrieve_date,
            retries=int(retries) if retries else None,
            retry_interval=int(retry_interval) if retry_interval else None,
        ),
    )
    # OCPP UpdateFirmware.conf is empty — the request_id is the only
    # field we surface. Operators learn whether the rollout succeeded
    # via inbound FirmwareStatusNotification (last_firmware_status).
    return {"request_id": request.state.request_id}


# ---- Smart Charging (E2-1E, ADR-0022) --------------------------------------


def _parse_charging_profile(
    body: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Pull the OCPP charging profile out of a request body.

    Body shape mirrors the proto's ChargingProfile message in lower
    snake_case (see docs/integration/02-gateway-rest-api.md). Returns
    (profile_dict, period_dicts) ready for `upsert_charging_profile`.
    """
    profile = body.get("charging_profile")
    if not isinstance(profile, dict):
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="charging_profile object is required",
        )

    schedule = profile.get("charging_schedule")
    if not isinstance(schedule, dict):
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="charging_profile.charging_schedule object is required",
        )

    raw_periods = schedule.get("charging_schedule_period") or []
    if not isinstance(raw_periods, list) or not raw_periods:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=(
                "charging_profile.charging_schedule.charging_schedule_period "
                "must be a non-empty list"
            ),
        )

    period_dicts: list[dict[str, Any]] = []
    for p in raw_periods:
        if not isinstance(p, dict):
            raise ApiError(
                status_code=400,
                error_code=ERR_BAD_REQUEST,
                message="each charging_schedule_period must be an object",
            )
        period_dicts.append(
            {
                "start_period": int(p.get("start_period") or 0),
                "limit": float(_require(p, "limit")),
                "number_phases": (int(p["number_phases"]) if p.get("number_phases") else None),
            }
        )

    profile_dict: dict[str, Any] = {
        "charging_profile_id": _as_int(
            _require(profile, "charging_profile_id"), field="charging_profile_id"
        ),
        "stack_level": int(profile.get("stack_level") or 0),
        "charging_profile_purpose": str(_require(profile, "charging_profile_purpose")),
        "charging_profile_kind": str(_require(profile, "charging_profile_kind")),
        "transaction_id": (
            int(profile["transaction_id"]) if profile.get("transaction_id") else None
        ),
        "recurrency_kind": (
            str(profile["recurrency_kind"]) if profile.get("recurrency_kind") else None
        ),
        "valid_from": str(profile["valid_from"]) if profile.get("valid_from") else None,
        "valid_to": str(profile["valid_to"]) if profile.get("valid_to") else None,
        "charging_schedule": {
            "duration": int(schedule["duration"]) if schedule.get("duration") else None,
            "charging_rate_unit": str(schedule.get("charging_rate_unit") or "W"),
            "min_charging_rate": (
                float(schedule["min_charging_rate"]) if schedule.get("min_charging_rate") else None
            ),
            "start_schedule": (
                str(schedule["start_schedule"]) if schedule.get("start_schedule") else None
            ),
        },
    }
    return profile_dict, period_dicts


def _build_ocpp_charging_profile(
    profile_dict: dict[str, Any], period_dicts: list[dict[str, Any]]
) -> dict[str, Any]:
    """Compose the dict the OCPP library expects for `cs_charging_profiles`.
    Mirror of `grpc_server._build_ocpp_charging_profile`."""
    schedule_dict = dict(profile_dict["charging_schedule"])
    schedule_dict["charging_schedule_period"] = period_dicts
    return {
        "charging_profile_id": profile_dict["charging_profile_id"],
        "stack_level": profile_dict["stack_level"],
        "charging_profile_purpose": profile_dict["charging_profile_purpose"],
        "charging_profile_kind": profile_dict["charging_profile_kind"],
        "charging_schedule": schedule_dict,
        "transaction_id": profile_dict["transaction_id"],
        "recurrency_kind": profile_dict["recurrency_kind"],
        "valid_from": profile_dict["valid_from"],
        "valid_to": profile_dict["valid_to"],
    }


@router.post(_BASE + "/set-charging-profile")
async def set_charging_profile(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    connector_id = _as_int(_require(body, "connector_id"), field="connector_id")
    profile_dict, period_dicts = _parse_charging_profile(body)

    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="SetChargingProfile",
        cp_id=cp_id,
        ocpp_request=ocpp_call.SetChargingProfile(
            connector_id=connector_id,
            cs_charging_profiles=_build_ocpp_charging_profile(profile_dict, period_dicts),
        ),
    )

    if ocpp_response.status == "Accepted":
        try:
            session_factory = request.app.state.session_factory
            async with session_scope(session_factory) as session:
                await upsert_charging_profile(
                    session,
                    cp_id=cp_id,
                    connector_id=connector_id,
                    profile=profile_dict,
                    schedule_periods=period_dicts,
                )
        except Exception as exc:
            log.exception(
                "rest.set_charging_profile.persist_failed",
                cp_id=cp_id,
                error=str(exc),
            )

    return _ok(request, ocpp_response.status)


@router.post(_BASE + "/clear-charging-profile")
async def clear_charging_profile(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    profile_id = body.get("charging_profile_id")
    connector_id = body.get("connector_id")
    purpose = body.get("purpose")
    stack_level = body.get("stack_level")

    purpose_enum: ocpp_enums.ChargingProfilePurposeType | None = None
    if purpose is not None:
        try:
            purpose_enum = ocpp_enums.ChargingProfilePurposeType(str(purpose))
        except ValueError as exc:
            raise ApiError(
                status_code=400,
                error_code=ERR_BAD_REQUEST,
                message=(
                    "purpose must be one of: ChargePointMaxProfile, TxDefaultProfile, TxProfile"
                ),
            ) from exc

    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="ClearChargingProfile",
        cp_id=cp_id,
        ocpp_request=ocpp_call.ClearChargingProfile(
            id=int(profile_id) if profile_id else None,
            connector_id=int(connector_id) if connector_id else None,
            charging_profile_purpose=purpose_enum,
            stack_level=int(stack_level) if stack_level else None,
        ),
    )

    if ocpp_response.status == "Accepted":
        try:
            session_factory = request.app.state.session_factory
            async with session_scope(session_factory) as session:
                await clear_charging_profiles(
                    session,
                    cp_id=cp_id,
                    profile_id=int(profile_id) if profile_id else None,
                    connector_id=int(connector_id) if connector_id else None,
                    purpose=purpose_enum.value if purpose_enum is not None else None,
                    stack_level=int(stack_level) if stack_level else None,
                )
        except Exception as exc:
            log.exception(
                "rest.clear_charging_profile.persist_failed",
                cp_id=cp_id,
                error=str(exc),
            )

    return _ok(request, ocpp_response.status)


@router.post(_BASE + "/get-composite-schedule")
async def get_composite_schedule(request: Request, cp_id: str) -> dict[str, Any]:
    body = await _body(request)
    connector_id = _as_int(_require(body, "connector_id"), field="connector_id")
    duration = _as_int(_require(body, "duration"), field="duration")
    if duration <= 0:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="duration must be > 0",
        )
    raw_unit = body.get("charging_rate_unit")
    rate_unit_arg: ocpp_enums.ChargingRateUnitType | None = None
    if raw_unit is not None:
        if raw_unit == "W":
            rate_unit_arg = ocpp_enums.ChargingRateUnitType.watts
        elif raw_unit == "A":
            rate_unit_arg = ocpp_enums.ChargingRateUnitType.amps
        else:
            raise ApiError(
                status_code=400,
                error_code=ERR_BAD_REQUEST,
                message="charging_rate_unit must be 'W' or 'A'",
            )

    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="GetCompositeSchedule",
        cp_id=cp_id,
        ocpp_request=ocpp_call.GetCompositeSchedule(
            connector_id=connector_id,
            duration=duration,
            charging_rate_unit=rate_unit_arg,
        ),
    )

    schedule = getattr(ocpp_response, "charging_schedule", None) or {}
    if not isinstance(schedule, dict):
        schedule = {}
    return {
        "status": ocpp_response.status,
        "connector_id": int(getattr(ocpp_response, "connector_id", connector_id) or 0),
        "schedule_start": getattr(ocpp_response, "schedule_start", None),
        "charging_schedule": {
            "duration": schedule.get("duration"),
            "charging_rate_unit": schedule.get("charging_rate_unit"),
            "charging_schedule_period": schedule.get("charging_schedule_period") or [],
            "min_charging_rate": schedule.get("min_charging_rate"),
            "start_schedule": schedule.get("start_schedule"),
        },
        "request_id": request.state.request_id,
    }


# ---- Read-only (no OCPP round-trip) ----------------------------------------


@router.get(_BASE + "/get-charger-status")
async def get_charger_status(request: Request, cp_id: str) -> dict[str, Any]:
    """Cached charger status from the registry + Postgres. Doesn't dial
    the WebSocket; safe to call from operator dashboards without
    back-pressuring chargers."""
    registry = request.app.state.registry
    owning_pod = await registry.get_pod(cp_id) if registry is not None else None
    online = owning_pod is not None

    last_status: str | None = None
    last_heartbeat_at: str | None = None
    session_factory = request.app.state.session_factory
    async with session_scope(session_factory) as session:
        row = await get_charge_point_status(session, cp_id=cp_id)
    if row is None and not online:
        # Never seen this charger and registry has no record either.
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=f"unknown cp_id: {cp_id}",
        )
    if row is not None:
        status_str, hb_at = row
        last_status = status_str
        last_heartbeat_at = hb_at.isoformat() if hb_at is not None else None

    return {
        "cp_id": cp_id,
        "online": online,
        "pod_id": owning_pod,
        "last_status": last_status,
        "last_heartbeat_at": last_heartbeat_at,
        "request_id": request.state.request_id,
    }
