"""Bridge between REST command routes and the gRPC dispatcher (E3-8).

The gRPC service in `transport/grpc_server.py` already implements the
hard parts of every OCPP CSMS-initiated command:

- charger lookup (in-process `ConnectionMap` first, then Redis registry),
- cross-pod routing via the `CommandBus`,
- OCPP-call timeout (30 s),
- Postgres mirror writes for SendLocalList / ReserveNow / CancelReservation
  / SetChargingProfile / ClearChargingProfile.

The REST routes in `commands.py` should not duplicate any of that. This
module exposes one async entry point that the routes call:

    response = await dispatch_ocpp_call(
        request,
        rpc="RemoteStart",
        cp_id="CP_001",
        ocpp_request=ocpp_call.RemoteStartTransaction(id_tag="X"),
    )

Internally it calls `OcppGatewayService._dispatch_ocpp_call` (the
transport-agnostic core) and translates `GRPCError` → `ApiError` per
the contract in `docs/integration/02-gateway-rest-api.md` § "Error
responses".

Side-effects (the mirror writes) are NOT in this module — they live
in the gRPC service handlers because they need to fire on the same
ACCEPTED branch. For the few RPCs with mirror writes, we re-export
the gRPC service's per-RPC method so the REST handler can invoke it
directly. The gRPC method itself isn't transport-coupled; it accepts
a fake-stream-like object.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from grpclib.const import Status
from grpclib.exceptions import GRPCError

from eveys_ocpp.api._errors import (
    ERR_BAD_REQUEST,
    ERR_CHARGER_OFFLINE,
    ERR_CHARGER_TIMEOUT,
    ERR_INTERNAL_ERROR,
    ERR_UNKNOWN_CP_ID,
    ApiError,
)

if TYPE_CHECKING:
    from fastapi import Request


# gRPC Status → (HTTP status, REST error_code). Mirror of the gRPC
# server's outgoing error model, inverted. Order of failure modes:
#
# - INVALID_ARGUMENT → 400 BAD_REQUEST (caller passed a malformed body)
# - NOT_FOUND        → 404 UNKNOWN_CP_ID (charger never booted; no row)
# - UNAVAILABLE      → 503 CHARGER_OFFLINE (registry shows no owning pod;
#                      charger booted at some point but isn't reachable now)
# - DEADLINE_EXCEEDED → 504 CHARGER_TIMEOUT (charger online but didn't reply
#                       within the 30 s ceiling)
# - INTERNAL / anything else → 500 INTERNAL_ERROR
_GRPC_TO_HTTP: dict[Status, tuple[int, str]] = {
    Status.INVALID_ARGUMENT: (400, ERR_BAD_REQUEST),
    Status.NOT_FOUND: (404, ERR_UNKNOWN_CP_ID),
    Status.UNAVAILABLE: (503, ERR_CHARGER_OFFLINE),
    Status.DEADLINE_EXCEEDED: (504, ERR_CHARGER_TIMEOUT),
    Status.INTERNAL: (500, ERR_INTERNAL_ERROR),
}


async def dispatch_ocpp_call(
    request: Request,
    *,
    rpc: str,
    cp_id: str,
    ocpp_request: Any,
) -> Any:
    """Forward an OCPP CALL through the shared gRPC dispatcher.

    Returns whatever the gRPC dispatcher returns: an `ocpp.v16.call_result`
    dataclass (same-pod) or a `SimpleNamespace` reconstructed from the
    bus reply (cross-pod). Both shapes expose `.status` (and any other
    response fields the gRPC handler writes).

    Raises `ApiError` on any dispatcher failure, with HTTP status and
    error_code mapped per the contract.
    """
    service = request.app.state.command_service
    if service is None:  # pragma: no cover — boot-time misconfiguration
        raise ApiError(
            status_code=500,
            error_code=ERR_INTERNAL_ERROR,
            message="command service not configured on this gateway",
        )

    try:
        return await service._dispatch_ocpp_call(rpc=rpc, cp_id=cp_id, ocpp_request=ocpp_request)
    except GRPCError as exc:
        http_status, error_code = _GRPC_TO_HTTP.get(exc.status, (500, ERR_INTERNAL_ERROR))
        # The bus path raises NOT_FOUND for "charger went offline mid-call"
        # too — same code as "never seen". The REST contract treats both as
        # 404 UNKNOWN_CP_ID, which is what the existing mapping does.
        raise ApiError(
            status_code=http_status,
            error_code=error_code,
            message=str(exc.message or "command dispatch failed"),
        ) from exc
