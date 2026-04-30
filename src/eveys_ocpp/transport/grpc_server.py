"""gRPC server (E2-4 scaffold + E2-5 RemoteStart + E2-6 remaining commands).

Stands up the platform-facing command surface defined in
`proto/ocpp_gw/v1/gateway.proto`.

Routing model for charger-targeted RPCs:
1. Look up the charger in the in-process `ConnectionMap` (live WS
   on this pod).
2. If not on this pod, consult the Redis `Registry` to see whether
   any pod owns the WS:
   - No pod → reply `NOT_FOUND` ("charger offline").
   - Different pod → reply `UNAVAILABLE` for now; cross-pod routing
     ships with E2-10 (Redis pub/sub command bus). The error message
     names the actual pod so callers can re-target.
3. If on this pod, send the OCPP request over the WS, await the
   charger's reply, translate to gRPC.

A 30s ceiling on the OCPP round-trip prevents a flaky charger from
tying up the gRPC slot indefinitely. gRPC clients can also set a
shorter deadline via grpclib; whichever fires first wins.

`GetChargerStatus` is the odd one out: no OCPP round-trip. It answers
from local registry + Postgres caches, so it is fast and safe to call
from operator dashboards without back-pressuring chargers.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from grpclib.const import Status
from grpclib.exceptions import GRPCError
from grpclib.server import Server, Stream
from ocpp.v16 import call as ocpp_call
from ocpp.v16 import enums as ocpp_enums

from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc, gateway_pb2
from eveys_ocpp.observability import bind_contextvars, clear_contextvars, get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import get_charge_point_status

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.connection import EveysChargePoint
    from eveys_ocpp.connections import ConnectionMap
    from eveys_ocpp.registry import Registry
    from eveys_ocpp.settings import Settings

log = get_logger(__name__)

# Cap on how long we'll wait for an OCPP charger to reply before giving up
# on a CSMS-initiated request. OCPP 1.6 doesn't mandate a value; chargers
# routinely take a couple of seconds. 30s is generous enough for slow
# devices and tight enough to bound a gRPC slot on a flaky charger.
_OCPP_REQUEST_TIMEOUT_SECONDS = 30.0


class OcppGatewayService(gateway_grpc.OcppGatewayBase):
    """Implementation of `OcppGateway`. All seven RPCs live here."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        connections: ConnectionMap | None = None,
        registry: Registry | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.settings = settings
        self.connections = connections
        self.registry = registry

    # ---- E2-5 RemoteStart ---------------------------------------------------

    async def RemoteStart(
        self,
        stream: Stream[gateway_pb2.RemoteStartRequest, gateway_pb2.RemoteStartResponse],
    ) -> None:
        request = await self._recv(stream)
        ocpp_response = await self._dispatch_ocpp_call(
            rpc="RemoteStart",
            cp_id=request.cp_id,
            ocpp_request=ocpp_call.RemoteStartTransaction(
                id_tag=request.id_tag,
                connector_id=request.connector_id or None,
            ),
        )
        await stream.send_message(
            gateway_pb2.RemoteStartResponse(
                status=_translate_remote_start_status(ocpp_response.status)
            )
        )

    # ---- E2-6 remaining commands -------------------------------------------

    async def RemoteStop(
        self,
        stream: Stream[gateway_pb2.RemoteStopRequest, gateway_pb2.RemoteStopResponse],
    ) -> None:
        request = await self._recv(stream)
        ocpp_response = await self._dispatch_ocpp_call(
            rpc="RemoteStop",
            cp_id=request.cp_id,
            ocpp_request=ocpp_call.RemoteStopTransaction(transaction_id=request.transaction_id),
        )
        await stream.send_message(
            gateway_pb2.RemoteStopResponse(
                status=_translate_remote_stop_status(ocpp_response.status)
            )
        )

    async def Reset(
        self,
        stream: Stream[gateway_pb2.ResetRequest, gateway_pb2.ResetResponse],
    ) -> None:
        request = await self._recv(stream)
        reset_type = _translate_reset_type_to_ocpp(request.type)
        ocpp_response = await self._dispatch_ocpp_call(
            rpc="Reset",
            cp_id=request.cp_id,
            ocpp_request=ocpp_call.Reset(type=reset_type),
        )
        await stream.send_message(
            gateway_pb2.ResetResponse(status=_translate_reset_status(ocpp_response.status))
        )

    async def ChangeConfiguration(
        self,
        stream: Stream[
            gateway_pb2.ChangeConfigurationRequest, gateway_pb2.ChangeConfigurationResponse
        ],
    ) -> None:
        request = await self._recv(stream)
        if not request.key:
            raise GRPCError(Status.INVALID_ARGUMENT, "key is required")
        ocpp_response = await self._dispatch_ocpp_call(
            rpc="ChangeConfiguration",
            cp_id=request.cp_id,
            ocpp_request=ocpp_call.ChangeConfiguration(key=request.key, value=request.value),
        )
        await stream.send_message(
            gateway_pb2.ChangeConfigurationResponse(
                status=_translate_change_configuration_status(ocpp_response.status)
            )
        )

    async def TriggerMessage(
        self,
        stream: Stream[gateway_pb2.TriggerMessageRequest, gateway_pb2.TriggerMessageResponse],
    ) -> None:
        request = await self._recv(stream)
        requested = _translate_trigger_message_to_ocpp(request.requested_message)
        ocpp_response = await self._dispatch_ocpp_call(
            rpc="TriggerMessage",
            cp_id=request.cp_id,
            ocpp_request=ocpp_call.TriggerMessage(
                requested_message=requested,
                connector_id=request.connector_id or None,
            ),
        )
        await stream.send_message(
            gateway_pb2.TriggerMessageResponse(
                status=_translate_trigger_message_status(ocpp_response.status)
            )
        )

    async def UnlockConnector(
        self,
        stream: Stream[gateway_pb2.UnlockConnectorRequest, gateway_pb2.UnlockConnectorResponse],
    ) -> None:
        request = await self._recv(stream)
        if request.connector_id <= 0:
            raise GRPCError(
                Status.INVALID_ARGUMENT,
                "connector_id must be > 0 (OCPP UnlockConnector requires a specific connector)",
            )
        ocpp_response = await self._dispatch_ocpp_call(
            rpc="UnlockConnector",
            cp_id=request.cp_id,
            ocpp_request=ocpp_call.UnlockConnector(connector_id=request.connector_id),
        )
        await stream.send_message(
            gateway_pb2.UnlockConnectorResponse(
                status=_translate_unlock_connector_status(ocpp_response.status)
            )
        )

    async def GetChargerStatus(
        self,
        stream: Stream[gateway_pb2.GetChargerStatusRequest, gateway_pb2.GetChargerStatusResponse],
    ) -> None:
        """Read-only: no OCPP round-trip. Answers from registry + Postgres."""
        request = await self._recv(stream)
        if not request.cp_id:
            raise GRPCError(Status.INVALID_ARGUMENT, "cp_id is required")

        bind_contextvars(rpc="GetChargerStatus", cp_id=request.cp_id, direction="rx")
        try:
            owning_pod = await self.registry.get_pod(request.cp_id) if self.registry else None
            online = owning_pod is not None

            last_status = ""
            last_heartbeat_at = ""
            async with session_scope(self.session_factory) as session:
                row = await get_charge_point_status(session, cp_id=request.cp_id)
            if row is not None:
                status_str, hb_at = row
                if status_str is not None:
                    last_status = status_str
                if hb_at is not None:
                    last_heartbeat_at = hb_at.isoformat()

            log.info("grpc.get_charger_status", online=online, owning_pod=owning_pod or "")
            await stream.send_message(
                gateway_pb2.GetChargerStatusResponse(
                    cp_id=request.cp_id,
                    online=online,
                    pod_id=owning_pod or "",
                    last_status=last_status,
                    last_heartbeat_at=last_heartbeat_at,
                )
            )
        finally:
            clear_contextvars()

    # ---- shared helpers -----------------------------------------------------

    @staticmethod
    async def _recv(stream: Stream[Any, Any]) -> Any:
        request = await stream.recv_message()
        if request is None:
            raise GRPCError(Status.INVALID_ARGUMENT, "no request body")
        return request

    async def _dispatch_ocpp_call(
        self,
        *,
        rpc: str,
        cp_id: str,
        ocpp_request: object,
    ) -> Any:
        """Resolve cp on this pod, send the OCPP request, return the reply.

        Raises `GRPCError` for the four non-happy paths:
        - INVALID_ARGUMENT: empty cp_id
        - NOT_FOUND: charger offline (no registry key)
        - UNAVAILABLE: charger online but on a different pod (E2-10)
        - DEADLINE_EXCEEDED: charger on this pod but didn't reply in 30s
        """
        bind_contextvars(rpc=rpc, cp_id=cp_id, direction="rx")
        try:
            cp = await self._resolve_local_cp(cp_id)
            log.info("grpc.dispatch", rpc=rpc)
            try:
                ocpp_response = await asyncio.wait_for(
                    cp.call(ocpp_request),
                    timeout=_OCPP_REQUEST_TIMEOUT_SECONDS,
                )
            except TimeoutError as exc:
                log.warning("grpc.timeout", rpc=rpc)
                raise GRPCError(
                    Status.DEADLINE_EXCEEDED,
                    f"charger did not reply within {_OCPP_REQUEST_TIMEOUT_SECONDS}s",
                ) from exc
            log.info("grpc.replied", rpc=rpc, ocpp_status=getattr(ocpp_response, "status", None))
            return ocpp_response
        finally:
            clear_contextvars()

    async def _resolve_local_cp(self, cp_id: str) -> EveysChargePoint:
        """Find the live WS for `cp_id` on this pod, or raise the right gRPC error.

        Three outcomes:
        - On this pod's `ConnectionMap` → returns the EveysChargePoint.
        - On a different pod (Registry says so) → UNAVAILABLE with pod_id
          in the message. Cross-pod routing is E2-10.
        - Nowhere → NOT_FOUND ("charger is offline").
        """
        if not cp_id:
            raise GRPCError(Status.INVALID_ARGUMENT, "cp_id is required")

        if self.connections is not None:
            cp = self.connections.get(cp_id)
            if cp is not None:
                return cp

        # Not on this pod — but Registry might still know where it is.
        owning_pod: str | None = None
        if self.registry is not None:
            owning_pod = await self.registry.get_pod(cp_id)

        if owning_pod is None:
            raise GRPCError(
                Status.NOT_FOUND,
                f"charger {cp_id} is offline",
            )

        # Charger is online but on a different pod.
        raise GRPCError(
            Status.UNAVAILABLE,
            (
                f"charger {cp_id} is on pod {owning_pod}; "
                "cross-pod routing is task E2-10. Retry against that pod, "
                "or send the request from a client that hashes on cp_id."
            ),
        )


# ---- proto-enum / OCPP-string translators -----------------------------------
#
# OCPP 1.6 statuses are PascalCase strings on the wire; the proto enums use
# the canonical *_UPPER_SNAKE_CASE shape. UNSPECIFIED catches any vendor
# extension we don't recognize so a flaky firmware can't crash the dispatcher.


def _translate_remote_start_status(ocpp_status: str) -> int:
    if ocpp_status == "Accepted":
        return gateway_pb2.REMOTE_START_STATUS_ACCEPTED
    if ocpp_status == "Rejected":
        return gateway_pb2.REMOTE_START_STATUS_REJECTED
    log.warning("grpc.unknown_ocpp_status", rpc="RemoteStart", ocpp_status=ocpp_status)
    return gateway_pb2.REMOTE_START_STATUS_UNSPECIFIED


def _translate_remote_stop_status(ocpp_status: str) -> int:
    if ocpp_status == "Accepted":
        return gateway_pb2.REMOTE_STOP_STATUS_ACCEPTED
    if ocpp_status == "Rejected":
        return gateway_pb2.REMOTE_STOP_STATUS_REJECTED
    log.warning("grpc.unknown_ocpp_status", rpc="RemoteStop", ocpp_status=ocpp_status)
    return gateway_pb2.REMOTE_STOP_STATUS_UNSPECIFIED


def _translate_reset_status(ocpp_status: str) -> int:
    if ocpp_status == "Accepted":
        return gateway_pb2.RESET_STATUS_ACCEPTED
    if ocpp_status == "Rejected":
        return gateway_pb2.RESET_STATUS_REJECTED
    log.warning("grpc.unknown_ocpp_status", rpc="Reset", ocpp_status=ocpp_status)
    return gateway_pb2.RESET_STATUS_UNSPECIFIED


def _translate_reset_type_to_ocpp(proto_type: int) -> ocpp_enums.ResetType:
    """Proto enum → OCPP `Reset.req.type` enum."""
    if proto_type == gateway_pb2.RESET_TYPE_HARD:
        return ocpp_enums.ResetType.hard
    if proto_type == gateway_pb2.RESET_TYPE_SOFT:
        return ocpp_enums.ResetType.soft
    raise GRPCError(Status.INVALID_ARGUMENT, "type must be RESET_TYPE_HARD or RESET_TYPE_SOFT")


def _translate_change_configuration_status(ocpp_status: str) -> int:
    if ocpp_status == "Accepted":
        return gateway_pb2.CHANGE_CONFIGURATION_STATUS_ACCEPTED
    if ocpp_status == "Rejected":
        return gateway_pb2.CHANGE_CONFIGURATION_STATUS_REJECTED
    if ocpp_status == "RebootRequired":
        return gateway_pb2.CHANGE_CONFIGURATION_STATUS_REBOOT_REQUIRED
    if ocpp_status == "NotSupported":
        return gateway_pb2.CHANGE_CONFIGURATION_STATUS_NOT_SUPPORTED
    log.warning("grpc.unknown_ocpp_status", rpc="ChangeConfiguration", ocpp_status=ocpp_status)
    return gateway_pb2.CHANGE_CONFIGURATION_STATUS_UNSPECIFIED


def _translate_trigger_message_to_ocpp(proto_kind: int) -> ocpp_enums.MessageTrigger:
    """Proto TriggerMessageType → OCPP `MessageTrigger` enum.

    OCPP 1.6 §6.51 enum values: BootNotification, DiagnosticsStatusNotification,
    FirmwareStatusNotification, Heartbeat, MeterValues, StatusNotification.
    """
    mapping: dict[int, ocpp_enums.MessageTrigger] = {
        gateway_pb2.TRIGGER_MESSAGE_TYPE_BOOT_NOTIFICATION: (
            ocpp_enums.MessageTrigger.boot_notification
        ),
        gateway_pb2.TRIGGER_MESSAGE_TYPE_DIAGNOSTICS_STATUS_NOTIFICATION: (
            ocpp_enums.MessageTrigger.diagnostics_status_notification
        ),
        gateway_pb2.TRIGGER_MESSAGE_TYPE_FIRMWARE_STATUS_NOTIFICATION: (
            ocpp_enums.MessageTrigger.firmware_status_notification
        ),
        gateway_pb2.TRIGGER_MESSAGE_TYPE_HEARTBEAT: ocpp_enums.MessageTrigger.heartbeat,
        gateway_pb2.TRIGGER_MESSAGE_TYPE_METER_VALUES: ocpp_enums.MessageTrigger.meter_values,
        gateway_pb2.TRIGGER_MESSAGE_TYPE_STATUS_NOTIFICATION: (
            ocpp_enums.MessageTrigger.status_notification
        ),
    }
    if proto_kind not in mapping:
        raise GRPCError(
            Status.INVALID_ARGUMENT,
            "requested_message must be a defined TriggerMessageType (not UNSPECIFIED)",
        )
    return mapping[proto_kind]


def _translate_trigger_message_status(ocpp_status: str) -> int:
    if ocpp_status == "Accepted":
        return gateway_pb2.TRIGGER_MESSAGE_STATUS_ACCEPTED
    if ocpp_status == "Rejected":
        return gateway_pb2.TRIGGER_MESSAGE_STATUS_REJECTED
    if ocpp_status == "NotImplemented":
        return gateway_pb2.TRIGGER_MESSAGE_STATUS_NOT_IMPLEMENTED
    log.warning("grpc.unknown_ocpp_status", rpc="TriggerMessage", ocpp_status=ocpp_status)
    return gateway_pb2.TRIGGER_MESSAGE_STATUS_UNSPECIFIED


def _translate_unlock_connector_status(ocpp_status: str) -> int:
    if ocpp_status == "Unlocked":
        return gateway_pb2.UNLOCK_CONNECTOR_STATUS_UNLOCKED
    if ocpp_status == "UnlockFailed":
        return gateway_pb2.UNLOCK_CONNECTOR_STATUS_UNLOCK_FAILED
    if ocpp_status == "NotSupported":
        return gateway_pb2.UNLOCK_CONNECTOR_STATUS_NOT_SUPPORTED
    log.warning("grpc.unknown_ocpp_status", rpc="UnlockConnector", ocpp_status=ocpp_status)
    return gateway_pb2.UNLOCK_CONNECTOR_STATUS_UNSPECIFIED


# -----------------------------------------------------------------------------


async def serve_forever(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    connections: ConnectionMap | None = None,
    registry: Registry | None = None,
) -> None:
    """Start the gRPC server and block until cancelled."""
    service = OcppGatewayService(
        session_factory=session_factory,
        settings=settings,
        connections=connections,
        registry=registry,
    )
    server = Server([service])
    await server.start(host=settings.grpc_host, port=settings.grpc_port)
    log.info("grpc.listening", host=settings.grpc_host, port=settings.grpc_port)
    try:
        await server.wait_closed()
    finally:
        log.info("grpc.shutdown")
        server.close()
        await server.wait_closed()


# Re-export for tests.
__all__ = ["OcppGatewayService", "gateway_grpc", "gateway_pb2", "serve_forever"]
