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
from dataclasses import asdict, is_dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from grpclib.const import Status
from grpclib.exceptions import GRPCError
from grpclib.server import Server, Stream
from ocpp.v16 import call as ocpp_call
from ocpp.v16 import enums as ocpp_enums

from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc, gateway_pb2
from eveys_ocpp.bus import BusReply, CommandBus
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


# Owning-side dispatch table: rpc name -> ocpp.v16.call dataclass.
# The owning pod uses this to reconstruct the OCPP request from a bus
# payload dict. The dataclass field names are the wire schema: anything
# the requesting side puts in `payload` must match a constructor kwarg.
# Adding a new gRPC command means adding one row here AND a translator
# below — same pattern as before, just centralized.
_OCPP_CALL_DISPATCH: dict[str, type[Any]] = {
    "RemoteStart": ocpp_call.RemoteStartTransaction,
    "RemoteStop": ocpp_call.RemoteStopTransaction,
    "Reset": ocpp_call.Reset,
    "ChangeConfiguration": ocpp_call.ChangeConfiguration,
    "TriggerMessage": ocpp_call.TriggerMessage,
    "UnlockConnector": ocpp_call.UnlockConnector,
    "GetConfiguration": ocpp_call.GetConfiguration,
    "ClearCache": ocpp_call.ClearCache,
    "DataTransfer": ocpp_call.DataTransfer,
}


class OcppGatewayService(gateway_grpc.OcppGatewayBase):
    """Implementation of `OcppGateway`. All seven RPCs live here."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        connections: ConnectionMap | None = None,
        registry: Registry | None = None,
        bus: CommandBus | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.settings = settings
        self.connections = connections
        self.registry = registry
        self.bus = bus
        # Wire owning-side dispatch into the bus so cross-pod requests
        # for chargers connected here actually run. Done here (not in
        # bus.__init__) to avoid a circular import: bus -> grpc_server.
        if bus is not None:
            bus.set_local_dispatcher(self._dispatch_local_for_bus)

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

    # ---- E2-1A — OCPP 1.6 Core completion -----------------------------------

    async def GetConfiguration(
        self,
        stream: Stream[gateway_pb2.GetConfigurationRequest, gateway_pb2.GetConfigurationResponse],
    ) -> None:
        """Read configuration keys from the charger.

        Empty `keys` means "everything" per OCPP 1.6 spec — we forward
        that semantic verbatim by passing `None` (the OCPP dataclass
        default) when the caller didn't list any. A populated list
        gets forwarded; the charger is responsible for echoing
        unknown keys in `unknown_key`.
        """
        request = await self._recv(stream)
        keys: list[str] | None = list(request.keys) if request.keys else None
        ocpp_response = await self._dispatch_ocpp_call(
            rpc="GetConfiguration",
            cp_id=request.cp_id,
            ocpp_request=ocpp_call.GetConfiguration(key=keys),
        )
        await stream.send_message(
            gateway_pb2.GetConfigurationResponse(
                configuration_key=[
                    _translate_configuration_key(item)
                    for item in (ocpp_response.configuration_key or [])
                ],
                unknown_key=list(ocpp_response.unknown_key or []),
            )
        )

    async def ClearCache(
        self,
        stream: Stream[gateway_pb2.ClearCacheRequest, gateway_pb2.ClearCacheResponse],
    ) -> None:
        """Wipe the charger's local Authorize cache."""
        request = await self._recv(stream)
        ocpp_response = await self._dispatch_ocpp_call(
            rpc="ClearCache",
            cp_id=request.cp_id,
            ocpp_request=ocpp_call.ClearCache(),
        )
        await stream.send_message(
            gateway_pb2.ClearCacheResponse(
                status=_translate_clear_cache_status(ocpp_response.status)
            )
        )

    async def DataTransfer(
        self,
        stream: Stream[gateway_pb2.DataTransferRequest, gateway_pb2.DataTransferResponse],
    ) -> None:
        """Send a vendor-specific DataTransfer payload to the charger.

        OCPP 1.6 spec requires `vendor_id`; the gateway enforces it at
        the boundary so a malformed call is rejected before reaching
        the charger (which would just respond `UnknownVendorId`).
        """
        request = await self._recv(stream)
        if not request.vendor_id:
            raise GRPCError(
                Status.INVALID_ARGUMENT,
                "vendor_id is required (OCPP DataTransfer namespaces all vendor traffic)",
            )
        ocpp_response = await self._dispatch_ocpp_call(
            rpc="DataTransfer",
            cp_id=request.cp_id,
            ocpp_request=ocpp_call.DataTransfer(
                vendor_id=request.vendor_id,
                message_id=request.message_id or None,
                data=request.data or None,
            ),
        )
        await stream.send_message(
            gateway_pb2.DataTransferResponse(
                status=_translate_data_transfer_status(ocpp_response.status),
                data=ocpp_response.data or "",
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
        ocpp_request: Any,
    ) -> Any:
        """Send the OCPP request, return the reply.

        Routing:
        - cp on this pod → call directly (existing same-pod path)
        - cp on a different pod, bus configured → forward via CommandBus
        - cp on a different pod, no bus → UNAVAILABLE (test fallback)
        - cp offline → NOT_FOUND
        - empty cp_id → INVALID_ARGUMENT

        Returns either an ocpp.v16 call_result dataclass (same-pod) or a
        lightweight stand-in object exposing ``.status`` (cross-pod). The
        existing translators only read ``.status`` so both shapes work.
        """
        if not cp_id:
            raise GRPCError(Status.INVALID_ARGUMENT, "cp_id is required")

        bind_contextvars(rpc=rpc, cp_id=cp_id, direction="rx")
        try:
            cp = self.connections.get(cp_id) if self.connections is not None else None
            if cp is not None:
                return await self._call_local_cp(cp, rpc=rpc, ocpp_request=ocpp_request)

            owning_pod: str | None = None
            if self.registry is not None:
                owning_pod = await self.registry.get_pod(cp_id)

            if owning_pod is None:
                raise GRPCError(Status.NOT_FOUND, f"charger {cp_id} is offline")

            if self.bus is None:
                # Bus not wired (test fixture or single-pod deployment) —
                # preserve the pre-E2-10 behaviour so callers see a stable
                # error rather than a confusing TIMEOUT.
                raise GRPCError(
                    Status.UNAVAILABLE,
                    (
                        f"charger {cp_id} is on pod {owning_pod}; "
                        "cross-pod bus not configured on this gateway"
                    ),
                )

            return await self._call_via_bus(
                rpc=rpc, cp_id=cp_id, owning_pod=owning_pod, ocpp_request=ocpp_request
            )
        finally:
            clear_contextvars()

    async def _call_local_cp(self, cp: EveysChargePoint, *, rpc: str, ocpp_request: Any) -> Any:
        log.info("grpc.dispatch", rpc=rpc, route="local")
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

    async def _call_via_bus(
        self, *, rpc: str, cp_id: str, owning_pod: str, ocpp_request: Any
    ) -> Any:
        """Round-trip the OCPP request through the cross-pod bus."""
        assert self.bus is not None  # narrowed by caller
        log.info("grpc.dispatch", rpc=rpc, route="bus", owning_pod=owning_pod)
        reply = await self.bus.request(
            cp_id=cp_id,
            owning_pod=owning_pod,
            rpc=rpc,
            payload=asdict(ocpp_request),
            timeout=_OCPP_REQUEST_TIMEOUT_SECONDS,
        )
        if reply.ok:
            log.info("grpc.replied", rpc=rpc, ocpp_status=reply.ocpp_status, route="bus")
            # If the owning side serialised the full response (E2-1A:
            # GetConfiguration, DataTransfer), reconstruct a duck-typed
            # object the per-RPC body's translator can read field by
            # field. Otherwise keep the legacy status-only stand-in
            # for the simple-status RPCs.
            if reply.ocpp_response is not None:
                return SimpleNamespace(**reply.ocpp_response)
            return _BusOcppResponse(status=reply.ocpp_status or "")

        log.warning(
            "grpc.bus_error",
            rpc=rpc,
            error_code=reply.error_code,
            error_message=reply.error_message,
        )
        gstatus = _BUS_ERROR_TO_GRPC_STATUS.get(reply.error_code or "", Status.UNAVAILABLE)
        raise GRPCError(gstatus, reply.error_message or "cross-pod dispatch failed")

    async def _dispatch_local_for_bus(
        self, rpc: str, cp_id: str, payload: dict[str, Any]
    ) -> BusReply:
        """Owning-side: handle a bus command for a charger connected here.

        Reconstruct the OCPP dataclass from ``payload``, run the round-trip,
        return a ``BusReply`` for the bus to publish back to the requester.
        """
        cp = self.connections.get(cp_id) if self.connections is not None else None
        if cp is None:
            # Charger disconnected between the requester's registry read
            # and this dispatch — not an error, just a race.
            return BusReply(ok=False, error_code="NOT_FOUND", error_message="charger went offline")

        dataclass_type = _OCPP_CALL_DISPATCH.get(rpc)
        if dataclass_type is None:
            return BusReply(
                ok=False,
                error_code="INTERNAL",
                error_message=f"unknown rpc on owning pod: {rpc}",
            )

        try:
            ocpp_request = dataclass_type(**payload)
        except TypeError as exc:
            return BusReply(
                ok=False,
                error_code="INTERNAL",
                error_message=f"payload mismatch for {rpc}: {exc}",
            )

        try:
            ocpp_response = await asyncio.wait_for(
                cp.call(ocpp_request),
                timeout=_OCPP_REQUEST_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            return BusReply(
                ok=False,
                error_code="DEADLINE_EXCEEDED",
                error_message=f"charger did not reply within {_OCPP_REQUEST_TIMEOUT_SECONDS}s",
            )
        # Serialise the full response payload so RPCs whose reply has
        # more than just a status (GetConfiguration's lists,
        # DataTransfer's optional `data`) round-trip correctly across
        # the bus. ``asdict`` handles every ocpp.v16.call_result
        # variant (all dataclasses); the requester reconstructs a
        # duck-typed object. The ``not isinstance(..., type)`` clause
        # narrows mypy: ``is_dataclass`` is True for both instances
        # and classes, but ``asdict`` only accepts instances.
        ocpp_response_dict: dict[str, Any] | None = (
            asdict(ocpp_response)
            if is_dataclass(ocpp_response) and not isinstance(ocpp_response, type)
            else None
        )
        return BusReply(
            ok=True,
            ocpp_status=getattr(ocpp_response, "status", ""),
            ocpp_response=ocpp_response_dict,
        )


# ---- bus-mode plumbing ------------------------------------------------------


class _BusOcppResponse:
    """Stand-in for an ocpp.v16 call_result with just `.status`.

    Cross-pod replies don't carry a full OCPP dataclass — they carry the
    status string, which is all the existing translators read. This lets
    the per-RPC method bodies stay identical for local and bus paths.
    """

    __slots__ = ("status",)

    def __init__(self, *, status: str) -> None:
        self.status = status


# Bus error_code -> gRPC Status. Mirror of the same set the local path raises.
_BUS_ERROR_TO_GRPC_STATUS: dict[str, Status] = {
    "NOT_FOUND": Status.NOT_FOUND,
    "DEADLINE_EXCEEDED": Status.DEADLINE_EXCEEDED,
    "INTERNAL": Status.INTERNAL,
}


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


def _translate_configuration_key(item: dict[str, Any]) -> gateway_pb2.ConfigurationKey:
    """OCPP returns each configuration key as a dict per the JSON
    schema. Translate to the typed proto message.

    `value` is required by the OCPP schema but the dataclass marks it
    optional; coerce a missing value to empty string at the gateway
    boundary (callers don't want a `null` here).
    """
    return gateway_pb2.ConfigurationKey(
        key=item.get("key", ""),
        readonly=bool(item.get("readonly", False)),
        value=item.get("value") or "",
    )


def _translate_clear_cache_status(ocpp_status: str) -> int:
    if ocpp_status == "Accepted":
        return gateway_pb2.CLEAR_CACHE_STATUS_ACCEPTED
    if ocpp_status == "Rejected":
        return gateway_pb2.CLEAR_CACHE_STATUS_REJECTED
    log.warning("grpc.unknown_ocpp_status", rpc="ClearCache", ocpp_status=ocpp_status)
    return gateway_pb2.CLEAR_CACHE_STATUS_UNSPECIFIED


def _translate_data_transfer_status(ocpp_status: str) -> int:
    if ocpp_status == "Accepted":
        return gateway_pb2.DATA_TRANSFER_STATUS_ACCEPTED
    if ocpp_status == "Rejected":
        return gateway_pb2.DATA_TRANSFER_STATUS_REJECTED
    if ocpp_status == "UnknownMessageId":
        return gateway_pb2.DATA_TRANSFER_STATUS_UNKNOWN_MESSAGE_ID
    if ocpp_status == "UnknownVendorId":
        return gateway_pb2.DATA_TRANSFER_STATUS_UNKNOWN_VENDOR_ID
    log.warning("grpc.unknown_ocpp_status", rpc="DataTransfer", ocpp_status=ocpp_status)
    return gateway_pb2.DATA_TRANSFER_STATUS_UNSPECIFIED


# -----------------------------------------------------------------------------


async def serve_forever(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    connections: ConnectionMap | None = None,
    registry: Registry | None = None,
    bus: CommandBus | None = None,
) -> None:
    """Start the gRPC server and block until cancelled."""
    service = OcppGatewayService(
        session_factory=session_factory,
        settings=settings,
        connections=connections,
        registry=registry,
        bus=bus,
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
