"""gRPC server (E2-4 scaffold + E2-5 RemoteStart).

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
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from grpclib.const import Status
from grpclib.exceptions import GRPCError
from grpclib.server import Server, Stream
from ocpp.v16 import call as ocpp_call

from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc, gateway_pb2
from eveys_ocpp.observability import bind_contextvars, clear_contextvars, get_logger

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
    """Implementation of `OcppGateway`.

    `RemoteStart` is wired end-to-end (E2-5). The other six RPCs are
    `UNIMPLEMENTED` placeholders pending E2-6.
    """

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
        request = await stream.recv_message()
        if request is None:
            raise GRPCError(Status.INVALID_ARGUMENT, "no request body")

        bind_contextvars(rpc="RemoteStart", cp_id=request.cp_id, direction="rx")
        try:
            cp = await self._resolve_local_cp(request.cp_id)

            log.info(
                "grpc.remote_start.dispatch",
                id_tag=request.id_tag,
                connector_id=request.connector_id or None,
            )
            try:
                ocpp_response = await asyncio.wait_for(
                    cp.call(
                        ocpp_call.RemoteStartTransaction(
                            id_tag=request.id_tag,
                            connector_id=request.connector_id or None,
                        )
                    ),
                    timeout=_OCPP_REQUEST_TIMEOUT_SECONDS,
                )
            except TimeoutError as exc:
                log.warning("grpc.remote_start.timeout")
                raise GRPCError(
                    Status.DEADLINE_EXCEEDED,
                    f"charger did not reply within {_OCPP_REQUEST_TIMEOUT_SECONDS}s",
                ) from exc

            status_pb = _translate_remote_start_status(ocpp_response.status)
            log.info("grpc.remote_start.replied", ocpp_status=ocpp_response.status)
            await stream.send_message(gateway_pb2.RemoteStartResponse(status=status_pb))
        finally:
            clear_contextvars()

    # ---- placeholders for E2-6 ----------------------------------------------

    async def RemoteStop(self, stream: object) -> None:
        await self._unimplemented("RemoteStop")

    async def Reset(self, stream: object) -> None:
        await self._unimplemented("Reset")

    async def ChangeConfiguration(self, stream: object) -> None:
        await self._unimplemented("ChangeConfiguration")

    async def TriggerMessage(self, stream: object) -> None:
        await self._unimplemented("TriggerMessage")

    async def UnlockConnector(self, stream: object) -> None:
        await self._unimplemented("UnlockConnector")

    async def GetChargerStatus(self, stream: object) -> None:
        await self._unimplemented("GetChargerStatus")

    # ---- shared helpers -----------------------------------------------------

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

    async def _unimplemented(self, rpc: str) -> None:
        bind_contextvars(rpc=rpc, direction="rx")
        log.info("grpc.unimplemented", rpc=rpc)
        clear_contextvars()
        raise GRPCError(
            Status.UNIMPLEMENTED,
            f"{rpc} is scaffolded but not yet implemented (lands E2-6)",
        )


def _translate_remote_start_status(ocpp_status: str) -> int:
    """OCPP 1.6 RemoteStartTransaction.conf.status → proto enum.

    OCPP defines two values: "Accepted" and "Rejected". The proto's
    UNSPECIFIED catches anything we don't recognize so a vendor
    extension can't crash the dispatcher.
    """
    if ocpp_status == "Accepted":
        return gateway_pb2.REMOTE_START_STATUS_ACCEPTED
    if ocpp_status == "Rejected":
        return gateway_pb2.REMOTE_START_STATUS_REJECTED
    log.warning("grpc.remote_start.unknown_ocpp_status", ocpp_status=ocpp_status)
    return gateway_pb2.REMOTE_START_STATUS_UNSPECIFIED


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
