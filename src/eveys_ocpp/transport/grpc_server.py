"""gRPC server scaffolding (E2-4).

Stands up the platform-facing command surface defined in
`proto/ocpp_gw/v1/gateway.proto`. Each RPC is a placeholder today —
real implementations land with E2-5 (RemoteStart) and E2-6 (the rest).

The scaffold proves:
- Generated stubs load.
- Server binds and serves on `EVEYS_OCPP_GRPC_HOST:EVEYS_OCPP_GRPC_PORT`.
- Each RPC routes correctly and returns a well-formed `UNIMPLEMENTED`
  status to the caller.

Why a single class implementing every RPC even when most are stubs:
the gRPC service definition is *one* service with seven methods.
grpclib expects one server class per service. Splitting into per-RPC
handler modules happens behind this class in E2-5/E2-6 — the public
service signature stays one file.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from grpclib.const import Status
from grpclib.exceptions import GRPCError
from grpclib.server import Server

from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc, gateway_pb2
from eveys_ocpp.observability import bind_contextvars, clear_contextvars, get_logger

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


class OcppGatewayService(gateway_grpc.OcppGatewayBase):
    """Placeholder implementation of `OcppGateway`.

    Holds the same handles the WS server holds (session factory, settings)
    so future RPCs can read/write Postgres and route OCPP messages to the
    right pod via Redis (E2-5 + E2-9).
    """

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        self.session_factory = session_factory
        self.settings = settings

    async def RemoteStart(self, stream: object) -> None:
        # Lands with E2-5. See proto/ocpp_gw/v1/gateway.proto.
        await self._unimplemented("RemoteStart")

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

    async def _unimplemented(self, rpc: str) -> None:
        bind_contextvars(rpc=rpc, direction="rx")
        log.info("grpc.unimplemented", rpc=rpc)
        clear_contextvars()
        raise GRPCError(
            Status.UNIMPLEMENTED,
            f"{rpc} is scaffolded but not yet implemented (lands E2-5/E2-6)",
        )


async def serve_forever(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    """Start the gRPC server and block until cancelled."""
    service = OcppGatewayService(session_factory=session_factory, settings=settings)
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
