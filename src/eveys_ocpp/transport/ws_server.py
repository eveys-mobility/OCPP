"""WebSocket server.

Listens on `EVEYS_OCPP_WS_HOST:EVEYS_OCPP_WS_PORT`. The path component is
the charger ID: `ws://host:port/<cp_id>`. Subprotocol must be `ocpp1.6`.

Auth, per-IP rate limiting, and TLS termination live at the edge (Envoy)
in production. Locally we accept everything.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from websockets import Subprotocol
from websockets.asyncio.server import ServerConnection, serve

from eveys_ocpp.connection import EveysChargePoint
from eveys_ocpp.observability import bind_contextvars, clear_contextvars, get_logger

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.settings import Settings

log = get_logger(__name__)

OCPP_SUBPROTOCOL = Subprotocol("ocpp1.6")


async def _on_connect(
    connection: ServerConnection,
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    """Per-connection coroutine. Lives for the duration of the WS."""
    if connection.subprotocol != OCPP_SUBPROTOCOL:
        log.warning("ws.subprotocol_mismatch", got=connection.subprotocol)
        await connection.close(1002, f"unsupported subprotocol; want {OCPP_SUBPROTOCOL}")
        return

    if connection.request is None:  # defensive — should never happen post-handshake
        await connection.close(1008, "no request handshake")
        return
    cp_id = connection.request.path.strip("/")
    if not cp_id:
        await connection.close(1008, "cp_id missing in URL path")
        return

    bind_contextvars(cp_id=cp_id)
    log.info("ws.connected")

    cp = EveysChargePoint(cp_id, connection, session_factory=session_factory, settings=settings)
    try:
        await cp.start()
    finally:
        log.info("ws.disconnected")
        clear_contextvars()


async def serve_forever(
    *, session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    """Start the WS server and block until cancelled."""

    async def handler(connection: ServerConnection) -> None:
        await _on_connect(connection, session_factory=session_factory, settings=settings)

    async with serve(
        handler,
        host=settings.ws_host,
        port=settings.ws_port,
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as server:
        log.info("ws.listening", host=settings.ws_host, port=settings.ws_port)
        await server.serve_forever()
