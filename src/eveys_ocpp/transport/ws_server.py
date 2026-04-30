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

    from eveys_ocpp.connections import ConnectionMap
    from eveys_ocpp.events import EventProducer
    from eveys_ocpp.registry import Registry
    from eveys_ocpp.settings import Settings

log = get_logger(__name__)

OCPP_SUBPROTOCOL = Subprotocol("ocpp1.6")


async def _on_connect(
    connection: ServerConnection,
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    registry: Registry | None,
    connections: ConnectionMap | None,
    event_producer: EventProducer | None = None,
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

    if registry is not None:
        await registry.mark_online(cp_id)

    cp = EveysChargePoint(
        cp_id,
        connection,
        session_factory=session_factory,
        settings=settings,
        registry=registry,
        event_producer=event_producer,
    )
    if connections is not None:
        connections.add(cp)
    try:
        await cp.start()
    finally:
        if connections is not None:
            connections.remove(cp)
        if registry is not None:
            # Compare-and-delete: only clear if we still own the key.
            # A reconnect to a different pod between disconnect and
            # this call must not clobber the new owner.
            await registry.mark_offline(cp_id)
        log.info("ws.disconnected")
        clear_contextvars()


async def serve_forever(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    registry: Registry | None = None,
    connections: ConnectionMap | None = None,
    event_producer: EventProducer | None = None,
) -> None:
    """Start the WS server and block until cancelled.

    `registry`, `connections`, and `event_producer` are all optional so
    unit tests + the W1-style local stack can skip Redis / Kafka /
    in-process routing. Production wiring (`__main__.py`) always passes
    all three — `connections` is how gRPC RemoteStart finds the live WS,
    `event_producer` is how MeterValues + future events reach Kafka,
    and `registry` is how cross-pod ownership is tracked.
    """

    async def handler(connection: ServerConnection) -> None:
        await _on_connect(
            connection,
            session_factory=session_factory,
            settings=settings,
            registry=registry,
            connections=connections,
            event_producer=event_producer,
        )

    async with serve(
        handler,
        host=settings.ws_host,
        port=settings.ws_port,
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as server:
        log.info("ws.listening", host=settings.ws_host, port=settings.ws_port)
        await server.serve_forever()
