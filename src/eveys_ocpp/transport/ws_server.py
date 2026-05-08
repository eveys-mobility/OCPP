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
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, clear_contextvars, get_logger

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.connections import ConnectionMap
    from eveys_ocpp.events import EventProducer
    from eveys_ocpp.idempotency import IdempotencyCache
    from eveys_ocpp.platform import AuthorizeCache, BackendHTTPClient
    from eveys_ocpp.registry import Registry
    from eveys_ocpp.settings import Settings
    from eveys_ocpp.transport._rate_limiter import RateLimiter

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
    idempotency: IdempotencyCache | None = None,
    backend_client: BackendHTTPClient | None = None,
    authorize_cache: AuthorizeCache | None = None,
    rate_limiter: RateLimiter | None = None,
) -> None:
    """Per-connection coroutine. Lives for the duration of the WS."""
    if connection.subprotocol != OCPP_SUBPROTOCOL:
        log.warning("ws.subprotocol_mismatch", got=connection.subprotocol)
        metrics_registry.WS_HANDSHAKE_FAILURES_TOTAL.labels(reason="subprotocol").inc()
        await connection.close(1002, f"unsupported subprotocol; want {OCPP_SUBPROTOCOL}")
        return

    if connection.request is None:  # defensive — should never happen post-handshake
        metrics_registry.WS_HANDSHAKE_FAILURES_TOTAL.labels(reason="no_request").inc()
        await connection.close(1008, "no request handshake")
        return
    cp_id = connection.request.path.strip("/")
    if not cp_id:
        metrics_registry.WS_HANDSHAKE_FAILURES_TOTAL.labels(reason="empty_cp_id").inc()
        await connection.close(1008, "cp_id missing in URL path")
        return

    bind_contextvars(cp_id=cp_id)
    log.info("ws.connected")
    metrics_registry.WS_CONNECTS_TOTAL.inc()
    metrics_registry.WS_CONNECTIONS_ACTIVE.inc()

    if registry is not None:
        await registry.mark_online(cp_id)

    cp = EveysChargePoint(
        cp_id,
        connection,
        session_factory=session_factory,
        settings=settings,
        registry=registry,
        event_producer=event_producer,
        idempotency=idempotency,
        backend_client=backend_client,
        authorize_cache=authorize_cache,
        rate_limiter=rate_limiter,
    )
    if connections is not None:
        connections.add(cp)
    disconnect_reason = "clean"
    try:
        await cp.start()
    except Exception:
        # Any unhandled exception out of cp.start() means the
        # connection terminated abnormally — broker error, runtime
        # bug, etc. Tag the metric so an alert can fire on a sustained
        # `error` rate distinct from clean disconnects.
        disconnect_reason = "error"
        raise
    finally:
        if connections is not None:
            connections.remove(cp)
        if registry is not None:
            # Compare-and-delete: only clear if we still own the key.
            # A reconnect to a different pod between disconnect and
            # this call must not clobber the new owner.
            await registry.mark_offline(cp_id)
        metrics_registry.WS_CONNECTIONS_ACTIVE.dec()
        metrics_registry.WS_DISCONNECTS_TOTAL.labels(reason=disconnect_reason).inc()
        log.info("ws.disconnected")
        clear_contextvars()


async def serve_forever(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    registry: Registry | None = None,
    connections: ConnectionMap | None = None,
    event_producer: EventProducer | None = None,
    idempotency: IdempotencyCache | None = None,
    backend_client: BackendHTTPClient | None = None,
    authorize_cache: AuthorizeCache | None = None,
    rate_limiter: RateLimiter | None = None,
) -> None:
    """Start the WS server and block until cancelled.

    All Redis/Kafka/HTTP dependencies are optional so unit tests + the
    W1-style local stack can opt out. Production wiring (`__main__.py`)
    always passes all of them — `connections` is how gRPC RemoteStart
    finds the live WS, `event_producer` is how events reach Kafka,
    `registry` is how cross-pod ownership is tracked, `idempotency` is
    how `BootNotification`/`StopTransaction` replays are dropped
    (E2-11), and `backend_client` is how `Authorize` /
    `StartTransaction` / `StopTransaction` consult the backend (E3-3
    onwards).
    """

    async def handler(connection: ServerConnection) -> None:
        await _on_connect(
            connection,
            session_factory=session_factory,
            settings=settings,
            registry=registry,
            connections=connections,
            event_producer=event_producer,
            idempotency=idempotency,
            backend_client=backend_client,
            authorize_cache=authorize_cache,
            rate_limiter=rate_limiter,
        )

    # E5-5 — mTLS context when the operator has wired one. None
    # means plain WS (dev / compose / e2e). The helper raises a
    # clean error at boot if `ws_mtls_enabled=True` but a path is
    # missing — better than a half-initialised SSLContext.
    from eveys_ocpp.transport._tls import build_server_ssl_context

    ssl_ctx = build_server_ssl_context(settings)

    async with serve(
        handler,
        host=settings.ws_host,
        port=settings.ws_port,
        subprotocols=[OCPP_SUBPROTOCOL],
        ssl=ssl_ctx,
    ) as server:
        log.info(
            "ws.listening",
            host=settings.ws_host,
            port=settings.ws_port,
            mtls=ssl_ctx is not None,
        )
        await server.serve_forever()
