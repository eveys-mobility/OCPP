"""Inbound REST server (E3-7, ADR-0026).

Programmatically runs `uvicorn.Server` inside the existing
`asyncio.TaskGroup` from `__main__.py` so the WS, gRPC, and REST
servers share one event loop, structured-logging context, and
shutdown signals.

The ASGI app lives in `eveys_ocpp.api._app`. This file is the
transport-layer wrapper: it owns the uvicorn config + lifecycle and
the shutdown handshake.

Symmetric with `transport/ws_server.py` and `transport/grpc_server.py`
— the gateway has three transports, each `serve_forever`-shaped.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import uvicorn

from eveys_ocpp.api._app import make_app
from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.registry import Registry
    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


async def serve_forever(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    registry: Registry | None,
    redis: Redis | None,
) -> None:
    """Build the FastAPI app, run uvicorn, return when the loop exits.

    Disables uvicorn's own access log and log-config so the gateway's
    structured-logging stack (structlog, configured in
    `observability.py`) is the only logger. Uvicorn lifespan is `on`
    so app-startup hooks fire (none today; reserved for future
    ClickHouse pool init in commit 4).
    """
    app = make_app(
        session_factory=session_factory,
        settings=settings,
        registry=registry,
        redis=redis,
    )
    config = uvicorn.Config(
        app,
        host=settings.rest_host,
        port=settings.rest_port,
        # Disable uvicorn's own logging — structlog owns the gateway's
        # log surface (observability.py). access_log=False prevents the
        # double-log between uvicorn's access logger and our request-id
        # middleware.
        log_config=None,
        access_log=False,
        lifespan="on",
        # Single worker; multiple workers would fork separate event
        # loops and break the WS + gRPC + REST single-process model.
        workers=1,
    )
    server = uvicorn.Server(config)
    log.info(
        "rest_server.start",
        host=settings.rest_host,
        port=settings.rest_port,
        auth_disabled=settings.rest_auth_disabled,
        token_count=_count_tokens(settings.rest_inbound_tokens),
    )
    try:
        await server.serve()
    finally:
        log.info("rest_server.stop")


def _count_tokens(raw: str) -> int:
    """Number of bearer tokens in the allowlist (for the startup log).

    We log the count, never the values — `rest_inbound_tokens` is a
    secret per ADR-0025."""
    return len([t for t in raw.split(",") if t.strip()])
