"""FastAPI app factory for the gateway REST API.

Assembles middleware (request-id correlation, bearer auth), exception
handlers (typed `ApiError` + Pydantic validation + framework
`HTTPException` + catch-all), and per-domain routers.

Per ADR-0026, OpenAPI / docs UIs are disabled — the contract lives in
`docs/integration/02-gateway-rest-api.md` and we don't want a self-
describing schema published to anyone who can curl the gateway.

Routes attach to `request.app.state` for the dependencies they need:

- `app.state.session_factory` — async SQLAlchemy session factory.
- `app.state.settings` — frozen `Settings`.
- `app.state.registry` — Redis online registry (presence lookups).

Future commits add `app.state.ch_client` (ClickHouse read client) for
the time-series endpoints.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from eveys_ocpp.api import charge_points, commands, health, transactions
from eveys_ocpp.api._auth import make_bearer_auth_middleware
from eveys_ocpp.api._errors import (
    ApiError,
    api_error_handler,
    http_exception_handler,
    internal_error_handler,
    validation_exception_handler,
)
from eveys_ocpp.observability import bind_contextvars

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.registry import Registry
    from eveys_ocpp.settings import Settings
    from eveys_ocpp.transport.grpc_server import OcppGatewayService


def make_app(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    registry: Registry | None,
    redis: Redis | None,
    command_service: OcppGatewayService | None = None,
) -> FastAPI:
    """Construct the gateway REST app.

    Caller is responsible for the ASGI server lifecycle; see
    `eveys_ocpp.transport.rest_server.serve_forever`.

    `redis` is a separate parameter from `registry` because the health
    probe pings Redis directly; threading it through keeps the route
    out of `Registry`'s private state.
    """
    app = FastAPI(
        title="eveys/ocpp gateway",
        # OpenAPI surface disabled per ADR-0026 D-rejected-alternatives.
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.session_factory = session_factory
    app.state.settings = settings
    app.state.registry = registry
    app.state.redis = redis
    # E3-8: command surface dispatches through this service. None is
    # acceptable in tests that exercise only the read API.
    app.state.command_service = command_service

    # Order matters: request-id MUST run before auth so the auth-reject
    # response carries the correlation id. Middlewares run in reverse
    # registration order (FastAPI/Starlette quirk), so register auth
    # first, then request-id last → request-id wraps everything.
    app.middleware("http")(make_bearer_auth_middleware(settings))
    app.middleware("http")(_request_id_middleware)

    # Exception handlers. ApiError is the typed shape routes raise;
    # the others catch framework-level rejections (route not found,
    # method not allowed, body validation).
    app.add_exception_handler(ApiError, api_error_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, internal_error_handler)

    # Routers. Each is mounted under `/api/v1` per the frozen contract.
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(charge_points.router, prefix="/api/v1")
    app.include_router(transactions.router, prefix="/api/v1")
    app.include_router(commands.router, prefix="/api/v1")
    # Reservations + profiles (commit 3) and meter-values + status-history
    # (commit 4) hook in here as their routers land.

    return app


async def _request_id_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[JSONResponse]],
) -> JSONResponse:
    """Echo the inbound `X-Request-ID` (or fabricate one) and attach to
    structured-logging context.

    Every response — success or error — carries `X-Request-ID` so the
    backend can correlate requests across pods and the gateway's
    structured logs."""
    rid = request.headers.get("x-request-id") or str(uuid.uuid4())
    request.state.request_id = rid
    bind_contextvars(request_id=rid, http_path=request.url.path, http_method=request.method)
    response = await call_next(request)
    response.headers["X-Request-ID"] = rid
    return response
