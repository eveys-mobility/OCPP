"""FastAPI app factory for the gateway REST API.

Assembles middleware (request-id correlation, bearer auth), exception
handlers (typed `ApiError` + Pydantic validation + framework
`HTTPException` + catch-all), and per-domain routers.

OpenAPI / docs UIs are gated behind `settings.rest_openapi_enabled`
(default False) per ADR-0026. The hand-written contract in
`docs/integration/02-gateway-rest-api.md` is still the canonical
source of truth; the schema generated from FastAPI is a complement,
not a replacement, because most routes return plain dicts annotated
via `responses=` rather than `response_model=` (so OpenAPI describes
the shape without forcing runtime validation that could break a
production endpoint). Operators flip the toggle on in dev / staging /
behind a VPN to get Swagger UI at `/api/v1/docs`.

Routes attach to `request.app.state` for the dependencies they need:

- `app.state.session_factory` — async SQLAlchemy session factory.
- `app.state.settings` — frozen `Settings`.
- `app.state.registry` — Redis online registry (presence lookups).

Future commits add `app.state.ch_client` (ClickHouse read client) for
the time-series endpoints.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from eveys_ocpp import __version__
from eveys_ocpp.api import (
    admin,
    authorizations,
    charge_points,
    charging_profiles,
    commands,
    credentials,
    health,
    pending_certificate_signings,
    ready,
    reservations,
    sys_config,
    sys_kpis,
    timeseries,
    transactions,
    webhook_backlog,
)
from eveys_ocpp.api._auth import make_bearer_auth_middleware
from eveys_ocpp.api._errors import (
    ApiError,
    api_error_handler,
    http_exception_handler,
    internal_error_handler,
    validation_exception_handler,
)
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, get_logger

log = get_logger(__name__)

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.clickhouse.read_client import ClickHouseReadClient
    from eveys_ocpp.connections import ConnectionMap
    from eveys_ocpp.registry import Registry
    from eveys_ocpp.settings import Settings
    from eveys_ocpp.shutdown import DrainController
    from eveys_ocpp.transport.grpc_server import OcppGatewayService


def make_app(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    registry: Registry | None,
    redis: Redis | None,
    command_service: OcppGatewayService | None = None,
    ch_client: ClickHouseReadClient | None = None,
    drain_controller: DrainController | None = None,
    connections: ConnectionMap | None = None,
) -> FastAPI:
    """Construct the gateway REST app.

    Caller is responsible for the ASGI server lifecycle; see
    `eveys_ocpp.transport.rest_server.serve_forever`.

    `redis` is a separate parameter from `registry` because the health
    probe pings Redis directly; threading it through keeps the route
    out of `Registry`'s private state.
    """
    # OpenAPI surface is gated behind `rest_openapi_enabled`. Default off
    # per ADR-0026 (the gateway doesn't self-publish a discoverable
    # schema in production); set the env var to True for dev / staging
    # to get Swagger UI at `/api/v1/docs` and ReDoc at `/api/v1/redoc`.
    # Auth still applies — only token-bearers can read the spec.
    if settings.rest_openapi_enabled:
        log.warning(
            "rest_openapi.enabled",
            detail=(
                "EVEYS_OCPP_REST_OPENAPI_ENABLED=True — the gateway is "
                "publishing OpenAPI schema + Swagger UI on `/api/v1/`. "
                "Acceptable in dev / staging / behind a VPN; never "
                "expose to the public internet without considering the "
                "schema-discovery threat model documented in ADR-0026."
            ),
        )
        openapi_kwargs: dict[str, Any] = {
            "openapi_url": "/api/v1/openapi.json",
            "docs_url": "/api/v1/docs",
            "redoc_url": "/api/v1/redoc",
        }
    else:
        openapi_kwargs = {
            "openapi_url": None,
            "docs_url": None,
            "redoc_url": None,
        }

    app = FastAPI(
        title="eveys/ocpp gateway",
        version=__version__,
        description=(
            "Inbound REST surface (E3-7 / E3-8). Backend services read "
            "charger and transaction state under `/api/v1/charge-points`, "
            "`/api/v1/transactions`, etc., and dispatch OCPP commands "
            "via `/api/v1/charge-points/{cp_id}/commands/*`. "
            "All endpoints require a bearer token (`EVEYS_OCPP_REST_INBOUND_TOKENS`); "
            "`/api/v1/health` is exempt. Errors carry the closed-enum "
            "envelope `{error, error_code, request_id}` documented in "
            "[`docs/integration/02-gateway-rest-api.md`]"
            "(https://github.com/eveys-mobility/OCPP/blob/main/docs/integration/02-gateway-rest-api.md)."
        ),
        **openapi_kwargs,
    )

    # Command routes reference `RemoteStartRequest` / `RemoteStopRequest` /
    # `ResetRequest` via `openapi_extra={"requestBody": ...}`, which uses
    # `$ref: #/components/schemas/<Name>` — but FastAPI only auto-emits
    # models that show up in route signatures (`response_model=`,
    # `Body(...)`, etc.). The three request models are referenced by
    # name but never appear in a signature, so they'd be missing from
    # `components/schemas` and the `$ref` would dangle. Wrap `app.openapi`
    # to inject them after FastAPI builds its base spec.
    if settings.rest_openapi_enabled:
        _install_extra_schemas(app)
    app.state.session_factory = session_factory
    app.state.settings = settings
    app.state.registry = registry
    app.state.redis = redis
    # E3-8: command surface dispatches through this service. None is
    # acceptable in tests that exercise only the read API.
    app.state.command_service = command_service
    # E3-7d: ClickHouse read client backs the timeseries endpoints
    # (meter-values, status-history). None is acceptable in tests
    # without ClickHouse; the route handlers raise INTERNAL_ERROR if
    # a request reaches them with no client wired.
    app.state.ch_client = ch_client
    # Drain controller drives `/api/v1/ready`. None is acceptable for
    # unit tests that build the app without a shutdown lifecycle —
    # the endpoint treats absence as "never draining".
    app.state.drain_controller = drain_controller
    # Per-pod live-WS map. The revoke endpoint reaches into it to
    # force-close an offending charger; None in unit tests that
    # exercise only read endpoints.
    app.state.connections = connections

    # Order matters: request-id MUST run before auth so the auth-reject
    # response carries the correlation id. Middlewares run in reverse
    # registration order (FastAPI/Starlette quirk), so register auth
    # first, then metrics, then request-id last → request-id wraps
    # everything; metrics wraps the auth + handlers so a 401 still
    # observes latency.
    app.middleware("http")(make_bearer_auth_middleware(settings))
    app.middleware("http")(_metrics_middleware)
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
    app.include_router(ready.router, prefix="/api/v1")
    app.include_router(charge_points.router, prefix="/api/v1")
    app.include_router(transactions.router, prefix="/api/v1")
    app.include_router(reservations.router, prefix="/api/v1")
    app.include_router(charging_profiles.router, prefix="/api/v1")
    app.include_router(commands.router, prefix="/api/v1")
    app.include_router(credentials.router, prefix="/api/v1")
    app.include_router(authorizations.router, prefix="/api/v1")
    app.include_router(pending_certificate_signings.router, prefix="/api/v1")
    app.include_router(timeseries.router, prefix="/api/v1")
    app.include_router(admin.router, prefix="/api/v1")
    app.include_router(sys_config.router, prefix="/api/v1")
    app.include_router(sys_kpis.router, prefix="/api/v1")
    app.include_router(webhook_backlog.router, prefix="/api/v1")
    # SSE event stream (ADR-0030). Bus lifecycle is owned by the
    # caller (transport/rest_server.py) so the route reads it off
    # `app.state.sse_bus`; None when the feature is off.
    if settings.sse_enabled:
        from eveys_ocpp.api import sse

        app.include_router(sse.router, prefix="/api/v1")
    app.state.sse_bus = None

    return app


def _install_extra_schemas(app: FastAPI) -> None:
    """Inject request models referenced via `openapi_extra` into the
    generated spec's `components.schemas` so the `$ref`s resolve, and
    declare the bearer-token security scheme so Swagger UI's
    "Authorize" button is wired up.

    See the comment at the call site in `make_app` for the why. Wraps
    `app.openapi` so the spec is computed lazily on first read but the
    extras land on every subsequent call (FastAPI caches the result on
    `app.openapi_schema` after first call).
    """
    from eveys_ocpp.api._schemas import (
        RemoteStartRequest,
        RemoteStopRequest,
        ResetRequest,
    )

    extras = (RemoteStartRequest, RemoteStopRequest, ResetRequest)
    base_openapi = app.openapi

    def custom_openapi() -> dict[str, Any]:
        spec = base_openapi()
        components = spec.setdefault("components", {})
        schemas = components.setdefault("schemas", {})
        for model in extras:
            if model.__name__ not in schemas:
                schemas[model.__name__] = model.model_json_schema(
                    ref_template="#/components/schemas/{model}"
                )
        # Declare the bearer scheme + apply globally so Swagger UI
        # shows the Authorize 🔒 button and reuses the token across
        # every "Try it out" call. The middleware still does the real
        # enforcement; this only teaches the spec how to ask.
        security_schemes = components.setdefault("securitySchemes", {})
        security_schemes.setdefault(
            "bearerAuth",
            {
                "type": "http",
                "scheme": "bearer",
                "description": (
                    "Static bearer token from `EVEYS_OCPP_REST_INBOUND_TOKENS` "
                    "(CSV allowlist). Run `make get-token` to print one from "
                    "your local env."
                ),
            },
        )
        spec.setdefault("security", [{"bearerAuth": []}])
        return spec

    app.openapi = custom_openapi  # type: ignore[method-assign]


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


async def _metrics_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[JSONResponse]],
) -> JSONResponse:
    """Increment `REST_REQUESTS_TOTAL{method,route,code}` and observe
    `REST_REQUEST_LATENCY_SECONDS{method,route}` on every request.

    `route` uses the FastAPI **route template** (e.g.
    `/api/v1/charge-points/{cp_id}`) — never the literal path — so a
    cp_id-keyed traffic pattern doesn't blow up label cardinality.
    Requests that don't match a route (404s, OPTIONS preflights) get
    `route="_unmatched"`.
    """
    started = time.perf_counter()
    response = await call_next(request)
    route_obj = request.scope.get("route")
    route_label = getattr(route_obj, "path", None) or "_unmatched"
    method = request.method
    metrics_registry.REST_REQUESTS_TOTAL.labels(
        method=method, route=route_label, code=str(response.status_code)
    ).inc()
    metrics_registry.REST_REQUEST_LATENCY_SECONDS.labels(method=method, route=route_label).observe(
        time.perf_counter() - started
    )
    return response
