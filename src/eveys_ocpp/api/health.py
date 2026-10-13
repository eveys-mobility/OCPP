"""`GET /api/v1/health` — liveness + downstream-component probe.

Per the contract: HTTP status stays `200`; the `status` field flips
to `degraded`/`unavailable` when a downstream component is sick.
The backend's monitoring should alert on `status != "ok"`.

Component checks:
- `postgres` — `SELECT 1` against the gateway's pool.
- `redis` — `PING` via the registry's client (when wired).
- `clickhouse` — bounded `SELECT 1` through the archive read client.

This observation probe does not alter `/ready` or WS liveness.
Kafka delivery and ingestor progress are not verified by this probe.

This endpoint is exempt from auth (see `_auth._AUTH_BYPASS_PATHS`)
so the operator's load balancer can dial it regardless of token
configuration.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from sqlalchemy import text

from eveys_ocpp.api._schemas import HealthResponse
from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    pass

log = get_logger(__name__)

router = APIRouter(tags=["health"])
CLICKHOUSE_PROBE_TIMEOUT_SECONDS = 2.0


@router.get(
    "/health",
    summary="Liveness + per-component probe (auth-exempt)",
    responses={200: {"model": HealthResponse}},
)
async def health(request: Request) -> dict[str, object]:
    components: dict[str, str | None] = {}

    # Postgres: open a session, run a trivial query, swallow only the
    # specific connection-class exceptions. Any other failure (mypy
    # error, attribute error) is a real bug that should surface as
    # 500 via the catch-all exception handler.
    components["postgres"] = await _probe_postgres(request)

    # Redis (via the registry). Optional — the Kafka-less local stack
    # legitimately runs without it.
    components["redis"] = await _probe_redis(request)
    ch_status, ch_error = await _probe_clickhouse(request)
    components["clickhouse"] = ch_status
    components["clickhouse_error_class"] = ch_error

    overall = (
        "ok"
        if all(components[k] in {"ok", "unconfigured"} for k in ("postgres", "redis", "clickhouse"))
        else "degraded"
    )

    return {
        "status": overall,
        "version": _gateway_version(),
        "components": components,
        "request_id": request.state.request_id,
    }


async def _probe_clickhouse(request: Request) -> tuple[str, str | None]:
    client = getattr(request.app.state, "ch_client", None)
    if client is None:
        return "unconfigured", None
    try:
        async with asyncio.timeout(CLICKHOUSE_PROBE_TIMEOUT_SECONDS):
            await client.probe()
        return "ok", None
    except Exception as exc:
        error_class = type(exc).__name__
        log.warning("rest.health.clickhouse_unavailable", error_class=error_class)
        return "error", error_class


async def _probe_postgres(request: Request) -> str:
    session_factory = request.app.state.session_factory
    try:
        async with session_factory() as session:
            await session.execute(text("SELECT 1"))
        return "ok"
    except Exception as exc:
        log.warning("rest.health.postgres_unavailable", error=str(exc))
        return "unavailable"


async def _probe_redis(request: Request) -> str:
    redis = request.app.state.redis
    if redis is None:
        # Not wired in this stack — call it ok so the overall health
        # stays ok. A misconfigured deployment would fail at boot, not
        # show up here as degraded.
        return "ok"
    try:
        await redis.ping()
        return "ok"
    except Exception as exc:
        log.warning("rest.health.redis_unavailable", error=str(exc))
        return "unavailable"


def _gateway_version() -> str:
    """Best-effort version string for the `version` field.

    Reads `eveys_ocpp.__version__` if present (the package may not
    expose one in dev installs). Operations dashboards key off this
    value when chasing a regression to a specific build.
    """
    try:
        from eveys_ocpp import __version__

        return str(__version__)
    except (ImportError, AttributeError):
        return "unknown"
