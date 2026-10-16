"""`GET /api/v1/health` — liveness + downstream-component probe.

Per the contract: HTTP status stays `200`; the `status` field flips
to `degraded`/`unavailable` when a downstream component is sick.
The backend's monitoring should alert on `status != "ok"`.

Component checks:
- `postgres` — `SELECT 1` against the gateway's pool.
- `redis` — `PING` via the registry's client (when wired).
- `clickhouse` — bounded `SELECT 1` through the archive read client.
- `ingestor` — last background sample of the ingestor's Kafka
  consumer-group lag (`eveys_ocpp.clickhouse.progress`). The request
  never waits on Kafka; `unknown` means no fresh sample.

This observation probe does not alter `/ready` or WS liveness.

This endpoint is exempt from auth (see `_auth._AUTH_BYPASS_PATHS`)
so the operator's load balancer can dial it regardless of token
configuration.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from sqlalchemy import text

from eveys_ocpp.api._schemas import HealthResponse
from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    from eveys_ocpp.clickhouse.progress import IngestorProgressProbe

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
    ingestor, ingestor_error, archive = _observe_ingestor(request)
    components["ingestor"] = ingestor
    components["ingestor_error_class"] = ingestor_error

    overall = (
        "ok"
        if all(
            components[k] in {"ok", "unconfigured"}
            for k in ("postgres", "redis", "clickhouse", "ingestor")
        )
        else "degraded"
    )

    return {
        "status": overall,
        "version": _gateway_version(),
        "components": components,
        "archive": archive,
        "request_id": request.state.request_id,
    }


def _observe_ingestor(request: Request) -> tuple[str, str | None, dict[str, object]]:
    """Read the background sample; never touches Kafka in the request."""
    probe: IngestorProgressProbe | None = getattr(request.app.state, "ingestor_probe", None)
    if probe is None:
        return (
            "unconfigured",
            None,
            {
                "state": "unknown",
                "reason": "ingestor_progress_not_observed",
                "consumer_lag_messages": None,
                "last_successful_commit_at": None,
                "observed_at": None,
            },
        )
    progress = probe.snapshot()
    return (
        progress.state,
        progress.error_class,
        {
            "state": progress.state,
            "reason": progress.reason,
            "consumer_lag_messages": progress.lag_messages,
            "last_successful_commit_at": _iso(progress.last_commit_at),
            "observed_at": _iso(progress.observed_at),
        },
    )


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


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
