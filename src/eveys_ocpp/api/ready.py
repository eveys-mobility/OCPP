"""`GET /api/v1/ready` — load-balancer readiness probe.

Distinct from `/health`:

- `/health` reports whether downstream components (Postgres, Redis)
  are reachable. It stays 200 even when the gateway is degraded;
  the body's `status` field carries the verdict.
- `/ready` reports whether this pod is willing to accept new
  charger connections. It returns 200 normally and **503 once the
  pod is draining**. The load balancer's readiness probe reads
  the HTTP status, so flipping to 503 removes the pod from the
  rotation pool — which is exactly what graceful shutdown needs.

Both endpoints are auth-exempt: the LB's probe doesn't carry a
bearer token, and there's no sensitive information in the body.

The endpoint reads `request.app.state.drain_controller`. When the
controller is absent (e.g. unit tests that build the app without
wiring shutdown), the endpoint falls back to "ready" — a missing
controller is treated as "drain has never been triggered".
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from eveys_ocpp.observability import get_logger

log = get_logger(__name__)

router = APIRouter(tags=["health"])


@router.get(
    "/ready",
    summary="Readiness probe (auth-exempt)",
    responses={
        200: {"description": "Pod is accepting new connections."},
        503: {"description": "Pod is draining; load balancer should remove from rotation."},
    },
)
async def ready(request: Request) -> JSONResponse:
    drain_controller = getattr(request.app.state, "drain_controller", None)
    is_draining = bool(drain_controller and drain_controller.is_draining)

    body: dict[str, object] = {
        "status": "draining" if is_draining else "ready",
        "request_id": request.state.request_id,
    }
    if is_draining and drain_controller is not None:
        started_at = drain_controller.drain_started_at
        if started_at is not None:
            import time

            body["draining_for_seconds"] = round(time.monotonic() - started_at, 3)

    return JSONResponse(status_code=503 if is_draining else 200, content=body)
