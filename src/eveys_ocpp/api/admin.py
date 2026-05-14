"""Admin endpoints — runtime config GET / SET / DELETE.

Three endpoints under `/api/v1/admin/config`:

  GET    /api/v1/admin/config
    Returns the current effective Settings dump (SecretStr fields
    auto-redact via E5-7) plus an `overrides` block listing
    in-process overrides currently in effect, plus an `allowlist`
    block listing fields the PATCH endpoint accepts.

  PATCH  /api/v1/admin/config
    Body: `{"updates": {"<field>": <value>, ...}}`. Validates each
    field against the closed allowlist in
    `eveys_ocpp.runtime_overrides`. Rejects non-allowlisted fields
    with a 400 + the allowed list.

  DELETE /api/v1/admin/config/overrides/{key}
    Removes a single override. Subsequent reads fall back to the
    boot-time Settings value.

**Per-pod scope.** Hitting these endpoints on pod A doesn't affect
pod B. Cluster-wide propagation via Redis pub/sub is a future
enhancement; for v0 the rolling deploy is the canonical mechanism
for fleet-wide changes. This is documented in the response envelope
so the operator UI can surface it.

**Auth.** Reuses the existing bearer-token middleware on
`/api/v1/*` (E3-7 / ADR-0026). No new auth surface; the operator
needs the same token any other admin caller uses.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from eveys_ocpp.api._errors import ERR_BAD_REQUEST, ERR_SERVICE_UNAVAILABLE, ApiError
from eveys_ocpp.observability import apply_log_level, get_logger
from eveys_ocpp.runtime_overrides import (
    OverrideNotAllowedError,
    all_overrides,
    allowlist,
    clear_override,
    set_override,
)

log = get_logger(__name__)

router = APIRouter(tags=["admin"])


class UpdateBody(BaseModel):
    """PATCH body. The keys in `updates` must be allowlisted; the
    values are coerced per the allowlist's `coerce` callable."""

    updates: dict[str, Any] = Field(
        default_factory=dict,
        description="Map of `field_name → new_value` for allowlisted fields.",
    )


def _settings_snapshot(request: Request) -> dict[str, Any]:
    """Render `Settings.model_dump(mode='json')`. SecretStr fields
    auto-redact to `**********` thanks to E5-7."""
    settings = request.app.state.settings
    # `mode='json'` ensures SecretStr serialises to its redacted
    # string form rather than the raw object.
    return dict(settings.model_dump(mode="json"))


@router.get(
    "/admin/config",
    summary="GET effective runtime configuration",
)
async def get_config(request: Request) -> dict[str, Any]:
    return {
        "settings": _settings_snapshot(request),
        "overrides": all_overrides(),
        "allowlist": allowlist(),
        "scope": "per-pod",
        "request_id": request.state.request_id,
    }


@router.patch(
    "/admin/config",
    summary="PATCH allowlisted runtime overrides",
)
async def patch_config(request: Request, body: UpdateBody) -> dict[str, Any]:
    if not body.updates:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="`updates` must be non-empty.",
        )

    applied: dict[str, Any] = {}
    rejected: dict[str, str] = {}
    for name, raw_value in body.updates.items():
        try:
            applied[name] = set_override(name, raw_value)
        except OverrideNotAllowedError as exc:
            rejected[name] = str(exc)
        except ValueError as exc:
            rejected[name] = str(exc)

    if rejected:
        # All-or-nothing on the rejection: any rejected field aborts
        # with 400 BUT we report what was already applied so the
        # operator can decide whether to revert. The applied set is
        # already in effect — this is "atomicity is not promised"
        # honesty, not silent partial-write.
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=(
                f"rejected: {rejected}; applied (still in effect): "
                f"{applied}; allowed: {sorted(allowlist())}"
            ),
        )

    # Side effects for fields that need more than just storing the
    # value (log_level reconfigures stdlib logging immediately).
    if "log_level" in applied:
        apply_log_level(applied["log_level"])

    return {
        "applied": applied,
        "overrides": all_overrides(),
        "scope": "per-pod",
        "request_id": request.state.request_id,
    }


@router.delete(
    "/admin/config/overrides/{key}",
    summary="DELETE a single runtime override (revert to env-driven default)",
)
async def delete_override(request: Request, key: str) -> dict[str, Any]:
    existed = clear_override(key)
    return {
        "cleared": existed,
        "key": key,
        "overrides": all_overrides(),
        "scope": "per-pod",
        "request_id": request.state.request_id,
    }


# ---------------------------------------------------------------------------
# Restart
# ---------------------------------------------------------------------------
#
# `POST /api/v1/admin/restart` lets the Console UI drive a process-restart for
# config keys that aren't on the runtime-overrides allowlist (kafka topic
# name, port, JWT secret). The endpoint:
#
#   - returns 202 immediately so the operator's browser sees a clean reply,
#   - schedules a SIGTERM on this PID ~500ms later (uvicorn handles SIGTERM
#     by walking the existing shutdown protocol in `shutdown.py`),
#   - is gated behind `admin_restart_enabled` (default False) so the endpoint
#     is dormant unless an operator explicitly turns it on,
#   - debounces: a second call inside `admin_restart_debounce_seconds`
#     returns 202 but does NOT schedule another exit. Guards against
#     double-clicks and the Console UI's overlay racing the button.
#
# The process actually coming back is the supervisor's job (compose's
# `restart: unless-stopped`, k8s Deployment, systemd). This endpoint just
# trips the trigger.


# Module-level so the debounce survives across calls within the same pod.
# `None` means "no restart scheduled yet on this pod".
_last_restart_scheduled_at: float | None = None

# Hold a reference to the scheduled exit task so the event loop's weakref
# doesn't GC it before it fires (ruff RUF006 — without this, the task can
# be reaped between the route returning and the 500ms sleep elapsing,
# leaving the process running instead of restarting).
_pending_exit_task: Any = None


async def _delayed_sigterm(delay_seconds: float) -> None:
    """Sleep, then SIGTERM ourselves.

    The sleep gives the 202 response time to flush all the way back to the
    operator's browser before the process starts tearing down. SIGTERM is
    what uvicorn wants to see for a graceful shutdown; the gateway's
    `shutdown.py` is wired to that signal already, so chargers get a clean
    WS close on the way out instead of a TCP reset.
    """
    import asyncio
    import os
    import signal

    await asyncio.sleep(delay_seconds)
    os.kill(os.getpid(), signal.SIGTERM)


@router.post(
    "/admin/restart",
    summary="Terminate this process so the supervisor respawns it (config-reload helper)",
    status_code=202,
)
async def restart(request: Request) -> dict[str, Any]:
    """Self-exit so a config change that needs a fresh boot can be applied
    from the Console UI instead of via SSH.

    Auth is the same admin-token allowlist that gates the rest of
    `/api/v1/admin/*`; no separate gate. Operators who want stricter access
    layer in a reverse-proxy ACL on this exact path."""
    import asyncio
    import time

    global _last_restart_scheduled_at

    settings = request.app.state.settings
    if not settings.admin_restart_enabled:
        raise ApiError(
            status_code=503,
            error_code=ERR_SERVICE_UNAVAILABLE,
            message=(
                "admin restart is disabled — set EVEYS_OCPP_ADMIN_RESTART_ENABLED=true to enable"
            ),
        )

    now = time.monotonic()
    debounce = settings.admin_restart_debounce_seconds
    if _last_restart_scheduled_at is not None and (now - _last_restart_scheduled_at) < debounce:
        # In-flight restart already armed. Return 202 with the
        # already-scheduled status so the caller doesn't see a spurious
        # failure, but DON'T queue a second SIGTERM.
        log.info("admin.restart.debounced", since_seconds=now - _last_restart_scheduled_at)
        return {
            "status": "already_scheduled",
            "exits_in_ms": 0,
            "scope": "per-pod",
            "request_id": request.state.request_id,
        }

    global _pending_exit_task
    _last_restart_scheduled_at = now
    log.warning("admin.restart.scheduled", exits_in_ms=500, pid=__import__("os").getpid())
    _pending_exit_task = asyncio.create_task(_delayed_sigterm(0.5))

    return {
        "status": "scheduled",
        "exits_in_ms": 500,
        "scope": "per-pod",
        "request_id": request.state.request_id,
    }


def _reset_restart_debounce_for_tests() -> None:
    """Vitest-equivalent reset hook — pytest fixtures clear the module-level
    debounce + pending-task reference between cases so consecutive tests
    don't see each other's 'already_scheduled' state. Do NOT call this in
    production code."""
    global _last_restart_scheduled_at, _pending_exit_task
    _last_restart_scheduled_at = None
    _pending_exit_task = None
