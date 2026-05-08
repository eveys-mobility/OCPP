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

from eveys_ocpp.api._errors import ERR_BAD_REQUEST, ApiError
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
