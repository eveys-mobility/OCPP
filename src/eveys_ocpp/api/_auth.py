"""Bearer-token authentication middleware (ADR-0026 D3).

The gateway validates inbound tokens against an env-driven allowlist
held in `Settings.rest_inbound_tokens` (CSV). Multi-value to support
rotation: the eveys-backend, a billing back-fill job, and a future
operator UI may all hit this surface, and rotating one consumer's
token must not flap the others.

Three modes:

1. `rest_auth_disabled=True` → bypass entirely. Dev / unit-test only.
   Logged loudly at startup so an accidental flip is visible.
2. `rest_auth_disabled=False` AND allowlist empty → reject every
   request with 401. Production safe-by-default: a missing
   configuration must NOT silently open the surface.
3. `rest_auth_disabled=False` AND allowlist non-empty → exact-match
   the bearer against the parsed list.

`/api/v1/health` is the one endpoint that bypasses auth (it's a probe;
the operator's load balancer needs it to dial the pod regardless of
token configuration).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import Request
from fastapi.responses import JSONResponse

from eveys_ocpp.api._errors import (
    ERR_UNAUTHORIZED,
    _envelope,
    _request_id,
)
from eveys_ocpp.observability import get_logger
from eveys_ocpp.settings import Settings

log = get_logger(__name__)

_BEARER_PREFIX = "Bearer "

# Endpoints exempt from auth. Must be the FastAPI mount path, not the
# raw URL — the routers are added under the `/api/v1` prefix.
_AUTH_BYPASS_PATHS = frozenset({"/api/v1/health"})


def parse_token_allowlist(raw: str) -> set[str]:
    """Turn a CSV `rest_inbound_tokens` value into a deduped set of
    bearer values. Whitespace and empty entries are dropped."""
    return {t.strip() for t in raw.split(",") if t.strip()}


def make_bearer_auth_middleware(
    settings: Settings,
) -> Callable[[Request, Callable[[Request], Awaitable[JSONResponse]]], Awaitable[JSONResponse]]:
    """Construct the FastAPI HTTP middleware closure.

    Reads `settings` once at app construction; `Settings` is frozen
    (ADR-0001) so a runtime env change requires a pod restart, which
    is the same model as every other Settings-driven knob.
    """
    allowlist = parse_token_allowlist(settings.rest_inbound_tokens)
    auth_disabled = settings.rest_auth_disabled

    if auth_disabled:
        log.warning(
            "rest_auth.disabled",
            note="every request accepted; dev/test only — never set in production",
        )
    elif not allowlist:
        log.warning(
            "rest_auth.empty_allowlist",
            note="every authenticated request will be rejected with 401",
        )

    async def middleware(
        request: Request,
        call_next: Callable[[Request], Awaitable[JSONResponse]],
    ) -> JSONResponse:
        if auth_disabled or request.url.path in _AUTH_BYPASS_PATHS:
            return await call_next(request)

        header = request.headers.get("authorization", "")
        if not header.startswith(_BEARER_PREFIX):
            return _unauthorised(request, reason="missing_or_malformed_header")
        token = header[len(_BEARER_PREFIX) :].strip()
        if not token or token not in allowlist:
            return _unauthorised(request, reason="token_not_in_allowlist")

        return await call_next(request)

    return middleware


def _unauthorised(request: Request, *, reason: str) -> JSONResponse:
    log.info("rest_auth.rejected", reason=reason, path=request.url.path)
    return JSONResponse(
        status_code=401,
        content=_envelope(
            error_code=ERR_UNAUTHORIZED,
            message="unauthorized",
            request_id=_request_id(request),
        ),
    )
