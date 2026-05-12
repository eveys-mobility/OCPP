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

`/api/v1/health` and `/api/v1/ready` bypass auth (probes; the
operator's load balancer needs them to dial the pod regardless of
token configuration).

When `rest_openapi_enabled=True` (dev / staging / behind a VPN),
the docs surface (`/api/v1/docs`, `/api/v1/redoc`,
`/api/v1/openapi.json`) also bypasses auth so an operator can
load the Swagger / ReDoc UI in a browser without first injecting
a token. The actual API endpoints exposed below those UIs still
require a token — only the schema fetch and the static UI bundle
are open. Production keeps `rest_openapi_enabled=False`, which
makes the bypass moot (the routes don't exist).
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
_AUTH_BYPASS_PATHS = frozenset({"/api/v1/health", "/api/v1/ready"})

# Additional bypass paths only enabled when `rest_openapi_enabled=True`
# (dev / staging). The Swagger / ReDoc UIs are static HTML + a JSON
# spec fetch — neither leaks runtime data, and forcing a token to load
# the UI itself is hostile UX. The protected API endpoints below the
# UI still require a token via the standard middleware path.
_OPENAPI_BYPASS_PATHS = frozenset({"/api/v1/docs", "/api/v1/redoc", "/api/v1/openapi.json"})


def _is_sse_path(path: str) -> bool:
    """The SSE endpoint is the one route that accepts a
    `?access_token=` query parameter in addition to a bearer header.

    Browsers' native ``EventSource`` cannot send custom headers, so an
    operator UI has no way to attach ``Authorization: Bearer …`` —
    the alternatives are a cookie + same-origin reverse proxy, or a
    query-param token. We pick the query-param route because it
    requires no proxy and lets every browser open the stream.

    The path-suffix match is intentionally tight: only
    ``/api/v1/charge-points/<cp_id>/events`` qualifies. Any other
    surface treating a query-param token as auth would be a footgun
    (URLs end up in proxy logs, browser history, referer headers); we
    keep the relaxation scoped to the one endpoint that can't use the
    header form.
    """
    return path.startswith("/api/v1/charge-points/") and path.endswith("/events")


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
    # E5-7: rest_inbound_tokens is a SecretStr. Unwrap here at the
    # parser boundary; the resulting `set[str]` lives only inside the
    # closure, never in a Settings dump.
    allowlist = parse_token_allowlist(settings.rest_inbound_tokens.get_secret_value())
    auth_disabled = settings.rest_auth_disabled
    openapi_enabled = settings.rest_openapi_enabled

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
        path = request.url.path
        if auth_disabled or path in _AUTH_BYPASS_PATHS:
            return await call_next(request)
        if openapi_enabled and path in _OPENAPI_BYPASS_PATHS:
            return await call_next(request)

        # Header-form is the canonical path. SSE additionally accepts
        # a query-param token because browsers' native EventSource
        # cannot set custom request headers — same allowlist, same
        # secret, just delivered over the URL. See `_is_sse_path` for
        # the why and the scope.
        header = request.headers.get("authorization", "")
        if header.startswith(_BEARER_PREFIX):
            token = header[len(_BEARER_PREFIX) :].strip()
        elif _is_sse_path(path):
            token = request.query_params.get("access_token", "").strip()
            if not token:
                return _unauthorised(request, reason="missing_access_token_query_param")
        else:
            return _unauthorised(request, reason="missing_or_malformed_header")
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
