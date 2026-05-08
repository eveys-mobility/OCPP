"""Bearer-token authentication middleware (ADR-0026 D3 + issue #84 PR-A).

The middleware accepts **two parallel token schemes**:

1. **Static tokens** from `Settings.rest_inbound_tokens` (CSV) —
   service-to-service callers like the eveys-backend. Match → an
   `AuthIdentity(kind="service")` is attached to `request.state`.
   These callers bypass per-user filtering (PR-C); they're trusted
   service principals, not human users.

2. **Opaque user tokens** issued by `POST /api/v1/auth/login` and
   stored in Redis (issue #84). Match → an
   `AuthIdentity(kind="user" | "superadmin")` is attached. PR-C's
   charger filtering reads the user_id from this identity.

Lookup order: static-list match first (cheaper, no Redis hit), then
Redis. A token that matches neither is a 401.

`rest_auth_disabled=True` bypasses the whole gate (dev / test only).
`/api/v1/health`, `/api/v1/ready`, and `/api/v1/auth/login` are
auth-exempt (probes / login itself).

Failure modes are uniform (same 401 envelope, same body) so an
attacker can't distinguish "unknown token" from "Redis blip" via
timing. The detailed reason is in the structured log.
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
_AUTH_BYPASS_PATHS = frozenset(
    {
        "/api/v1/health",
        "/api/v1/ready",
        # Login can't require a login token (issue #84 PR-A).
        "/api/v1/auth/login",
    }
)


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

    if auth_disabled:
        log.warning(
            "rest_auth.disabled",
            note="every request accepted; dev/test only — never set in production",
        )
    elif not allowlist:
        log.info(
            "rest_auth.empty_static_allowlist",
            note=(
                "no static `rest_inbound_tokens` configured; only login-"
                "issued user tokens (issue #84) will authenticate."
            ),
        )

    # Lazy import — avoids circular `auth → api` import at boot.
    from eveys_ocpp.auth import AuthIdentity, lookup_token

    async def middleware(
        request: Request,
        call_next: Callable[[Request], Awaitable[JSONResponse]],
    ) -> JSONResponse:
        if auth_disabled:
            # Dev shortcut: attach a permissive identity so downstream
            # routes that read `request.state.identity` don't break.
            request.state.identity = AuthIdentity(
                kind="service", username="auth-disabled", user_id=None
            )
            return await call_next(request)

        if request.url.path in _AUTH_BYPASS_PATHS:
            return await call_next(request)

        header = request.headers.get("authorization", "")
        if not header.startswith(_BEARER_PREFIX):
            return _unauthorised(request, reason="missing_or_malformed_header")
        token = header[len(_BEARER_PREFIX) :].strip()
        if not token:
            return _unauthorised(request, reason="empty_token")

        # Static-token callers (eveys-backend's service auth) come
        # first — cheaper than a Redis lookup, and rejecting them
        # later would force every legitimate service request to pay
        # the Redis round-trip.
        if token in allowlist:
            request.state.identity = AuthIdentity(
                kind="service",
                username="static-token",
                user_id=None,
            )
            return await call_next(request)

        # Issue #84: opaque user token in Redis.
        redis = getattr(request.app.state, "redis", None)
        if redis is not None:
            identity = await lookup_token(redis, token=token)
            if identity is not None:
                request.state.identity = identity
                return await call_next(request)

        return _unauthorised(request, reason="token_not_recognised")

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
