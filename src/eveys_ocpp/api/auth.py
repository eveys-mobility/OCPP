"""Login + logout endpoints (issue #84 PR-A).

Two routes under `/api/v1/auth/`:

  POST /api/v1/auth/login
    Body: `{"username": "...", "password": "..."}`
    Success: `{"access_token": "...", "token_type": "Bearer",
              "expires_at": "<iso>", "scope": "user|superadmin"}`
    Failure: 401 with the standard error envelope.

  POST /api/v1/auth/logout
    Header: `Authorization: Bearer <token>`
    Revokes the token; subsequent requests with the same token
    return 401.

Login is the **only** endpoint exempt from auth — see
`api/_auth.py::_AUTH_BYPASS_PATHS`. Every other route, including
logout, requires a bearer.

Login responses are deliberately uniform across failure modes
(unknown user / wrong password / expired user → same 401 with the
same body). The actual reason is in the structured log line, not
the wire response. This is the standard timing/enumeration
mitigation; an attacker probing the user table can't tell the
difference between "no such user" and "user exists but wrong
password" from the HTTP layer.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from eveys_ocpp.api._errors import ERR_UNAUTHORIZED, ApiError
from eveys_ocpp.auth import authenticate_login, issue_token, revoke_token
from eveys_ocpp.observability import get_logger

log = get_logger(__name__)

router = APIRouter(tags=["auth"])


class LoginBody(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=1, max_length=256)


@router.post(
    "/auth/login",
    summary="Exchange username + password for an opaque access token",
)
async def login(request: Request, body: LoginBody) -> dict[str, object]:
    settings = request.app.state.settings
    session_factory = request.app.state.session_factory
    redis = request.app.state.redis

    if redis is None:
        # Auth tokens are stored in Redis; with no Redis, the gateway
        # can't issue a usable token. This is a deploy-time
        # configuration error, not a per-request failure.
        raise ApiError(
            status_code=503,
            error_code="INTERNAL_ERROR",
            message="auth backing store (Redis) not configured",
        )

    async with session_factory() as session:
        identity = await authenticate_login(
            username=body.username,
            password=body.password,
            session=session,
            settings=settings,
        )

    if identity is None:
        # Uniform response across all failure modes — see module
        # docstring. The detailed reason is in the structured log
        # emitted by `authenticate_login`.
        raise ApiError(
            status_code=401,
            error_code=ERR_UNAUTHORIZED,
            message="invalid credentials",
        )

    token, expires_at = await issue_token(
        redis,
        identity=identity,
        ttl_seconds=settings.auth_token_ttl_seconds,
    )
    log.info(
        "auth.login_success",
        username=identity.username,
        kind=identity.kind,
    )
    return {
        "access_token": token,
        "token_type": "Bearer",
        "expires_at": expires_at.isoformat(),
        "scope": identity.kind,
        "request_id": request.state.request_id,
    }


@router.post(
    "/auth/logout",
    summary="Revoke the bearer token on the request",
)
async def logout(request: Request) -> dict[str, object]:
    redis = request.app.state.redis
    if redis is None:
        # Without Redis there's no token to revoke; treat as no-op
        # rather than 500 — the operator's intent (drop the session)
        # is satisfied by the absence of state.
        return {"revoked": False, "request_id": request.state.request_id}

    header = request.headers.get("authorization", "")
    if not header.lower().startswith("bearer "):
        # The auth middleware already rejected this case before we
        # got here, but be defensive — a future change to the
        # bypass list could route us a no-token request.
        raise ApiError(
            status_code=400,
            error_code="BAD_REQUEST",
            message="logout requires Bearer token in Authorization header",
        )
    token = header[7:].strip()
    revoked = await revoke_token(redis, token=token)
    log.info("auth.logout", revoked=revoked)
    return {
        "revoked": revoked,
        "request_id": request.state.request_id,
    }
