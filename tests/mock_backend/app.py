"""FastAPI app — mock implementation of the backend REST contract.

Endpoints follow `docs/integration/01-backend-rest-contract.md`
verbatim. Every response uses the canonical envelope:

    { "success": bool, "data": dict | None, "message": str, "error_code"? }

In-memory idempotency cache: a request whose ``Idempotency-Key``
matches a prior call returns the prior response unchanged. A
mismatched body for the same key returns 409 Conflict per the spec.

The cache is process-local and unbounded — fine for dev use, never
deployed in production.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .config import MockBackendConfig

# ---- Request / response models ---------------------------------------------


class _AuthorizeRequest(BaseModel):
    id_tag: str = Field(..., max_length=20)
    cp_id: str = Field(..., max_length=64)


class _SessionsOpenRequest(BaseModel):
    transaction_id: int
    cp_id: str
    connector_id: int
    id_tag: str
    meter_start_wh: int
    started_reported_at: str
    reservation_id: int | None = None


class _SessionsCloseRequest(BaseModel):
    transaction_id: int
    cp_id: str
    id_tag: str
    meter_stop_wh: int
    stopped_reported_at: str
    stop_reason: str | None = None
    transaction_data: list[dict[str, Any]] | None = None


class _ChargePointRegisterRequest(BaseModel):
    cp_id: str
    vendor: str | None = None
    model: str | None = None
    firmware_version: str | None = None
    serial_number: str | None = None
    boot_at: str


# ---- Helpers ---------------------------------------------------------------


def _envelope_ok(data: dict[str, Any], message: str) -> dict[str, Any]:
    return {"success": True, "data": data, "message": message}


def _envelope_err(message: str, error_code: str) -> dict[str, Any]:
    return {
        "success": False,
        "data": None,
        "message": message,
        "error_code": error_code,
    }


def _body_hash(body: bytes) -> str:
    """Stable SHA-256 of the request body for idempotency-conflict
    detection."""
    return hashlib.sha256(body).hexdigest()


def _request_id_from_headers(x_request_id: str | None) -> str:
    return x_request_id if x_request_id else str(uuid.uuid4())


# ---- App factory -----------------------------------------------------------


def build_app(config: MockBackendConfig | None = None) -> FastAPI:
    """Build a fresh FastAPI app bound to the given config.

    Tests use this to inject custom behaviour (blocked id_tags,
    forced 503s) without touching env vars. The standalone entry
    point uses ``MockBackendConfig.from_env()``.
    """
    cfg = config or MockBackendConfig.from_env()
    app = FastAPI(
        title="Eveys backend (mock)",
        version="0.1.0",
        description=(
            "Dev-only mock implementing `docs/integration/"
            "01-backend-rest-contract.md`. Not for production use."
        ),
    )

    # Idempotency cache: key -> (body_hash, response_dict).
    # Replay with same key + same body returns the stored response.
    # Replay with same key + different body returns 409 Conflict.
    idempotency_cache: dict[str, tuple[str, dict[str, Any]]] = {}

    # ---- Auth dependency ---------------------------------------------------

    def _check_auth(authorization: str | None) -> None:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=_envelope_err("missing bearer token", "UNAUTHORIZED"),
            )
        token = authorization[len("Bearer ") :]
        if token != cfg.bearer_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=_envelope_err("invalid bearer token", "UNAUTHORIZED"),
            )

    # ---- Idempotency wrapper ----------------------------------------------

    async def _with_idempotency(
        request: Request,
        idempotency_key: str | None,
        compute: Any,
    ) -> dict[str, Any]:
        """Run ``compute()`` if the idempotency key is new; replay if
        seen with same body; raise 409 if seen with different body.

        ``compute`` is an awaitable returning the response dict.
        """
        body = await request.body()
        if idempotency_key is None:
            return await compute()

        existing = idempotency_cache.get(idempotency_key)
        body_hash = _body_hash(body)
        if existing is not None:
            stored_hash, stored_response = existing
            if stored_hash == body_hash:
                return stored_response
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=_envelope_err(
                    "idempotency key reused with different body",
                    "IDEMPOTENCY_CONFLICT",
                ),
            )

        response = await compute()
        idempotency_cache[idempotency_key] = (body_hash, response)
        return response

    # ---- Endpoints ---------------------------------------------------------

    @app.exception_handler(HTTPException)
    async def _http_exc_handler(_req: Request, exc: HTTPException) -> JSONResponse:
        # FastAPI's default wraps `detail` under {"detail": ...}; we want the
        # canonical envelope returned verbatim.
        if isinstance(exc.detail, dict):
            return JSONResponse(status_code=exc.status_code, content=exc.detail)
        return JSONResponse(
            status_code=exc.status_code,
            content=_envelope_err(str(exc.detail), "INTERNAL_ERROR"),
        )

    @app.post("/api/eveys/authorize")
    async def authorize(
        request: Request,
        body: _AuthorizeRequest,
        authorization: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ) -> JSONResponse:
        _check_auth(authorization)

        if cfg.fail_authorize:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content=_envelope_err(
                    "authorize subsystem is failing (test mode)",
                    "DB_UNAVAILABLE",
                ),
            )

        request_id = _request_id_from_headers(x_request_id)

        async def _compute() -> dict[str, Any]:
            if not cfg.force_accept_all and body.id_tag in cfg.blocked_id_tags:
                response = _envelope_ok(
                    {
                        "id_tag": body.id_tag,
                        "request_id": request_id,
                        "id_tag_info": {
                            "status": "Blocked",
                            "parent_id_tag": None,
                            "expiry_date": None,
                        },
                    },
                    "id_tag is blocked",
                )
            else:
                response = _envelope_ok(
                    {
                        "id_tag": body.id_tag,
                        "request_id": request_id,
                        "id_tag_info": {
                            "status": "Accepted",
                            "parent_id_tag": None,
                            "expiry_date": None,
                        },
                    },
                    "Authorization granted",
                )
            return response

        result = await _with_idempotency(request, idempotency_key, _compute)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=result,
            headers={"X-Request-ID": request_id},
        )

    @app.post("/api/eveys/sessions/open")
    async def sessions_open(
        request: Request,
        body: _SessionsOpenRequest,
        authorization: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ) -> JSONResponse:
        _check_auth(authorization)
        request_id = _request_id_from_headers(x_request_id)

        async def _compute() -> dict[str, Any]:
            return _envelope_ok(
                {
                    "transaction_id": body.transaction_id,
                    "request_id": request_id,
                    "command_id": _stable_command_id(body.transaction_id),
                    "id_tag_info": {
                        "status": "Accepted",
                        "parent_id_tag": None,
                        "expiry_date": None,
                    },
                },
                "Session opened",
            )

        result = await _with_idempotency(request, idempotency_key, _compute)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=result,
            headers={"X-Request-ID": request_id},
        )

    @app.post("/api/eveys/sessions/close")
    async def sessions_close(
        request: Request,
        body: _SessionsCloseRequest,
        authorization: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ) -> JSONResponse:
        _check_auth(authorization)
        request_id = _request_id_from_headers(x_request_id)

        async def _compute() -> dict[str, Any]:
            return _envelope_ok(
                {
                    "transaction_id": body.transaction_id,
                    "request_id": request_id,
                    "command_id": _stable_command_id(body.transaction_id),
                    "id_tag_info": {
                        "status": "Accepted",
                        "parent_id_tag": None,
                        "expiry_date": None,
                    },
                },
                "Session closed",
            )

        result = await _with_idempotency(request, idempotency_key, _compute)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=result,
            headers={"X-Request-ID": request_id},
        )

    @app.post("/api/eveys/charge-points/register")
    async def charge_points_register(
        request: Request,
        body: _ChargePointRegisterRequest,
        authorization: str | None = Header(default=None),
        x_request_id: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ) -> JSONResponse:
        _check_auth(authorization)
        request_id = _request_id_from_headers(x_request_id)

        async def _compute() -> dict[str, Any]:
            return _envelope_ok(
                {
                    "cp_id": body.cp_id,
                    "request_id": request_id,
                    "command_id": _stable_command_id(body.cp_id),
                    "registration_status": "Accepted",
                    "heartbeat_interval_seconds": cfg.heartbeat_interval_seconds,
                },
                "Charge point registered",
            )

        result = await _with_idempotency(request, idempotency_key, _compute)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=result,
            headers={"X-Request-ID": request_id},
        )

    @app.get("/api/eveys/health")
    async def health(
        x_request_id: str | None = Header(default=None),
    ) -> JSONResponse:
        request_id = _request_id_from_headers(x_request_id)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=_envelope_ok(
                {
                    "status": "ok",
                    "version": app.version,
                    "request_id": request_id,
                },
                "ok",
            ),
            headers={"X-Request-ID": request_id},
        )

    return app


def _stable_command_id(seed: object) -> int:
    """Deterministic int derived from a seed.

    Real backend assigns a DB id; we derive one from the inbound key
    so replays with the same body see the same command_id.
    """
    h = hashlib.sha256(json.dumps(seed, default=str).encode("utf-8")).digest()
    return int.from_bytes(h[:6], "big")


# Module-level app for `uvicorn tests.mock_backend.app:app` etc.
app = build_app()
