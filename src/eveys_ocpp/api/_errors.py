"""Error envelope + handler wiring for the gateway REST API.

Per `docs/integration/02-gateway-rest-api.md` and ADR-0023, every error
on this surface returns the shape:

    { "error": "<human msg>", "error_code": "<STABLE_CODE>", "request_id": "<uuid>" }

Stable error codes are documented in the contract; this module surfaces
them as constants so handlers don't open-code strings.

`request_id` is the value of the inbound `X-Request-ID` header (or a
freshly-generated UUID when missing). It's threaded into the request
state by `request_id_middleware` in `_app.py`.
"""

from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from eveys_ocpp.observability import get_logger

log = get_logger(__name__)


# Stable codes the contract promises. Add a new code here AND in
# `docs/integration/02-gateway-rest-api.md` § "Error responses" — the
# table there is the operator-facing contract.
ERR_BAD_REQUEST = "BAD_REQUEST"
ERR_UNAUTHORIZED = "UNAUTHORIZED"
ERR_FORBIDDEN = "FORBIDDEN"
ERR_UNKNOWN_CP_ID = "UNKNOWN_CP_ID"
ERR_UNKNOWN_TRANSACTION_ID = "UNKNOWN_TRANSACTION_ID"
ERR_UNKNOWN_RESERVATION_ID = "UNKNOWN_RESERVATION_ID"
ERR_CHARGER_OFFLINE = "CHARGER_OFFLINE"
ERR_CHARGER_TIMEOUT = "CHARGER_TIMEOUT"
ERR_WINDOW_TOO_LARGE = "WINDOW_TOO_LARGE"
ERR_RATE_LIMITED = "RATE_LIMITED"
ERR_INTERNAL_ERROR = "INTERNAL_ERROR"


class ApiError(HTTPException):
    """A REST error with a stable `error_code`.

    Routes raise this; the global handler turns it into the contract
    envelope. The HTTPException base lets FastAPI's middleware see it
    as an expected error (no 500 stack trace), and the `error_code`
    flows to the response body.
    """

    def __init__(self, *, status_code: int, error_code: str, message: str) -> None:
        super().__init__(status_code=status_code, detail=message)
        self.error_code = error_code
        self.message = message


def _request_id(request: Request) -> str:
    rid: Any = getattr(request.state, "request_id", None)
    return str(rid) if rid else ""


def _envelope(*, error_code: str, message: str, request_id: str) -> dict[str, Any]:
    return {"error": message, "error_code": error_code, "request_id": request_id}


async def api_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Map our typed `ApiError` to the contract envelope."""
    if not isinstance(exc, ApiError):  # pragma: no cover — defensive
        return await internal_error_handler(request, exc)
    return JSONResponse(
        status_code=exc.status_code,
        content=_envelope(
            error_code=exc.error_code,
            message=exc.message,
            request_id=_request_id(request),
        ),
    )


async def http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Map FastAPI/Starlette `HTTPException` (e.g. 404 from a missing
    route, 405 method-not-allowed) to the contract envelope.

    These are NOT routes raising `ApiError` — they're framework-level
    rejections. We give them a generic `error_code` matching their
    HTTP status."""
    if not isinstance(exc, HTTPException):  # pragma: no cover — defensive
        return await internal_error_handler(request, exc)
    code_by_status = {
        400: ERR_BAD_REQUEST,
        401: ERR_UNAUTHORIZED,
        403: ERR_FORBIDDEN,
        404: ERR_BAD_REQUEST,  # unknown route is a client mistake
        405: ERR_BAD_REQUEST,  # wrong method is a client mistake
        429: ERR_RATE_LIMITED,
    }
    error_code = code_by_status.get(exc.status_code, ERR_INTERNAL_ERROR)
    return JSONResponse(
        status_code=exc.status_code,
        content=_envelope(
            error_code=error_code,
            message=str(exc.detail) if exc.detail else "",
            request_id=_request_id(request),
        ),
    )


async def validation_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Pydantic body / query validation failures → 400 BAD_REQUEST.

    The default FastAPI shape is `{"detail": [...]}` which leaks
    Pydantic internals; we collapse to a single human message and the
    contract envelope so the spec stays clean."""
    if not isinstance(exc, RequestValidationError):  # pragma: no cover
        return await internal_error_handler(request, exc)
    # Take the first error's message — terse, matches the contract's
    # one-line `error` field. Backend logs see the full Pydantic detail
    # via the `validation_failed` log entry below.
    errors = exc.errors()
    first = errors[0] if errors else {}
    msg = first.get("msg", "validation failed")
    log.info("rest.validation_failed", error_count=len(errors), errors=errors)
    return JSONResponse(
        status_code=400,
        content=_envelope(
            error_code=ERR_BAD_REQUEST,
            message=str(msg),
            request_id=_request_id(request),
        ),
    )


async def internal_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Anything else → 500 INTERNAL_ERROR. The exception is logged with
    its traceback; the response body never leaks it to the client."""
    log.exception(
        "rest.internal_error",
        path=request.url.path,
        method=request.method,
        request_id=_request_id(request),
    )
    return JSONResponse(
        status_code=500,
        content=_envelope(
            error_code=ERR_INTERNAL_ERROR,
            message="internal server error",
            request_id=_request_id(request),
        ),
    )
