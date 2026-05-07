"""Async HTTP client for the backend integration (E3-2, ADR-0023).

Implements `docs/integration/01-backend-rest-contract.md`. One
client per gateway process; constructed at startup, closed at
shutdown. Stateful: holds an ``httpx.AsyncClient`` (HTTP/2,
connection pool, TLS settings) and a ``CircuitBreaker``.

The client unwraps the backend's response envelope at the boundary
so handlers consume typed dataclasses instead of dicts. Errors
raise typed exceptions from ``platform.errors`` so handlers map
fallback policy without inspecting HTTP statuses.

Per-call cross-cutting concerns (all enforced inside ``_request``):

- **Bearer auth** — ``Authorization: Bearer <token>`` from settings.
- **Correlation IDs** — generates ``X-Request-ID`` per call; binds
  to ``trace_id``/``request_id`` in structured logs.
- **Idempotency** — caller passes the key; the client forwards as
  ``Idempotency-Key`` header.
- **Retries** — only on 5xx + network/timeout. 4xx is non-retryable
  by definition (it's a business outcome the caller has to handle).
- **Circuit breaker** — wraps every call. Open breaker → short-circuit
  with ``BackendCircuitOpenError``.
- **Per-endpoint timeout** — passed via ``httpx.Timeout(...)``.

Nothing about OCPP fallback policy lives here — that's a handler
concern. The client just raises typed exceptions.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

import httpx

from eveys_ocpp.observability import get_logger
from eveys_ocpp.platform.circuit_breaker import CircuitBreaker
from eveys_ocpp.platform.errors import (
    BackendAuthError,
    BackendBusinessError,
    BackendNetworkError,
    BackendTimeoutError,
)
from eveys_ocpp.settings import Settings

log = get_logger(__name__)


# ---- Result dataclasses ----------------------------------------------------


@dataclass(frozen=True, slots=True)
class IdTagInfo:
    """Mirror of OCPP `IdTagInfo` (status/parent/expiry). The status
    is the OCPP wire string the gateway forwards verbatim to the
    charger as ``Authorize.conf.idTagInfo.status``."""

    status: str
    parent_id_tag: str | None = None
    expiry_date: str | None = None


@dataclass(frozen=True, slots=True)
class AuthorizeResult:
    id_tag: str
    id_tag_info: IdTagInfo
    request_id: str


@dataclass(frozen=True, slots=True)
class SessionOpenResult:
    transaction_id: int
    id_tag_info: IdTagInfo
    request_id: str
    command_id: int | None = None


@dataclass(frozen=True, slots=True)
class SessionCloseResult:
    transaction_id: int
    id_tag_info: IdTagInfo
    request_id: str
    command_id: int | None = None


@dataclass(frozen=True, slots=True)
class ChargePointRegisterResult:
    cp_id: str
    registration_status: str  # "Accepted" / "Pending" / "Rejected"
    heartbeat_interval_seconds: int
    request_id: str
    command_id: int | None = None


# ---- Helpers ---------------------------------------------------------------


def _new_request_id() -> str:
    return str(uuid.uuid4())


def _id_tag_info_from(payload: dict[str, Any]) -> IdTagInfo:
    return IdTagInfo(
        status=str(payload.get("status") or ""),
        parent_id_tag=payload.get("parent_id_tag") or None,
        expiry_date=payload.get("expiry_date") or None,
    )


# ---- The client ------------------------------------------------------------


class BackendHTTPClient:
    """Single instance per gateway process. Construct via
    ``BackendHTTPClient.from_settings(settings)``; close via ``aclose()``.

    Methods mirror the five backend-side endpoints from
    `docs/integration/01-backend-rest-contract.md`. Each returns a
    typed result on success and raises a typed exception otherwise.
    """

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        http: httpx.AsyncClient,
        breaker: CircuitBreaker,
        settings: Settings,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._http = http
        self._breaker = breaker
        self._settings = settings

    @classmethod
    def from_settings(cls, settings: Settings) -> BackendHTTPClient:
        """Build a client from project Settings.

        The httpx.AsyncClient inherits settings.backend_base_url as
        its `base_url`; the client's per-method paths are relative.
        Default per-call timeout is the largest endpoint timeout —
        each method overrides via `httpx.Timeout` per request.
        """
        if not settings.outbound_tls_verify:
            # Loud warning — a False verify in production silently
            # disables a real security control. Logged once at boot so
            # the value is grep-able in container logs and alertable
            # via Prometheus log-counter rules. The webhook dispatcher
            # logs the same warning when it starts; both legs together
            # mean a misconfigured production never fails silent.
            log.warning(
                "backend.tls_verify_disabled",
                detail=(
                    "EVEYS_OCPP_OUTBOUND_TLS_VERIFY=False — accepting "
                    "any TLS cert on the backend leg. Acceptable for "
                    "local dev (self-signed toger.test); never in "
                    "production."
                ),
            )
        http = httpx.AsyncClient(
            base_url=settings.backend_base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {settings.backend_token}"},
            timeout=httpx.Timeout(settings.backend_timeout_default_seconds),
            verify=settings.outbound_tls_verify,
        )
        breaker = CircuitBreaker(
            name="backend",
            threshold=settings.backend_circuit_breaker_threshold,
            cooldown_seconds=settings.backend_circuit_breaker_cooldown_seconds,
        )
        return cls(
            base_url=settings.backend_base_url,
            token=settings.backend_token,
            http=http,
            breaker=breaker,
            settings=settings,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    @property
    def breaker_state(self) -> str:
        """Exposed for ops health endpoints / tests."""
        return self._breaker.state

    # ---- HTTP plumbing -----------------------------------------------------

    async def _request(
        self,
        *,
        method: str,
        path: str,
        timeout_seconds: float,
        max_retries: int,
        json_body: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Issue one logical request with retry + breaker.

        Returns the parsed envelope `data` field on success
        (`{"success": true, ...}` is unwrapped here). Raises typed
        exceptions otherwise:

        - 4xx with `success=false` → BackendBusinessError (or
          BackendAuthError on 401/403). Not retried.
        - 5xx, network failure, timeout → counted as breaker failure;
          retried up to `max_retries`; final failure raises a
          `BackendUnavailableError` subclass.
        """
        await self._breaker.before_call()

        request_id = _new_request_id()
        headers: dict[str, str] = {"X-Request-ID": request_id}
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key

        last_exc: Exception | None = None
        for attempt in range(max_retries + 1):
            try:
                response = await self._http.request(
                    method,
                    path,
                    json=json_body,
                    headers=headers,
                    timeout=httpx.Timeout(timeout_seconds),
                )
            except httpx.TimeoutException as exc:
                last_exc = exc
                log.warning(
                    "backend.timeout",
                    path=path,
                    attempt=attempt,
                    request_id=request_id,
                    error=str(exc),
                )
                if attempt < max_retries:
                    continue
                await self._breaker.record_failure()
                raise BackendTimeoutError(
                    f"backend timeout after {max_retries + 1} attempts on {path}"
                ) from exc
            except httpx.HTTPError as exc:
                last_exc = exc
                log.warning(
                    "backend.network_error",
                    path=path,
                    attempt=attempt,
                    request_id=request_id,
                    error=str(exc),
                )
                if attempt < max_retries:
                    continue
                await self._breaker.record_failure()
                raise BackendNetworkError(
                    f"backend network error after {max_retries + 1} attempts on {path}"
                ) from exc

            status = response.status_code

            # 4xx — non-retryable business / auth errors. Surface immediately.
            if 400 <= status < 500:
                envelope = _safe_envelope(response)
                error_code = str(envelope.get("error_code") or "BAD_REQUEST")
                message = str(envelope.get("message") or response.text)
                # 4xx is *not* a transport failure; don't trip the
                # breaker on it (the backend is healthy, the caller
                # is wrong).
                await self._breaker.record_success()
                if status in (401, 403):
                    raise BackendAuthError(message, error_code=error_code, http_status=status)
                raise BackendBusinessError(message, error_code=error_code, http_status=status)

            # 5xx — retryable transport failure.
            if status >= 500:
                envelope = _safe_envelope(response)
                error_code = str(envelope.get("error_code") or "INTERNAL_ERROR")
                last_exc = BackendNetworkError(f"backend {status} on {path}", error_code=error_code)
                log.warning(
                    "backend.server_error",
                    path=path,
                    attempt=attempt,
                    status=status,
                    error_code=error_code,
                    request_id=request_id,
                )
                if attempt < max_retries:
                    continue
                await self._breaker.record_failure()
                raise last_exc

            # 2xx — happy path.
            envelope = _safe_envelope(response)
            await self._breaker.record_success()
            if not envelope.get("success"):
                # 200 OK with `success: false` is the business-rejection
                # path documented in the contract. Treat the same as 4xx:
                # surface as BackendBusinessError so the caller can
                # decide what to do — the "rejection" is semantic, not
                # transport, and shouldn't trip the breaker.
                error_code = str(envelope.get("error_code") or "BUSINESS_REJECTION")
                message = str(envelope.get("message") or "rejected")
                raise BackendBusinessError(message, error_code=error_code, http_status=status)
            data = envelope.get("data")
            if not isinstance(data, dict):
                raise BackendBusinessError(
                    "backend returned success but `data` was missing or non-object",
                    error_code="MALFORMED_RESPONSE",
                    http_status=status,
                )
            return data

        # Unreachable — the loop either returns or raises.
        raise last_exc or BackendNetworkError("unreachable: retry loop fell through")

    # ---- Endpoint methods --------------------------------------------------

    async def authorize(
        self,
        *,
        id_tag: str,
        cp_id: str,
        idempotency_key: str | None = None,
    ) -> AuthorizeResult:
        data = await self._request(
            method="POST",
            path="/authorize",
            timeout_seconds=self._settings.backend_timeout_authorize_seconds,
            max_retries=self._settings.backend_retry_attempts_authorize,
            json_body={"id_tag": id_tag, "cp_id": cp_id},
            idempotency_key=idempotency_key,
        )
        return AuthorizeResult(
            id_tag=str(data.get("id_tag") or id_tag),
            request_id=str(data.get("request_id") or ""),
            id_tag_info=_id_tag_info_from(data.get("id_tag_info") or {}),
        )

    async def open_session(
        self,
        *,
        transaction_id: int,
        cp_id: str,
        connector_id: int,
        id_tag: str,
        meter_start_wh: int,
        started_reported_at: str,
        reservation_id: int | None = None,
        idempotency_key: str | None = None,
    ) -> SessionOpenResult:
        body: dict[str, Any] = {
            "transaction_id": transaction_id,
            "cp_id": cp_id,
            "connector_id": connector_id,
            "id_tag": id_tag,
            "meter_start_wh": meter_start_wh,
            "started_reported_at": started_reported_at,
            "reservation_id": reservation_id,
        }
        data = await self._request(
            method="POST",
            path="/sessions/open",
            timeout_seconds=self._settings.backend_timeout_sessions_open_seconds,
            max_retries=self._settings.backend_retry_attempts_sessions_open,
            json_body=body,
            idempotency_key=idempotency_key,
        )
        return SessionOpenResult(
            transaction_id=int(data.get("transaction_id") or transaction_id),
            request_id=str(data.get("request_id") or ""),
            command_id=int(data["command_id"]) if data.get("command_id") is not None else None,
            id_tag_info=_id_tag_info_from(data.get("id_tag_info") or {}),
        )

    async def close_session(
        self,
        *,
        transaction_id: int,
        cp_id: str,
        id_tag: str,
        meter_stop_wh: int,
        stopped_reported_at: str,
        stop_reason: str | None = None,
        transaction_data: list[dict[str, Any]] | None = None,
        idempotency_key: str | None = None,
    ) -> SessionCloseResult:
        body: dict[str, Any] = {
            "transaction_id": transaction_id,
            "cp_id": cp_id,
            "id_tag": id_tag,
            "meter_stop_wh": meter_stop_wh,
            "stopped_reported_at": stopped_reported_at,
            "stop_reason": stop_reason,
            "transaction_data": transaction_data,
        }
        data = await self._request(
            method="POST",
            path="/sessions/close",
            timeout_seconds=self._settings.backend_timeout_sessions_close_seconds,
            max_retries=self._settings.backend_retry_attempts_sessions_close,
            json_body=body,
            idempotency_key=idempotency_key,
        )
        return SessionCloseResult(
            transaction_id=int(data.get("transaction_id") or transaction_id),
            request_id=str(data.get("request_id") or ""),
            command_id=int(data["command_id"]) if data.get("command_id") is not None else None,
            id_tag_info=_id_tag_info_from(data.get("id_tag_info") or {}),
        )

    async def register_charge_point(
        self,
        *,
        cp_id: str,
        vendor: str | None,
        model: str | None,
        firmware_version: str | None,
        serial_number: str | None,
        boot_at: str,
        idempotency_key: str | None = None,
    ) -> ChargePointRegisterResult:
        data = await self._request(
            method="POST",
            path="/charge-points/register",
            timeout_seconds=self._settings.backend_timeout_default_seconds,
            max_retries=0,  # cold path; one shot, fall back on timeout
            json_body={
                "cp_id": cp_id,
                "vendor": vendor,
                "model": model,
                "firmware_version": firmware_version,
                "serial_number": serial_number,
                "boot_at": boot_at,
            },
            idempotency_key=idempotency_key,
        )
        return ChargePointRegisterResult(
            cp_id=str(data.get("cp_id") or cp_id),
            request_id=str(data.get("request_id") or ""),
            command_id=int(data["command_id"]) if data.get("command_id") is not None else None,
            registration_status=str(data.get("registration_status") or "Accepted"),
            heartbeat_interval_seconds=int(data.get("heartbeat_interval_seconds") or 60),
        )

    async def health(self) -> dict[str, Any]:
        """Health probe. No retries, short timeout."""
        return await self._request(
            method="GET",
            path="/health",
            timeout_seconds=self._settings.backend_timeout_default_seconds,
            max_retries=0,
        )


# ---- envelope parsing -------------------------------------------------------


def _safe_envelope(response: httpx.Response) -> dict[str, Any]:
    """Parse the response body as JSON; return an empty dict on
    parse failure so callers don't have to guard.

    The contract guarantees JSON envelopes on every response, but
    misbehaving backends may serve plain text (e.g. an upstream proxy's
    503 page). Treat that as `{}` so the caller's error path runs.
    """
    try:
        body = response.json()
    except Exception:
        return {}
    if not isinstance(body, dict):
        return {}
    return body
