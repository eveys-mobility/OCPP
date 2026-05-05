"""Typed exceptions raised by ``BackendHTTPClient``.

The OCPP handlers catch these to decide on fallback policy without
inspecting HTTP status codes or `error_code` strings directly.

Hierarchy:

    BackendError                       (root — never raised directly)
    ├── BackendUnavailableError        (transport down, breaker open, retries exhausted)
    │   ├── BackendCircuitOpenError    (breaker is currently open)
    │   ├── BackendTimeoutError        (per-endpoint timeout exceeded after retries)
    │   └── BackendNetworkError        (connect / read / DNS failure)
    └── BackendBusinessError           (backend replied 4xx with a stable error_code)
        └── BackendAuthError           (401 / 403 — caller's bearer token is wrong)

Hot-path callers (handlers) should treat ``BackendUnavailableError`` as
"fall back to the configured offline policy" and ``BackendBusinessError``
as "the request itself is wrong; stop retrying."
"""

from __future__ import annotations


class BackendError(Exception):
    """Base for everything the BackendHTTPClient raises."""


class BackendUnavailableError(BackendError):
    """Backend couldn't service the request — transport-level fault.

    Carries an optional ``error_code`` if the backend returned a
    JSON envelope on its way out (e.g. the 503 path returns
    ``{"error_code": "DB_UNAVAILABLE"}``); otherwise None when the
    failure was network / timeout / breaker.
    """

    def __init__(self, message: str, *, error_code: str | None = None) -> None:
        super().__init__(message)
        self.error_code = error_code


class BackendCircuitOpenError(BackendUnavailableError):
    """The circuit breaker is open — short-circuiting the call.

    The breaker opens after ``backend_circuit_breaker_threshold``
    consecutive failures. Handlers should fall back immediately;
    no retry will help.
    """


class BackendTimeoutError(BackendUnavailableError):
    """Request didn't complete within the per-endpoint timeout
    (after the retry budget). The breaker counts this as a failure."""


class BackendNetworkError(BackendUnavailableError):
    """Connect / read / DNS failure. Counts as a breaker failure."""


class BackendBusinessError(BackendError):
    """Backend understood the request and rejected it with a stable
    ``error_code`` (e.g. ``UNKNOWN_ID_TAG``, ``IDEMPOTENCY_CONFLICT``).
    Not retryable — the caller's request is malformed or the resource
    doesn't exist.
    """

    def __init__(self, message: str, *, error_code: str, http_status: int | None = None) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.http_status = http_status


class BackendAuthError(BackendBusinessError):
    """401 / 403. Either bearer token is missing/invalid or scope is
    insufficient. Operations should fix the credential, not retry."""
