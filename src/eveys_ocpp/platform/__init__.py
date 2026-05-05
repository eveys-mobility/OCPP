"""Backend integration layer (E3-2, ADR-0023).

Async HTTP client + typed result dataclasses + typed exceptions for
the contract in `docs/integration/01-backend-rest-contract.md`.

Public surface:

    from eveys_ocpp.platform import (
        BackendHTTPClient,                  # the client
        AuthorizeResult, IdTagInfo,         # result dataclasses
        SessionOpenResult, SessionCloseResult, ChargePointRegisterResult,
        BackendError,                       # exception hierarchy
        BackendUnavailableError, BackendCircuitOpenError,
        BackendTimeoutError, BackendNetworkError,
        BackendBusinessError, BackendAuthError,
    )

The OCPP handlers in `eveys_ocpp.handlers.v16.*` will import from
this module in E3-3..E3-6.
"""

from __future__ import annotations

from .client import (
    AuthorizeResult,
    BackendHTTPClient,
    ChargePointRegisterResult,
    IdTagInfo,
    SessionCloseResult,
    SessionOpenResult,
)
from .errors import (
    BackendAuthError,
    BackendBusinessError,
    BackendCircuitOpenError,
    BackendError,
    BackendNetworkError,
    BackendTimeoutError,
    BackendUnavailableError,
)

__all__ = [
    "AuthorizeResult",
    "BackendAuthError",
    "BackendBusinessError",
    "BackendCircuitOpenError",
    "BackendError",
    "BackendHTTPClient",
    "BackendNetworkError",
    "BackendTimeoutError",
    "BackendUnavailableError",
    "ChargePointRegisterResult",
    "IdTagInfo",
    "SessionCloseResult",
    "SessionOpenResult",
]
