"""Unit tests for the Authorize handler.

Two regimes:

1. **No backend client wired** (W1 dev / unit tests without
   `backend_base_url`): the handler returns `Accepted` for any
   id_tag — useful for protocol-level testing without a backend.
2. **Backend client wired** (E3-3 production path): the handler
   delegates to `BackendHTTPClient.authorize` and forwards the
   resulting status verbatim. On `BackendUnavailableError`, the
   configured fallback policy applies.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16.enums import AuthorizationStatus

from eveys_ocpp.handlers.v16 import authorize
from eveys_ocpp.platform import (
    AuthorizeResult,
    BackendBusinessError,
    BackendCircuitOpenError,
    BackendNetworkError,
    BackendTimeoutError,
    IdTagInfo,
)


def _result(status: str, **info: object) -> AuthorizeResult:
    """Tiny helper — typed result with the given OCPP status."""
    return AuthorizeResult(
        id_tag=str(info.pop("id_tag", "RFID_X")),
        request_id=str(info.pop("request_id", "rid-test")),
        id_tag_info=IdTagInfo(
            status=status,
            parent_id_tag=info.get("parent_id_tag"),  # type: ignore[arg-type]
            expiry_date=info.get("expiry_date"),  # type: ignore[arg-type]
        ),
    )


# ---- No backend client wired ---------------------------------------------


@pytest.mark.asyncio
async def test_accepts_when_no_backend_client_wired(fake_cp: Any) -> None:
    """W1 / dev-laptop path: backend_client is None → stub Accepted.
    Lets the protocol layer be tested end-to-end without a backend."""
    fake_cp.backend_client = None

    result = await authorize.handle(fake_cp, id_tag="RFID_X")

    assert result.id_tag_info.status == AuthorizationStatus.accepted


# ---- Backend wired: forward charger replies verbatim ---------------------


@pytest.mark.asyncio
async def test_forwards_accepted_from_backend(fake_cp: Any) -> None:
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(
        return_value=_result("Accepted", parent_id_tag="FAMILY_007"),
    )

    result = await authorize.handle(fake_cp, id_tag="RFID_VIP", message_id="msg-1")

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    assert result.id_tag_info.parent_id_tag == "FAMILY_007"
    fake_cp.backend_client.authorize.assert_awaited_once()
    kwargs = fake_cp.backend_client.authorize.await_args.kwargs
    assert kwargs["id_tag"] == "RFID_VIP"
    assert kwargs["cp_id"] == "TEST_CP_001"
    # Idempotency-Key follows the documented shape.
    assert kwargs["idempotency_key"] == "ocpp-auth-TEST_CP_001-RFID_VIP-msg-1"


@pytest.mark.asyncio
async def test_forwards_blocked_from_backend(fake_cp: Any) -> None:
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(return_value=_result("Blocked"))

    result = await authorize.handle(fake_cp, id_tag="RFID_BAN")
    assert result.id_tag_info.status == AuthorizationStatus.blocked


@pytest.mark.asyncio
async def test_forwards_expired_from_backend(fake_cp: Any) -> None:
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(return_value=_result("Expired"))

    result = await authorize.handle(fake_cp, id_tag="RFID_OLD")
    assert result.id_tag_info.status == AuthorizationStatus.expired


@pytest.mark.asyncio
async def test_forwards_invalid_from_backend(fake_cp: Any) -> None:
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(return_value=_result("Invalid"))

    result = await authorize.handle(fake_cp, id_tag="RFID_BAD")
    assert result.id_tag_info.status == AuthorizationStatus.invalid


@pytest.mark.asyncio
async def test_forwards_concurrent_tx_from_backend(fake_cp: Any) -> None:
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(return_value=_result("ConcurrentTx"))

    result = await authorize.handle(fake_cp, id_tag="RFID_DUP")
    assert result.id_tag_info.status == AuthorizationStatus.concurrent_tx


@pytest.mark.asyncio
async def test_unknown_status_from_backend_maps_to_invalid(fake_cp: Any) -> None:
    """Forward-compat: a future backend that adds a new status doesn't
    crash the handler. Unknown → Invalid (safer than Accepted)."""
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(return_value=_result("SomeNewBackendStatus"))

    result = await authorize.handle(fake_cp, id_tag="RFID_X")
    assert result.id_tag_info.status == AuthorizationStatus.invalid


@pytest.mark.asyncio
async def test_business_error_from_backend_returns_invalid(fake_cp: Any) -> None:
    """e.g. UNKNOWN_ID_TAG from the backend — pass through as Invalid
    so the charger refuses the transaction. Don't surface the
    error_code to OCPP; the charger doesn't speak that language."""
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(
        side_effect=BackendBusinessError("unknown id_tag", error_code="UNKNOWN_ID_TAG"),
    )

    result = await authorize.handle(fake_cp, id_tag="RFID_X")
    assert result.id_tag_info.status == AuthorizationStatus.invalid


# ---- Fallback policy on backend-unavailable ------------------------------


@pytest.mark.asyncio
async def test_fallback_reject_returns_invalid_on_timeout(
    fake_cp: Any, settings_factory: Any
) -> None:
    """Default fallback `reject` → Invalid on timeout. Loud, safe."""
    fake_cp.settings = settings_factory(backend_authorize_fallback="reject")
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(
        side_effect=BackendTimeoutError("backend timed out"),
    )

    result = await authorize.handle(fake_cp, id_tag="RFID_X")
    assert result.id_tag_info.status == AuthorizationStatus.invalid


@pytest.mark.asyncio
async def test_fallback_reject_returns_invalid_on_circuit_open(
    fake_cp: Any, settings_factory: Any
) -> None:
    fake_cp.settings = settings_factory(backend_authorize_fallback="reject")
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(
        side_effect=BackendCircuitOpenError("circuit open"),
    )

    result = await authorize.handle(fake_cp, id_tag="RFID_X")
    assert result.id_tag_info.status == AuthorizationStatus.invalid


@pytest.mark.asyncio
async def test_fallback_reject_returns_invalid_on_network_error(
    fake_cp: Any, settings_factory: Any
) -> None:
    fake_cp.settings = settings_factory(backend_authorize_fallback="reject")
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(
        side_effect=BackendNetworkError("DNS"),
    )

    result = await authorize.handle(fake_cp, id_tag="RFID_X")
    assert result.id_tag_info.status == AuthorizationStatus.invalid


@pytest.mark.asyncio
async def test_fallback_accept_offline_returns_accepted_with_expiry(
    fake_cp: Any, settings_factory: Any
) -> None:
    """`accept_offline` policy → Accepted with a 5-min expiry the
    charger caches locally per OCPP § 4.2."""
    fake_cp.settings = settings_factory(backend_authorize_fallback="accept_offline")
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(
        side_effect=BackendTimeoutError("timeout"),
    )

    result = await authorize.handle(fake_cp, id_tag="RFID_X")
    assert result.id_tag_info.status == AuthorizationStatus.accepted
    # Expiry is set to ~5 minutes from now in ISO-8601 form. Just
    # check it's an ISO string (the exact value depends on now()).
    expiry = result.id_tag_info.expiry_date
    assert expiry is not None
    assert "T" in expiry
    assert expiry.endswith("+00:00")
