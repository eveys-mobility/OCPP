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
    """W1 / local-dev path: backend_client is None → stub Accepted.
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


# ---- Cache integration (E3-4) -------------------------------------------


@pytest.mark.asyncio
async def test_cache_hit_skips_backend_call(fake_cp: Any) -> None:
    """Cache hit → forward the cached `IdTagInfo` directly; the
    backend client is never invoked. This is the latency-budget
    win the cache exists for."""
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(
        side_effect=AssertionError("backend must not be called on cache hit"),
    )
    fake_cp.authorize_cache = AsyncMock()
    fake_cp.authorize_cache.get = AsyncMock(
        return_value=IdTagInfo(status="Accepted", parent_id_tag="FAMILY"),
    )

    result = await authorize.handle(fake_cp, id_tag="RFID_VIP")

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    assert result.id_tag_info.parent_id_tag == "FAMILY"
    fake_cp.authorize_cache.get.assert_awaited_once_with(cp_id="TEST_CP_001", id_tag="RFID_VIP")
    fake_cp.backend_client.authorize.assert_not_awaited()


@pytest.mark.asyncio
async def test_cache_miss_falls_through_to_backend_and_caches_result(
    fake_cp: Any,
) -> None:
    """Cache miss → backend round-trip → cache.set() with the result."""
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(
        return_value=_result("Accepted", parent_id_tag="FAMILY"),
    )
    fake_cp.authorize_cache = AsyncMock()
    fake_cp.authorize_cache.get = AsyncMock(return_value=None)
    fake_cp.authorize_cache.set = AsyncMock()

    result = await authorize.handle(fake_cp, id_tag="RFID_VIP")

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    fake_cp.backend_client.authorize.assert_awaited_once()
    # Cache populated with the freshly-resolved result.
    fake_cp.authorize_cache.set.assert_awaited_once()
    set_kwargs = fake_cp.authorize_cache.set.await_args.kwargs
    assert set_kwargs["cp_id"] == "TEST_CP_001"
    assert set_kwargs["id_tag"] == "RFID_VIP"
    assert set_kwargs["info"].status == "Accepted"


@pytest.mark.asyncio
async def test_cache_caches_blocked_outcome_too(fake_cp: Any) -> None:
    """Caching `Blocked` is just as valuable as caching `Accepted` —
    refuses repeated taps from a known-bad tag without a backend
    round-trip per tap."""
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(return_value=_result("Blocked"))
    fake_cp.authorize_cache = AsyncMock()
    fake_cp.authorize_cache.get = AsyncMock(return_value=None)
    fake_cp.authorize_cache.set = AsyncMock()

    result = await authorize.handle(fake_cp, id_tag="RFID_BAD")
    assert result.id_tag_info.status == AuthorizationStatus.blocked
    fake_cp.authorize_cache.set.assert_awaited_once()
    set_kwargs = fake_cp.authorize_cache.set.await_args.kwargs
    assert set_kwargs["info"].status == "Blocked"


@pytest.mark.asyncio
async def test_business_error_is_not_cached(fake_cp: Any) -> None:
    """`BackendBusinessError` (e.g. UNKNOWN_ID_TAG) → returns Invalid
    to the charger but does NOT poison the cache. A backend fix
    landing for the id_tag should be visible on the next tap, not
    after the cache TTL."""
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(
        side_effect=BackendBusinessError("unknown", error_code="UNKNOWN_ID_TAG"),
    )
    fake_cp.authorize_cache = AsyncMock()
    fake_cp.authorize_cache.get = AsyncMock(return_value=None)
    fake_cp.authorize_cache.set = AsyncMock()

    result = await authorize.handle(fake_cp, id_tag="RFID_X")
    assert result.id_tag_info.status == AuthorizationStatus.invalid
    fake_cp.authorize_cache.set.assert_not_awaited()


@pytest.mark.asyncio
async def test_unavailable_fallback_is_not_cached(fake_cp: Any, settings_factory: Any) -> None:
    """Fallback policy depends on current settings, not a stale call.
    The cache must not store a Backend*Error outcome — let the next
    tap re-roundtrip and re-evaluate.

    Pin `backend_authorize_fallback="reject"` explicitly: the local
    dev `.env` typically sets it to `accept_offline` (working config
    when the toger.test backend is down), which would flip the
    expected status from Invalid to Accepted and turn this test into
    a `.env`-presence flake. Sibling tests in this file already set
    the policy explicitly; this one was missed (#155). See #156 for
    the broader unit-suite hermeticity fix."""
    fake_cp.settings = settings_factory(backend_authorize_fallback="reject")
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(
        side_effect=BackendTimeoutError("timed out"),
    )
    fake_cp.authorize_cache = AsyncMock()
    fake_cp.authorize_cache.get = AsyncMock(return_value=None)
    fake_cp.authorize_cache.set = AsyncMock()

    result = await authorize.handle(fake_cp, id_tag="RFID_X")
    assert result.id_tag_info.status == AuthorizationStatus.invalid
    fake_cp.authorize_cache.set.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_cache_wired_falls_through_directly(fake_cp: Any) -> None:
    """`authorize_cache=None` (Redis not wired) → handler does the
    backend round-trip but doesn't try to cache. Same shape as the
    happy-path forward test, just confirming we don't try to call
    `.set()` on a missing cache."""
    fake_cp.backend_client = AsyncMock()
    fake_cp.backend_client.authorize = AsyncMock(return_value=_result("Accepted"))
    fake_cp.authorize_cache = None  # explicit

    result = await authorize.handle(fake_cp, id_tag="RFID_X")
    assert result.id_tag_info.status == AuthorizationStatus.accepted
