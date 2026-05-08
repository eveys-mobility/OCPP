"""Unit tests for `BackendHTTPClient`.

Strategy: drive the client against the in-repo mock backend
(`tests/mock_backend`) via `httpx.ASGITransport`. Same network
boundary the real backend will sit behind; same envelope shape;
no subprocess overhead per test.

For failure-mode coverage (timeouts, network errors, 5xx, breaker)
we use a stub `httpx.MockTransport` that returns the exact response
the test wants — no need to coax the mock backend into misbehaving.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
import pytest

from eveys_ocpp.platform.circuit_breaker import CircuitBreaker
from eveys_ocpp.platform.client import BackendHTTPClient
from eveys_ocpp.platform.errors import (
    BackendAuthError,
    BackendBusinessError,
    BackendCircuitOpenError,
    BackendNetworkError,
    BackendTimeoutError,
)
from eveys_ocpp.settings import Settings
from tests.mock_backend import build_app
from tests.mock_backend.config import MockBackendConfig

_TOKEN = "platform-test-token"


# ---- Helpers ---------------------------------------------------------------


def _settings_for_test(**overrides: Any) -> Settings:
    """Build a Settings with backend integration enabled and short
    timeouts so failure tests don't slow down the suite."""
    base = {
        "backend_base_url": "http://mock/api/eveys",
        "backend_token": _TOKEN,
        "backend_timeout_authorize_seconds": 1.0,
        "backend_timeout_sessions_open_seconds": 1.0,
        "backend_timeout_sessions_close_seconds": 1.0,
        "backend_timeout_default_seconds": 1.0,
        "backend_retry_attempts_authorize": 0,
        "backend_retry_attempts_sessions_open": 0,
        "backend_retry_attempts_sessions_close": 0,
        "backend_circuit_breaker_threshold": 3,
        "backend_circuit_breaker_cooldown_seconds": 30.0,
    }
    base.update(overrides)
    # `_env_file=None` so a developer's local `.env` (which may flip
    # `outbound_tls_verify=false` for self-signed dev backends) doesn't
    # leak in and fail TLS-defaults-related tests. CI has no `.env`
    # checked in, so this only manifests locally.
    return Settings(_env_file=None, **base)


def _client_against_mock(
    config: MockBackendConfig | None = None, settings: Settings | None = None
) -> BackendHTTPClient:
    """Wire the platform client to the in-process mock backend."""
    settings = settings or _settings_for_test()
    config = config or MockBackendConfig(bearer_token=_TOKEN)
    app = build_app(config)
    http = httpx.AsyncClient(
        base_url="http://mock/api/eveys",
        headers={"Authorization": f"Bearer {settings.backend_token.get_secret_value()}"},
        transport=httpx.ASGITransport(app=app),
        timeout=httpx.Timeout(settings.backend_timeout_default_seconds),
    )
    breaker = CircuitBreaker(
        name="backend",
        threshold=settings.backend_circuit_breaker_threshold,
        cooldown_seconds=settings.backend_circuit_breaker_cooldown_seconds,
    )
    return BackendHTTPClient(
        base_url=settings.backend_base_url,
        token=settings.backend_token.get_secret_value(),
        http=http,
        breaker=breaker,
        settings=settings,
    )


def _client_with_transport(
    handler: Callable[[httpx.Request], httpx.Response],
    settings: Settings | None = None,
) -> BackendHTTPClient:
    """Wire the client to a `MockTransport` for failure-mode tests
    where the response (or exception) is hand-crafted."""
    settings = settings or _settings_for_test()
    http = httpx.AsyncClient(
        base_url="http://mock/api/eveys",
        headers={"Authorization": f"Bearer {settings.backend_token.get_secret_value()}"},
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(settings.backend_timeout_default_seconds),
    )
    breaker = CircuitBreaker(
        name="backend",
        threshold=settings.backend_circuit_breaker_threshold,
        cooldown_seconds=settings.backend_circuit_breaker_cooldown_seconds,
    )
    return BackendHTTPClient(
        base_url=settings.backend_base_url,
        token=settings.backend_token.get_secret_value(),
        http=http,
        breaker=breaker,
        settings=settings,
    )


@pytest.fixture
async def mock_client() -> AsyncIterator[BackendHTTPClient]:
    client = _client_against_mock()
    try:
        yield client
    finally:
        await client.aclose()


# ---- Happy path: each endpoint -------------------------------------------


@pytest.mark.asyncio
async def test_authorize_returns_typed_result_with_id_tag_info(
    mock_client: BackendHTTPClient,
) -> None:
    result = await mock_client.authorize(id_tag="RFID_HAPPY", cp_id="CP_001")
    assert result.id_tag == "RFID_HAPPY"
    assert result.id_tag_info.status == "Accepted"
    # request_id is whatever the mock echoed; just non-empty.
    assert result.request_id


@pytest.mark.asyncio
async def test_authorize_blocked_id_tag_returns_blocked_status() -> None:
    """Mock blocks RFID_BLOCKED → backend replies success=true with
    `id_tag_info.status="Blocked"` (a business outcome, not an error).
    The client returns the typed result; the handler maps it."""
    settings = _settings_for_test()
    config = MockBackendConfig(bearer_token=_TOKEN, blocked_id_tags=frozenset({"RFID_BLOCKED"}))
    client = _client_against_mock(config=config, settings=settings)
    try:
        result = await client.authorize(id_tag="RFID_BLOCKED", cp_id="CP_001")
        assert result.id_tag_info.status == "Blocked"
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_open_session_returns_command_id_and_status(
    mock_client: BackendHTTPClient,
) -> None:
    result = await mock_client.open_session(
        transaction_id=12345,
        cp_id="CP_001",
        connector_id=1,
        id_tag="RFID_HAPPY",
        meter_start_wh=0,
        started_reported_at="2026-05-05T14:00:00+00:00",
    )
    assert result.transaction_id == 12345
    assert result.id_tag_info.status == "Accepted"
    # The mock returns a stable command_id derived from the body.
    assert isinstance(result.command_id, int)


@pytest.mark.asyncio
async def test_close_session_returns_typed_result(mock_client: BackendHTTPClient) -> None:
    result = await mock_client.close_session(
        transaction_id=12345,
        cp_id="CP_001",
        id_tag="RFID_HAPPY",
        meter_stop_wh=12345,
        stopped_reported_at="2026-05-05T15:00:00+00:00",
        stop_reason="Local",
    )
    assert result.transaction_id == 12345
    assert result.id_tag_info.status == "Accepted"


@pytest.mark.asyncio
async def test_register_charge_point_returns_heartbeat_interval(
    mock_client: BackendHTTPClient,
) -> None:
    result = await mock_client.register_charge_point(
        cp_id="CP_NEW",
        vendor="ACME",
        model="X1",
        firmware_version="1.0",
        serial_number="SN-001",
        boot_at="2026-05-05T14:00:00+00:00",
    )
    assert result.cp_id == "CP_NEW"
    assert result.registration_status == "Accepted"
    assert result.heartbeat_interval_seconds == 60


@pytest.mark.asyncio
async def test_health_returns_envelope_data(mock_client: BackendHTTPClient) -> None:
    data = await mock_client.health()
    assert data["status"] == "ok"


# ---- Auth + business errors ----------------------------------------------


@pytest.mark.asyncio
async def test_wrong_token_raises_auth_error() -> None:
    """Mock issues 401 on a bad token; client raises BackendAuthError."""
    settings = _settings_for_test(backend_token="wrong-token")
    client = _client_against_mock(settings=settings)
    try:
        with pytest.raises(BackendAuthError) as exc:
            await client.authorize(id_tag="RFID_X", cp_id="CP_001")
        assert exc.value.error_code == "UNAUTHORIZED"
        assert exc.value.http_status == 401
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_backend_503_raises_network_error_after_retries() -> None:
    """503 from the backend → BackendNetworkError after the retry
    budget. Counts toward the breaker."""
    settings = _settings_for_test()
    config = MockBackendConfig(bearer_token=_TOKEN, fail_authorize=True)
    client = _client_against_mock(config=config, settings=settings)
    try:
        with pytest.raises(BackendNetworkError) as exc:
            await client.authorize(id_tag="RFID_X", cp_id="CP_001")
        assert exc.value.error_code == "DB_UNAVAILABLE"
    finally:
        await client.aclose()


# ---- Retry behaviour -----------------------------------------------------


@pytest.mark.asyncio
async def test_5xx_is_retried_then_succeeds() -> None:
    """First call returns 503; retry returns 200 — the result is the
    successful one and no exception is raised."""
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return httpx.Response(503, json={"success": False, "error_code": "TEMP"})
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "id_tag": "RFID_X",
                    "request_id": "rid-1",
                    "id_tag_info": {"status": "Accepted"},
                },
                "message": "ok",
            },
        )

    settings = _settings_for_test(backend_retry_attempts_authorize=1)
    client = _client_with_transport(handler, settings)
    try:
        result = await client.authorize(id_tag="RFID_X", cp_id="CP_001")
        assert result.id_tag_info.status == "Accepted"
        assert call_count["n"] == 2
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_4xx_is_not_retried() -> None:
    """400 → no retry; immediate BackendBusinessError. Breaker isn't tripped."""
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return httpx.Response(
            400,
            json={"success": False, "error_code": "BAD_REQUEST", "message": "bad"},
        )

    settings = _settings_for_test(backend_retry_attempts_authorize=3)
    client = _client_with_transport(handler, settings)
    try:
        with pytest.raises(BackendBusinessError) as exc:
            await client.authorize(id_tag="RFID_X", cp_id="CP_001")
        assert exc.value.error_code == "BAD_REQUEST"
        assert call_count["n"] == 1  # not retried
        # Breaker still closed — 4xx isn't a transport failure.
        assert client.breaker_state == "closed"
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_timeout_after_retries_raises_timeout_error() -> None:
    """Every attempt times out → BackendTimeoutError. The breaker
    counts this as one failure (the whole logical call)."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("simulated timeout")

    settings = _settings_for_test(backend_retry_attempts_authorize=2)
    client = _client_with_transport(handler, settings)
    try:
        with pytest.raises(BackendTimeoutError):
            await client.authorize(id_tag="RFID_X", cp_id="CP_001")
        # One logical call → one breaker failure regardless of retry count.
        assert client.breaker_state == "closed"  # below threshold
    finally:
        await client.aclose()


# ---- Circuit breaker integration ----------------------------------------


@pytest.mark.asyncio
async def test_breaker_opens_after_consecutive_503s() -> None:
    """Threshold=3 consecutive 503s → breaker opens; the next call
    short-circuits with BackendCircuitOpenError."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"success": False, "error_code": "DOWN"})

    settings = _settings_for_test(
        backend_retry_attempts_authorize=0, backend_circuit_breaker_threshold=3
    )
    client = _client_with_transport(handler, settings)
    try:
        for _ in range(3):
            with pytest.raises(BackendNetworkError):
                await client.authorize(id_tag="RFID_X", cp_id="CP_001")
        assert client.breaker_state == "open"

        # Subsequent call short-circuits — handler isn't even hit.
        with pytest.raises(BackendCircuitOpenError):
            await client.authorize(id_tag="RFID_X", cp_id="CP_001")
    finally:
        await client.aclose()


# ---- Headers / correlation ----------------------------------------------


@pytest.mark.asyncio
async def test_request_includes_bearer_and_x_request_id() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers.get("authorization", "")
        captured["x_request_id"] = request.headers.get("x-request-id", "")
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "id_tag": "RFID_X",
                    "request_id": "echoed",
                    "id_tag_info": {"status": "Accepted"},
                },
                "message": "ok",
            },
        )

    client = _client_with_transport(handler)
    try:
        await client.authorize(id_tag="RFID_X", cp_id="CP_001")
        assert captured["authorization"] == f"Bearer {_TOKEN}"
        assert captured["x_request_id"]  # uuid generated per call
    finally:
        await client.aclose()


def test_from_settings_propagates_tls_verify_flag() -> None:
    """`outbound_tls_verify=False` (local-dev override for self-signed
    backend certs) must reach the underlying httpx client. httpx stores
    `verify` on its transport's SSL context — when verify=False, the
    context's verify_mode is CERT_NONE."""
    import asyncio
    import ssl

    settings = Settings(
        _env_file=None,
        backend_base_url="https://backend.test",
        backend_token="t",
        outbound_tls_verify=False,
    )
    client = BackendHTTPClient.from_settings(settings)
    try:
        transport = client._http._transport
        ctx = transport._pool._ssl_context  # type: ignore[attr-defined]
        assert ctx.verify_mode == ssl.CERT_NONE
    finally:
        asyncio.run(client.aclose())


def test_from_settings_keeps_tls_verify_on_by_default() -> None:
    """The default must be True — production must not regress to a
    permissive setting because someone forgot to set the env var."""
    import asyncio
    import ssl

    settings = Settings(
        _env_file=None,
        backend_base_url="https://backend.test",
        backend_token="t",
    )
    client = BackendHTTPClient.from_settings(settings)
    try:
        transport = client._http._transport
        ctx = transport._pool._ssl_context  # type: ignore[attr-defined]
        # CERT_REQUIRED is httpx's production-safe mode for verify=True.
        assert ctx.verify_mode == ssl.CERT_REQUIRED
    finally:
        asyncio.run(client.aclose())


@pytest.mark.asyncio
async def test_idempotency_key_passed_through() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["idempotency_key"] = request.headers.get("idempotency-key", "")
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "id_tag": "RFID_X",
                    "request_id": "echoed",
                    "id_tag_info": {"status": "Accepted"},
                },
                "message": "ok",
            },
        )

    client = _client_with_transport(handler)
    try:
        await client.authorize(id_tag="RFID_X", cp_id="CP_001", idempotency_key="custom-key-123")
        assert captured["idempotency_key"] == "custom-key-123"
    finally:
        await client.aclose()
