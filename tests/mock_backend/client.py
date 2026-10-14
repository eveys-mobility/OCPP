"""Real backend client against the existing mock contract with no network transport."""

from __future__ import annotations

import httpx

from eveys_ocpp.platform.circuit_breaker import CircuitBreaker
from eveys_ocpp.platform.client import BackendHTTPClient
from eveys_ocpp.settings import Settings
from tests.mock_backend.app import build_app
from tests.mock_backend.config import MockBackendConfig


def in_process_backend_client(settings: Settings) -> BackendHTTPClient:
    token = "isolated-e2e-backend-only"
    base_url = "http://mock-backend/api/eveys"
    config = MockBackendConfig(bearer_token=token, blocked_id_tags=frozenset({"SMOKE_BLOCKED"}))
    http = httpx.AsyncClient(
        base_url=base_url, headers={"Authorization": f"Bearer {token}"},
        transport=httpx.ASGITransport(app=build_app(config)), trust_env=False,
    )
    return BackendHTTPClient(
        base_url=base_url, token=token, http=http, settings=settings,
        breaker=CircuitBreaker(name="isolated-smoke", threshold=5, cooldown_seconds=1),
    )
