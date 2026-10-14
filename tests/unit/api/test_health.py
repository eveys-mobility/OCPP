"""Tests for `GET /api/v1/health`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest


@pytest.mark.asyncio
async def test_clickhouse_connectivity_does_not_claim_archive_progress(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/health")

    assert response.status_code == 200
    body = response.json()
    assert body["components"]["clickhouse"] == "ok"
    assert body["components"].get("ingestor") == "unknown"
    assert body["status"] == "degraded"
    assert body.get("archive") == {
        "state": "unknown",
        "reason": "ingestor_progress_not_observed",
        "consumer_lag_messages": None,
        "last_successful_commit_at": None,
    }


@pytest.mark.asyncio
async def test_health_reports_core_reachability_without_claiming_archive_progress(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "degraded"
    assert body["components"]["postgres"] == "ok"
    assert body["request_id"]
    assert "version" in body
    assert body["components"]["clickhouse"] == "ok"
    assert body["components"]["clickhouse_error_class"] is None


@pytest.mark.asyncio
async def test_health_degrades_on_clickhouse_failure_without_failing_readiness(
    client: httpx.AsyncClient,
    fake_ch_client: MagicMock,
) -> None:
    fake_ch_client.probe = AsyncMock(side_effect=RuntimeError("private connection details"))

    response = await client.get("/api/v1/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "degraded"
    assert body["components"]["clickhouse"] == "error"
    assert body["components"]["clickhouse_error_class"] == "RuntimeError"
    assert "private connection details" not in response.text
    ready = await client.get("/api/v1/ready")
    assert ready.status_code == 200


@pytest.mark.asyncio
async def test_health_bounds_the_clickhouse_probe(
    client: httpx.AsyncClient,
    fake_ch_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    from eveys_ocpp.api import health as health_module

    async def blocked_probe() -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(health_module, "CLICKHOUSE_PROBE_TIMEOUT_SECONDS", 0.01)
    fake_ch_client.probe = blocked_probe
    response = await asyncio.wait_for(client.get("/api/v1/health"), timeout=1)

    assert response.status_code == 200
    assert response.json()["components"]["clickhouse"] == "error"
    assert response.json()["components"]["clickhouse_error_class"] == "TimeoutError"


@pytest.mark.asyncio
async def test_health_returns_xrequestid_in_response_header(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/health", headers={"X-Request-ID": "test-correlation-1"})

    assert response.headers["X-Request-ID"] == "test-correlation-1"
    assert response.json()["request_id"] == "test-correlation-1"


@pytest.mark.asyncio
async def test_health_degrades_when_postgres_fails(
    settings: object,
) -> None:
    """A Postgres outage flips `status` to `degraded` but keeps
    HTTP 200 — the contract specifies the body, not the status, is
    the alerting signal.

    Builds a separate app/client so the failing factory doesn't bleed
    into other tests sharing the conftest's `client` fixture.
    """
    from eveys_ocpp.api._app import make_app

    class _BadSession:
        async def __aenter__(self) -> _BadSession:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        commit = AsyncMock()
        rollback = AsyncMock()
        close = AsyncMock()
        execute = AsyncMock(side_effect=RuntimeError("postgres down"))

    class _BadFactory:
        def __call__(self) -> _BadSession:
            return _BadSession()

    registry = MagicMock()
    registry.get_pod = AsyncMock(return_value=None)
    app = make_app(
        session_factory=_BadFactory(),
        settings=settings,  # type: ignore[arg-type]
        registry=registry,
        redis=None,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gw",
    ) as ac:
        response = await ac.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json()["status"] == "degraded"
    assert response.json()["components"]["postgres"] == "unavailable"
