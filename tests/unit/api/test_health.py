"""Tests for `GET /api/v1/health`."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import pytest_asyncio

from eveys_ocpp.clickhouse.progress import IngestorProgress
from eveys_ocpp.settings import Settings


@pytest.mark.asyncio
async def test_clickhouse_connectivity_does_not_claim_archive_progress(
    client: httpx.AsyncClient,
) -> None:
    """Without the ingestor probe wired, a ClickHouse read says nothing
    about ingestion; the archive stays unobserved."""
    response = await client.get("/api/v1/health")

    assert response.status_code == 200
    body = response.json()
    assert body["components"]["clickhouse"] == "ok"
    assert body["components"]["ingestor"] == "unconfigured"
    assert body["components"]["ingestor_error_class"] is None
    assert body["status"] == "ok"
    assert body["archive"] == {
        "state": "unknown",
        "reason": "ingestor_progress_not_observed",
        "consumer_lag_messages": None,
        "last_successful_commit_at": None,
        "observed_at": None,
    }


@pytest.mark.asyncio
async def test_health_reports_core_reachability(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["components"]["postgres"] == "ok"
    assert body["request_id"]
    assert "version" in body
    assert body["components"]["clickhouse"] == "ok"
    assert body["components"]["clickhouse_error_class"] is None


def _progress(state: str, **overrides: Any) -> IngestorProgress:
    sampled = datetime(2026, 10, 16, 15, 20, 4, tzinfo=UTC)
    values: dict[str, Any] = {
        "state": state,
        "reason": {
            "ok": None,
            "lagging": "consumer_lag_above_threshold",
            "stalled": "consumer_commits_stalled",
            "unknown": "ingestor_progress_not_observed",
        }[state],
        "lag_messages": 11,
        "last_commit_at": sampled,
        "observed_at": sampled,
        "error_class": None,
    }
    values.update(overrides)
    return IngestorProgress(**values)


@pytest_asyncio.fixture
async def probed_client(
    settings: Settings,
    fake_session_factory: Any,
    fake_registry: MagicMock,
    fake_ch_client: MagicMock,
) -> AsyncIterator[tuple[httpx.AsyncClient, MagicMock]]:
    from eveys_ocpp.api._app import make_app

    app = make_app(
        session_factory=fake_session_factory,
        settings=settings,
        registry=fake_registry,
        redis=None,
        ch_client=fake_ch_client,
    )
    probe = MagicMock()
    probe.snapshot = MagicMock(return_value=_progress("ok"))
    app.state.ingestor_probe = probe
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://gw"
    ) as ac:
        yield ac, probe


@pytest.mark.asyncio
async def test_health_is_ok_when_the_ingestor_keeps_up(
    probed_client: tuple[httpx.AsyncClient, MagicMock],
) -> None:
    client, _ = probed_client

    response = await client.get("/api/v1/health")

    body = response.json()
    assert body["status"] == "ok"
    assert body["components"]["ingestor"] == "ok"
    assert body["components"]["ingestor_error_class"] is None
    assert body["archive"] == {
        "state": "ok",
        "reason": None,
        "consumer_lag_messages": 11,
        "last_successful_commit_at": "2026-10-16T15:20:04+00:00",
        "observed_at": "2026-10-16T15:20:04+00:00",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["lagging", "stalled", "unknown"])
async def test_health_degrades_on_ingestor_faults_without_failing_readiness(
    probed_client: tuple[httpx.AsyncClient, MagicMock],
    state: str,
) -> None:
    client, probe = probed_client
    probe.snapshot.return_value = _progress(state, error_class="KafkaConnectionError")

    response = await client.get("/api/v1/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "degraded"
    assert body["components"]["ingestor"] == state
    assert body["components"]["ingestor_error_class"] == "KafkaConnectionError"
    assert body["archive"]["state"] == state
    assert body["archive"]["reason"] == _progress(state).reason
    ready = await client.get("/api/v1/ready")
    assert ready.status_code == 200


@pytest.mark.asyncio
async def test_health_never_waits_on_kafka(settings: Settings, fake_ch_client: MagicMock) -> None:
    """A broker that never answers leaves the sample missing; the
    request still returns at once with the ingestor `unknown`."""
    import asyncio

    from eveys_ocpp.api._app import make_app
    from eveys_ocpp.clickhouse.progress import IngestorProgressProbe

    class _HangingSource:
        async def read(self, *_: object) -> dict[object, object]:
            await asyncio.Event().wait()
            return {}

        async def reset(self) -> None:
            return None

    registry = MagicMock()
    registry.get_pod = AsyncMock(return_value=None)

    class _Factory:
        def __call__(self) -> Any:
            session = MagicMock()
            session.__aenter__ = AsyncMock(return_value=session)
            session.__aexit__ = AsyncMock(return_value=None)
            session.execute = AsyncMock()
            return session

    app = make_app(
        session_factory=_Factory(),  # type: ignore[arg-type]
        settings=settings,
        registry=registry,
        redis=None,
        ch_client=fake_ch_client,
    )
    probe = IngestorProgressProbe(settings, source=_HangingSource())
    await probe.start()
    app.state.ingestor_probe = probe
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw"
        ) as ac:
            response = await asyncio.wait_for(ac.get("/api/v1/health"), timeout=1)
    finally:
        await probe.stop()

    assert response.json()["components"]["ingestor"] == "unknown"
    assert response.json()["status"] == "degraded"


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
