"""The REST server owns the ingestor progress probe's lifecycle."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest
import uvicorn

from eveys_ocpp.clickhouse import progress
from eveys_ocpp.settings import Settings
from eveys_ocpp.transport import rest_server


class _Probe:
    instances: ClassVar[list[_Probe]] = []

    def __init__(self, settings: Settings) -> None:
        self.events: list[str] = []
        _Probe.instances.append(self)

    async def start(self) -> None:
        self.events.append("start")

    async def stop(self) -> None:
        self.events.append("stop")


@pytest.fixture
def served_apps(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    apps: list[Any] = []

    async def serve(self: uvicorn.Server, sockets: Any = None) -> None:
        apps.append(self.config.app)

    _Probe.instances.clear()
    monkeypatch.setattr(uvicorn.Server, "serve", serve)
    monkeypatch.setattr(progress, "IngestorProgressProbe", _Probe)
    return apps


async def test_probe_runs_for_the_lifetime_of_the_rest_server(served_apps: list[Any]) -> None:
    await rest_server.serve_forever(
        session_factory=MagicMock(), settings=Settings(), registry=None, redis=None
    )

    assert served_apps[0].state.ingestor_probe is _Probe.instances[0]
    assert _Probe.instances[0].events == ["start", "stop"]


async def test_disabled_probe_is_not_started(served_apps: list[Any]) -> None:
    await rest_server.serve_forever(
        session_factory=MagicMock(),
        settings=Settings(health_ingestor_probe_enabled=False),
        registry=None,
        redis=None,
    )

    assert served_apps[0].state.ingestor_probe is None
    assert _Probe.instances == []
