"""Startup failure cleanup and CLI resource wiring with inert dependencies."""

from __future__ import annotations

import asyncio

import pytest

from eveys_ocpp.clickhouse import ingestor as module
from eveys_ocpp.settings import Settings


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [OSError("archive unavailable"), asyncio.CancelledError()])
async def test_failed_or_cancelled_archive_connect_closes_started_consumer(
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
) -> None:
    class Consumer:
        stopped = False

        def subscribe(self, *_: object, **__: object) -> None:
            pass

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            self.stopped = True

    class Connection:
        closed = False

        async def connect(self) -> None:
            raise failure

        async def close(self) -> None:
            self.closed = True

    consumer, connection = Consumer(), Connection()
    monkeypatch.setattr(module, "AIOKafkaConsumer", lambda *_, **__: consumer)
    monkeypatch.setattr(module, "Connection", lambda **_: connection)
    ingestor = module.ClickHouseIngestor(Settings())

    with pytest.raises(type(failure)):
        await ingestor.start()

    assert consumer.stopped and connection.closed
    assert ingestor._consumer is None and ingestor._conn is None


@pytest.mark.asyncio
async def test_retry_after_failed_start_resets_shutdown_and_running_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    class Consumer:
        def subscribe(self, *_: object, **__: object) -> None:
            pass

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

    class Connection:
        async def connect(self) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise OSError("controlled first connect failure")

        async def close(self) -> None:
            pass

    monkeypatch.setattr(module, "AIOKafkaConsumer", lambda *_, **__: Consumer())
    monkeypatch.setattr(module, "Connection", lambda **_: Connection())
    ingestor = module.ClickHouseIngestor(Settings())
    with pytest.raises(OSError):
        await ingestor.start()

    await ingestor.start()

    assert not ingestor._shutdown.is_set()
    assert module.metrics_registry.CLICKHOUSE_INGESTOR_RUNNING._value.get() == 1
    await ingestor.stop()
    assert module.metrics_registry.CLICKHOUSE_INGESTOR_RUNNING._value.get() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_start", [True, False])
async def test_cli_cleans_ingestor_and_metrics_on_startup_or_loop_failure(
    monkeypatch: pytest.MonkeyPatch,
    fail_start: bool,
) -> None:
    trace: list[str] = []

    class Ingestor:
        _shutdown = asyncio.Event()

        async def start(self) -> None:
            trace.append("ingestor.start")
            if fail_start:
                raise RuntimeError("controlled startup failure")

        async def ingest_loop(self) -> None:
            trace.append("ingestor.loop")
            raise RuntimeError("controlled loop failure")

        async def stop(self) -> None:
            trace.append("ingestor.stop")

    class Metrics:
        async def start(self) -> None:
            trace.append("metrics.start")

        async def stop(self) -> None:
            trace.append("metrics.stop")

    monkeypatch.setattr(module, "get_settings", lambda: Settings(metrics_enabled=True))
    monkeypatch.setattr(module, "ClickHouseIngestor", lambda _: Ingestor())
    monkeypatch.setattr(module, "MetricsServer", lambda **_: Metrics(), raising=False)
    monkeypatch.setattr(asyncio.get_running_loop(), "add_signal_handler", lambda *_: None)

    with pytest.raises(RuntimeError, match="controlled"):
        await module._run()

    middle = [] if fail_start else ["ingestor.loop"]
    assert trace == ["metrics.start", "ingestor.start", *middle, "ingestor.stop", "metrics.stop"]
