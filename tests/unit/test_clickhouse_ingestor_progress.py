"""Progress means acknowledged offsets, not an advanced consumer position."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from aiokafka import TopicPartition
from prometheus_client import REGISTRY

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.clickhouse import ingestor as module
from eveys_ocpp.settings import Settings


class _Consumer:
    def __init__(
        self, ingestor: module.ClickHouseIngestor, topic: str, *, commit_fail: bool = False
    ):
        self.ingestor, self.tp = ingestor, TopicPartition(topic, 0)
        self.commit_fail = commit_fail
        self.polled = False
        self.listener: Any = None

    def subscribe(self, topics: tuple[str, ...], *, listener: Any) -> None:
        assert self.tp.topic in topics
        self.listener = listener

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def getmany(self, **_: Any) -> dict[Any, list[Any]]:
        if self.polled:
            self.ingestor._shutdown.set()
            return {}
        self.polled = True
        envelope = events_pb2.EventEnvelope(
            event_id="TEST_PROGRESS",
            occurred_at="2026-10-13T00:00:00Z",
            cp_id="TEST_CP",
            cp_boot=events_pb2.CpBoot(vendor="TEST"),
        )
        return {
            self.tp: [
                SimpleNamespace(value=envelope.SerializeToString(), offset=10, topic=self.tp.topic)
            ]
        }

    async def commit(self, offsets: dict[Any, int]) -> None:
        assert offsets == {self.tp: 11}
        if self.commit_fail:
            self.ingestor._shutdown.set()
            raise RuntimeError("commit not acknowledged")

    def assignment(self) -> set[Any]:
        return {self.tp}

    def highwater(self, tp: Any) -> int:
        assert tp == self.tp
        return 30

    def last_poll_timestamp(self, tp: Any) -> int:
        assert tp == self.tp
        return 1_000_000

    async def position(self, _tp: Any) -> int:
        pytest.fail("uncommitted consumer position is not archive delivery progress")


class _Writer:
    def cursor(self) -> _Writer:
        return self

    async def __aenter__(self) -> _Writer:
        return self

    async def __aexit__(self, *_: object) -> None:
        pass

    async def executemany(self, sql: str, rows: list[dict[str, Any]]) -> None:
        assert sql.startswith("INSERT INTO cp_boot")
        assert rows[0]["event_id"] == "TEST_PROGRESS"


def _sample(name: str, topic: str) -> float | None:
    return REGISTRY.get_sample_value(name, {"topic": topic, "partition": "0"})


@pytest.mark.asyncio
async def test_archive_lag_uses_acknowledged_commit_and_has_known_and_sample_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module, "time", SimpleNamespace(time=lambda: 1000.0), raising=False)
    ingestor = module.ClickHouseIngestor(Settings(clickhouse_ingestor_batch_size=1))
    consumer = _Consumer(ingestor, "progress_ack")
    ingestor._consumer, ingestor._conn = consumer, _Writer()

    await ingestor.ingest_loop()

    assert _sample("eveys_ocpp_clickhouse_ingestor_consumer_lag_messages", "progress_ack") == 19
    assert _sample("eveys_ocpp_clickhouse_ingestor_consumer_lag_known", "progress_ack") == 1
    assert (
        _sample("eveys_ocpp_clickhouse_ingestor_lag_sample_timestamp_seconds", "progress_ack")
        == 1000
    )
    assert (
        REGISTRY.get_sample_value(
            "eveys_ocpp_clickhouse_ingestor_last_successful_commit_timestamp_seconds"
        )
        == 1000
    )


@pytest.mark.asyncio
async def test_failed_commit_keeps_lag_unknown_and_does_not_advance_ack_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module, "time", SimpleNamespace(time=lambda: 2000.0), raising=False)
    before = REGISTRY.get_sample_value(
        "eveys_ocpp_clickhouse_ingestor_last_successful_commit_timestamp_seconds"
    )
    errors_before = (
        REGISTRY.get_sample_value(
            "eveys_ocpp_clickhouse_ingestor_errors_total", {"stage": "commit"}
        )
        or 0
    )
    ingestor = module.ClickHouseIngestor(Settings(clickhouse_ingestor_batch_size=1))
    consumer = _Consumer(ingestor, "progress_failed", commit_fail=True)
    ingestor._consumer, ingestor._conn = consumer, _Writer()

    await ingestor.ingest_loop()

    assert _sample("eveys_ocpp_clickhouse_ingestor_consumer_lag_messages", "progress_failed") == -1
    assert _sample("eveys_ocpp_clickhouse_ingestor_consumer_lag_known", "progress_failed") == 0
    assert (
        REGISTRY.get_sample_value(
            "eveys_ocpp_clickhouse_ingestor_last_successful_commit_timestamp_seconds"
        )
        == before
    )
    assert (
        REGISTRY.get_sample_value(
            "eveys_ocpp_clickhouse_ingestor_errors_total", {"stage": "commit"}
        )
        == errors_before + 1
    )


@pytest.mark.asyncio
async def test_same_partition_reassignment_clears_old_ack_until_new_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(clickhouse_ingestor_batch_size=1)
    ingestor = module.ClickHouseIngestor(settings)
    consumer = _Consumer(ingestor, settings.kafka_topic_cp_boot)
    monkeypatch.setattr(module, "AIOKafkaConsumer", lambda *_, **__: consumer)

    async def connect() -> None:
        ingestor._conn = _Writer()

    monkeypatch.setattr(ingestor, "_connect", connect)
    await ingestor.start()
    assert consumer.listener is not None
    ingestor._acknowledged_offsets[consumer.tp] = 11
    ingestor._observe_progress()
    assert _sample("eveys_ocpp_clickhouse_ingestor_consumer_lag_known", consumer.tp.topic) == 1

    await consumer.listener.on_partitions_revoked([consumer.tp])
    await consumer.listener.on_partitions_assigned([consumer.tp])
    ingestor._observe_progress()

    assert _sample("eveys_ocpp_clickhouse_ingestor_consumer_lag_known", consumer.tp.topic) == 0
    assert _sample("eveys_ocpp_clickhouse_ingestor_consumer_lag_messages", consumer.tp.topic) == -1
    await ingestor.ingest_loop()
    assert _sample("eveys_ocpp_clickhouse_ingestor_consumer_lag_known", consumer.tp.topic) == 1


@pytest.mark.asyncio
async def test_rebalance_during_commit_ack_cannot_prove_new_assignment_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(clickhouse_ingestor_batch_size=1)
    ingestor = module.ClickHouseIngestor(settings)

    class RebalancedConsumer(_Consumer):
        async def commit(self, offsets: dict[Any, int]) -> None:
            assert offsets == {self.tp: 11}
            await self.listener.on_partitions_revoked([self.tp])
            await self.listener.on_partitions_assigned([self.tp])

    consumer = RebalancedConsumer(ingestor, settings.kafka_topic_cp_boot)
    monkeypatch.setattr(module, "AIOKafkaConsumer", lambda *_, **__: consumer)

    async def connect() -> None:
        ingestor._conn = _Writer()

    monkeypatch.setattr(ingestor, "_connect", connect)
    await ingestor.start()

    await ingestor.ingest_loop()

    assert _sample("eveys_ocpp_clickhouse_ingestor_consumer_lag_known", consumer.tp.topic) == 0
    assert consumer.tp not in ingestor._acknowledged_offsets


def test_empty_local_poll_does_not_refresh_per_partition_highwater_age() -> None:
    ingestor = module.ClickHouseIngestor(Settings())
    consumer = _Consumer(ingestor, "progress_stale_zero")
    ingestor._consumer = consumer
    ingestor._acknowledged_offsets[consumer.tp] = 30
    ingestor._last_poll_timestamp = 1000.0
    ingestor._observe_progress()
    ingestor._last_poll_timestamp = 9000.0  # a later getmany returned locally buffered/empty data

    ingestor._observe_progress()

    assert _sample("eveys_ocpp_clickhouse_ingestor_consumer_lag_messages", consumer.tp.topic) == 0
    assert _sample("eveys_ocpp_clickhouse_ingestor_consumer_lag_known", consumer.tp.topic) == 1
    assert (
        _sample("eveys_ocpp_clickhouse_ingestor_lag_sample_timestamp_seconds", consumer.tp.topic)
        == 1000
    )


def test_no_partition_fetch_timestamp_cannot_claim_known_fresh_lag() -> None:
    ingestor = module.ClickHouseIngestor(Settings())
    consumer = _Consumer(ingestor, "progress_missing_fetch")
    consumer.last_poll_timestamp = lambda _: None
    ingestor._consumer = consumer
    ingestor._acknowledged_offsets[consumer.tp] = 30
    ingestor._last_poll_timestamp = 9000.0

    ingestor._observe_progress()

    assert _sample("eveys_ocpp_clickhouse_ingestor_consumer_lag_messages", consumer.tp.topic) == -1
    assert _sample("eveys_ocpp_clickhouse_ingestor_consumer_lag_known", consumer.tp.topic) == 0
    assert (
        _sample("eveys_ocpp_clickhouse_ingestor_lag_sample_timestamp_seconds", consumer.tp.topic)
        == 0
    )
