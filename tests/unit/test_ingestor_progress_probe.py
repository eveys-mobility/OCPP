"""Ingestor progress as the gateway reports it on `/api/v1/health`."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest
from aiokafka.structs import TopicPartition

from eveys_ocpp.clickhouse import progress as module
from eveys_ocpp.clickhouse.progress import (
    IngestorProgressProbe,
    KafkaOffsetSource,
    PartitionOffsets,
    ingested_topics,
)
from eveys_ocpp.settings import Settings

FRAMES = TopicPartition("cp.ocpp_frames", 0)
METER = TopicPartition("cp.meter", 0)
T0 = 1_760_000_000.0


class _Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> float:
        return self.now


class _Source:
    def __init__(self) -> None:
        self.offsets: dict[TopicPartition, PartitionOffsets] = {}
        self.error: Exception | None = None
        self.block = False
        self.reads: list[tuple[str, tuple[str, ...]]] = []
        self.resets = 0

    async def read(
        self, group_id: str, topics: Sequence[str]
    ) -> Mapping[TopicPartition, PartitionOffsets]:
        self.reads.append((group_id, tuple(topics)))
        if self.block:
            await asyncio.Event().wait()
        if self.error is not None:
            raise self.error
        return dict(self.offsets)

    async def reset(self) -> None:
        self.resets += 1


def _settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "health_ingestor_probe_interval_seconds": 10.0,
        "health_ingestor_lag_threshold_messages": 1000,
        "health_ingestor_stall_seconds": 60.0,
    }
    values.update(overrides)
    return Settings(**values)


def _probe(**overrides: Any) -> tuple[IngestorProgressProbe, _Source, _Clock]:
    source, clock = _Source(), _Clock()
    return IngestorProgressProbe(_settings(**overrides), source=source, clock=clock), source, clock


async def _sample_at(
    probe: IngestorProgressProbe, source: _Source, clock: _Clock, at: float, **offsets: Any
) -> None:
    clock.now = T0 + at
    source.offsets = {
        tp: PartitionOffsets(*values)
        for tp, values in (
            (FRAMES, offsets.get("frames")),
            (METER, offsets.get("meter")),
        )
        if values is not None
    }
    await probe.sample_once()


def test_ingested_topics_match_the_ingestor_tables_and_leave_out_tx_stopped() -> None:
    topics = ingested_topics(Settings())

    assert topics == (
        "cp.meter",
        "cp.status",
        "cp.boot",
        "tx.started",
        "cp.offline_duration",
        "cp.ocpp_frames",
    )
    assert "tx.stopped" not in topics


def test_partition_lag_counts_from_log_start_when_nothing_usable_is_committed() -> None:
    assert PartitionOffsets(committed=90, start=0, end=100).lag == 10
    assert PartitionOffsets(committed=None, start=40, end=100).lag == 60
    # Retention removed messages the group never committed past.
    assert PartitionOffsets(committed=10, start=40, end=100).lag == 60
    assert PartitionOffsets(committed=100, start=0, end=100).lag == 0


async def test_unknown_until_the_first_sample() -> None:
    probe, _, _ = _probe()

    snapshot = probe.snapshot()

    assert snapshot.state == "unknown"
    assert snapshot.reason == "ingestor_progress_not_observed"
    assert snapshot.lag_messages is None
    assert snapshot.observed_at is None


async def test_caught_up_ingestor_is_ok() -> None:
    probe, source, clock = _probe()

    await _sample_at(probe, source, clock, 0, frames=(500, 0, 500), meter=(70, 0, 70))
    snapshot = probe.snapshot()

    assert snapshot.state == "ok"
    assert snapshot.reason is None
    assert snapshot.lag_messages == 0
    assert snapshot.observed_at == datetime.fromtimestamp(T0, tz=UTC)
    assert snapshot.error_class is None
    assert source.reads == [("eveys-ocpp-clickhouse-ingestor", ingested_topics(Settings()))]


async def test_in_flight_batch_with_moving_commits_stays_ok_and_records_commit_time() -> None:
    probe, source, clock = _probe()

    await _sample_at(probe, source, clock, 0, frames=(500, 0, 512))
    assert probe.snapshot().last_commit_at is None

    for step in range(1, 10):
        at = step * 10
        await _sample_at(probe, source, clock, at, frames=(500 + at, 0, 512 + at))

    snapshot = probe.snapshot()
    assert snapshot.state == "ok"
    assert snapshot.lag_messages == 12
    assert snapshot.last_commit_at == datetime.fromtimestamp(T0 + 90, tz=UTC)


async def test_pending_messages_without_commit_movement_become_stalled() -> None:
    probe, source, clock = _probe()

    await _sample_at(probe, source, clock, 0, frames=(500, 0, 503))
    await _sample_at(probe, source, clock, 50, frames=(500, 0, 640))
    assert probe.snapshot().state == "ok"

    await _sample_at(probe, source, clock, 60, frames=(500, 0, 660))
    snapshot = probe.snapshot()

    assert snapshot.state == "stalled"
    assert snapshot.reason == "consumer_commits_stalled"
    assert snapshot.lag_messages == 160


async def test_new_message_after_a_quiet_period_is_not_reported_as_stalled() -> None:
    probe, source, clock = _probe()

    await _sample_at(probe, source, clock, 0, meter=(70, 0, 70))
    await _sample_at(probe, source, clock, 600, meter=(70, 0, 70))
    await _sample_at(probe, source, clock, 610, meter=(70, 0, 71))

    assert probe.snapshot().state == "ok"

    await _sample_at(probe, source, clock, 670, meter=(70, 0, 71))
    assert probe.snapshot().state == "stalled"


async def test_partition_the_group_never_committed_is_stalled_after_the_window() -> None:
    probe, source, clock = _probe()

    await _sample_at(probe, source, clock, 0, frames=(None, 0, 25))
    assert probe.snapshot().lag_messages == 25
    assert probe.snapshot().state == "ok"

    await _sample_at(probe, source, clock, 60, frames=(None, 0, 30))
    assert probe.snapshot().state == "stalled"


async def test_lag_above_threshold_with_moving_commits_is_lagging() -> None:
    probe, source, clock = _probe(health_ingestor_lag_threshold_messages=100)

    await _sample_at(probe, source, clock, 0, frames=(1000, 0, 1101))
    await _sample_at(probe, source, clock, 10, frames=(1500, 0, 1650))
    snapshot = probe.snapshot()

    assert snapshot.state == "lagging"
    assert snapshot.reason == "consumer_lag_above_threshold"
    assert snapshot.lag_messages == 150


async def test_stalled_wins_over_lagging() -> None:
    probe, source, clock = _probe(health_ingestor_lag_threshold_messages=100)

    await _sample_at(probe, source, clock, 0, frames=(1000, 0, 1200))
    await _sample_at(probe, source, clock, 60, frames=(1000, 0, 1300))

    assert probe.snapshot().state == "stalled"


async def test_failed_sample_keeps_the_fresh_state_then_turns_unknown() -> None:
    probe, source, clock = _probe()
    await _sample_at(probe, source, clock, 0, frames=(500, 0, 500))

    source.error = RuntimeError("broker details that must not leak")
    clock.now = T0 + 10
    await probe.sample_once()

    snapshot = probe.snapshot()
    assert snapshot.state == "ok"
    assert snapshot.error_class == "RuntimeError"
    assert source.resets == 1

    clock.now = T0 + 31
    snapshot = probe.snapshot()
    assert snapshot.state == "unknown"
    assert snapshot.reason == "ingestor_progress_not_observed"
    # The last known values stay visible next to their sample time.
    assert snapshot.lag_messages == 0
    assert snapshot.observed_at == datetime.fromtimestamp(T0, tz=UTC)

    source.error = None
    await _sample_at(probe, source, clock, 40, frames=(510, 0, 510))
    snapshot = probe.snapshot()
    assert snapshot.state == "ok"
    assert snapshot.error_class is None


async def test_sample_is_bounded_by_the_probe_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "PROBE_TIMEOUT_SECONDS", 0.01)
    probe, source, _ = _probe()
    source.block = True

    await asyncio.wait_for(probe.sample_once(), timeout=1)

    assert probe.snapshot().error_class == "TimeoutError"
    assert probe.snapshot().state == "unknown"
    assert source.resets == 1


async def test_start_samples_at_once_and_stop_releases_the_clients() -> None:
    probe, source, _ = _probe()
    source.offsets = {FRAMES: PartitionOffsets(500, 0, 500)}

    await probe.start()
    for _ in range(100):
        if source.reads:
            break
        await asyncio.sleep(0)
    await probe.stop()

    assert len(source.reads) == 1
    assert probe.snapshot().state == "ok"
    assert source.resets == 1


class _Admin:
    instances: ClassVar[list[_Admin]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.metadata: list[dict[str, Any]] = []
        self.offset_calls: list[tuple[str, list[TopicPartition]]] = []
        self.committed: dict[TopicPartition, Any] = {}
        self.closed = False
        _Admin.instances.append(self)

    async def start(self) -> None:
        pass

    async def close(self) -> None:
        self.closed = True

    async def describe_topics(self, topics: list[str]) -> list[dict[str, Any]]:
        return self.metadata

    async def list_consumer_group_offsets(
        self, group_id: str, partitions: list[TopicPartition]
    ) -> dict[TopicPartition, Any]:
        self.offset_calls.append((group_id, partitions))
        return self.committed


class _Consumer:
    instances: ClassVar[list[_Consumer]] = []

    def __init__(self, *topics: str, **kwargs: Any) -> None:
        assert not topics
        self.kwargs = kwargs
        self.stopped = False
        _Consumer.instances.append(self)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        self.stopped = True

    async def beginning_offsets(self, partitions: list[TopicPartition]) -> dict[Any, int]:
        return dict.fromkeys(partitions, 0)

    async def end_offsets(self, partitions: list[TopicPartition]) -> dict[Any, int]:
        return dict.fromkeys(partitions, 100)

    def subscribe(self, *_: Any, **__: Any) -> None:  # pragma: no cover - must not happen
        raise AssertionError("the health probe must not subscribe")

    async def commit(self, *_: Any, **__: Any) -> None:  # pragma: no cover - must not happen
        raise AssertionError("the health probe must not commit")


@pytest.fixture
def kafka_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    _Admin.instances.clear()
    _Consumer.instances.clear()
    monkeypatch.setattr(module, "AIOKafkaAdminClient", _Admin)
    monkeypatch.setattr(module, "AIOKafkaConsumer", _Consumer)


def _topic(name: str, *, error_code: int = 0, partitions: int = 1) -> dict[str, Any]:
    return {
        "error_code": error_code,
        "topic": name,
        "partitions": [{"error_code": 0, "partition": p} for p in range(partitions)],
    }


@pytest.mark.usefixtures("kafka_clients")
async def test_kafka_source_reads_offsets_without_joining_the_ingestor_group() -> None:
    source = KafkaOffsetSource(Settings())
    await source._open()
    admin = _Admin.instances[0]
    admin.metadata = [
        _topic("cp.ocpp_frames"),
        _topic("cp.meter"),
        # Topic never created (e.g. frame publishing disabled): nothing to archive.
        _topic("cp.boot", error_code=3, partitions=0),
    ]
    admin.committed = {
        FRAMES: SimpleNamespace(offset=90, metadata=""),
        METER: SimpleNamespace(offset=-1, metadata=""),
    }

    offsets = await source.read("eveys-ocpp-clickhouse-ingestor", ingested_topics(Settings()))

    assert offsets == {
        FRAMES: PartitionOffsets(committed=90, start=0, end=100),
        METER: PartitionOffsets(committed=None, start=0, end=100),
    }
    assert _Admin.instances[0].offset_calls == [("eveys-ocpp-clickhouse-ingestor", [FRAMES, METER])]
    assert _Consumer.instances[0].kwargs["group_id"] is None
    assert _Consumer.instances[0].kwargs["enable_auto_commit"] is False

    await source.reset()
    assert _Admin.instances[0].closed
    assert _Consumer.instances[0].stopped


@pytest.mark.usefixtures("kafka_clients")
async def test_kafka_source_raises_on_other_metadata_errors() -> None:
    from aiokafka.errors import LeaderNotAvailableError

    source = KafkaOffsetSource(Settings())
    await source._open()
    _Admin.instances[0].metadata = [_topic("cp.meter", error_code=LeaderNotAvailableError.errno)]

    with pytest.raises(LeaderNotAvailableError):
        await source.read("eveys-ocpp-clickhouse-ingestor", ("cp.meter",))


@pytest.mark.usefixtures("kafka_clients")
async def test_kafka_source_with_no_existing_topics_reports_nothing_pending() -> None:
    source = KafkaOffsetSource(Settings())
    await source._open()
    _Admin.instances[0].metadata = [_topic("cp.meter", error_code=3, partitions=0)]

    assert await source.read("eveys-ocpp-clickhouse-ingestor", ("cp.meter",)) == {}
    assert _Admin.instances[0].offset_calls == []
