"""Kafka position/commit barriers using a native-writer fake, never a broker."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from aiokafka import TopicPartition

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.clickhouse.ingestor import ClickHouseIngestor, IngestorFatalError
from eveys_ocpp.settings import Settings


def _record(offset: int, event_id: str, *, status: bool = False) -> SimpleNamespace:
    env = events_pb2.EventEnvelope(
        event_id=event_id,
        occurred_at="2026-10-13T00:00:00+00:00",
        cp_id="CP_TEST",
    )
    if status:
        env.cp_status.CopyFrom(events_pb2.CpStatus(connector_id=1, status="Charging"))
    else:
        env.cp_boot.CopyFrom(events_pb2.CpBoot(vendor="TEST"))
    return SimpleNamespace(
        value=env.SerializeToString(), offset=offset, topic="cp.status" if status else "cp.boot"
    )


class _Consumer:
    def __init__(self, ingestor: ClickHouseIngestor, batches: list[dict[Any, list[Any]]]):
        self.ingestor = ingestor
        self.batches = list(batches)
        self.positions = {TopicPartition("unread", 9): 999}
        self.commits: list[dict[Any, int]] = []
        self.trace: list[str] = []
        self.commit_failures = 0

    async def getmany(self, **_: Any) -> dict[Any, list[Any]]:
        self.trace.append("poll")
        if not self.batches:
            self.ingestor._shutdown.set()
            return {}
        batch = self.batches.pop(0)
        for partition, records in batch.items():
            if records:
                self.positions[partition] = records[-1].offset + 1
        return batch

    async def commit(self, offsets: dict[Any, int] | None = None) -> None:
        self.trace.append("commit")
        if self.commit_failures:
            self.commit_failures -= 1
            raise RuntimeError("commit temporarily unavailable")
        self.commits.append(dict(self.positions if offsets is None else offsets))


class _Writer:
    def __init__(self, consumer: _Consumer, *, failures: dict[str, int] | None = None):
        self.consumer = consumer
        self.failures = failures or {}
        self.rows: dict[str, list[str]] = {}
        self.closed = False
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.closed = True

    def cursor(self) -> _Cursor:
        return _Cursor(self)


class _Cursor:
    def __init__(self, writer: _Writer):
        self.writer = writer

    async def __aenter__(self) -> _Cursor:
        return self

    async def __aexit__(self, *_: Any) -> None:
        pass

    async def executemany(self, sql: str, rows: list[dict[str, Any]]) -> None:
        table = sql.split()[2]
        self.writer.consumer.trace.append(f"insert:{table}")
        if self.writer.failures.get(table, 0):
            self.writer.failures[table] -= 1
            raise RuntimeError("write temporarily unavailable")
        self.writer.rows.setdefault(table, []).extend(row["event_id"] for row in rows)


def _allow_reconnect(writer: _Writer, monkeypatch: pytest.MonkeyPatch) -> None:
    def connection(**_: Any) -> _Writer:
        fresh = _Writer(writer.consumer, failures=writer.failures)
        fresh.rows = writer.rows
        return fresh

    monkeypatch.setattr("eveys_ocpp.clickhouse.ingestor.Connection", connection)


@pytest.fixture
def no_retry_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    async def no_sleep(_seconds: float) -> None:
        pass

    monkeypatch.setattr("eveys_ocpp.clickhouse.ingestor.asyncio.sleep", no_sleep)


@pytest.mark.asyncio
async def test_failed_batch_is_retried_before_polling_or_committing_a_later_offset(
    no_retry_delay: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    partition = TopicPartition("cp.boot", 0)
    ingestor = ClickHouseIngestor(Settings(clickhouse_ingestor_batch_size=1))
    consumer = _Consumer(
        ingestor, [{partition: [_record(10, "A")]}, {partition: [_record(11, "B")]}]
    )
    writer = _Writer(consumer, failures={"cp_boot": 1})
    _allow_reconnect(writer, monkeypatch)
    ingestor._consumer, ingestor._conn = consumer, writer

    await ingestor.ingest_loop()

    assert writer.rows == {"cp_boot": ["A", "B"]}
    assert consumer.trace[:4] == ["poll", "insert:cp_boot", "insert:cp_boot", "commit"]
    assert consumer.commits == [{partition: 11}, {partition: 12}]


@pytest.mark.asyncio
async def test_explicit_commit_contains_only_offsets_in_the_written_batch() -> None:
    p0, p1 = TopicPartition("cp.boot", 0), TopicPartition("cp.boot", 1)
    ingestor = ClickHouseIngestor(Settings(clickhouse_ingestor_batch_size=3))
    consumer = _Consumer(
        ingestor, [{p0: [_record(10, "A"), _record(11, "B")], p1: [_record(5, "C")]}]
    )
    writer = _Writer(consumer)
    ingestor._consumer, ingestor._conn = consumer, writer

    await ingestor.ingest_loop()

    assert writer.rows == {"cp_boot": ["A", "B", "C"]}
    assert consumer.commits == [{p0: 12, p1: 6}]


@pytest.mark.asyncio
async def test_partial_table_failure_retries_only_the_table_without_an_ack(
    no_retry_delay: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    boot, status = TopicPartition("cp.boot", 0), TopicPartition("cp.status", 0)
    ingestor = ClickHouseIngestor(Settings(clickhouse_ingestor_batch_size=2))
    consumer = _Consumer(
        ingestor, [{boot: [_record(10, "A")], status: [_record(20, "B", status=True)]}]
    )
    writer = _Writer(consumer, failures={"cp_status": 1})
    _allow_reconnect(writer, monkeypatch)
    ingestor._consumer, ingestor._conn = consumer, writer

    await ingestor.ingest_loop()

    assert writer.rows == {"cp_boot": ["A"], "cp_status": ["B"]}
    assert consumer.trace[:5] == [
        "poll",
        "insert:cp_boot",
        "insert:cp_status",
        "insert:cp_status",
        "commit",
    ]
    assert consumer.commits == [{boot: 11, status: 21}]


@pytest.mark.asyncio
async def test_commit_retry_does_not_reinsert_already_acknowledged_tables(
    no_retry_delay: None,
) -> None:
    partition = TopicPartition("cp.boot", 0)
    ingestor = ClickHouseIngestor(Settings(clickhouse_ingestor_batch_size=1))
    consumer = _Consumer(
        ingestor, [{partition: [_record(10, "A")]}, {partition: [_record(11, "B")]}]
    )
    consumer.commit_failures = 1
    writer = _Writer(consumer)
    ingestor._consumer, ingestor._conn = consumer, writer

    await ingestor.ingest_loop()

    assert writer.rows == {"cp_boot": ["A", "B"]}
    assert consumer.trace[:5] == ["poll", "insert:cp_boot", "commit", "commit", "poll"]
    assert consumer.commits == [{partition: 11}, {partition: 12}]


@pytest.mark.asyncio
async def test_retry_limit_leaves_failed_batch_and_later_offsets_uncommitted(
    no_retry_delay: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    partition = TopicPartition("cp.boot", 0)
    ingestor = ClickHouseIngestor(
        Settings(clickhouse_ingestor_batch_size=1, clickhouse_ingestor_max_flush_failures=3)
    )
    consumer = _Consumer(
        ingestor, [{partition: [_record(10, "A")]}, {partition: [_record(11, "B")]}]
    )
    writer = _Writer(consumer, failures={"cp_boot": 3})
    _allow_reconnect(writer, monkeypatch)
    ingestor._consumer, ingestor._conn = consumer, writer

    with pytest.raises(IngestorFatalError):
        await ingestor.ingest_loop()

    assert consumer.commits == []
    assert consumer.positions[partition] == 11
    assert len(consumer.batches) == 1
    assert writer.rows == {}


@pytest.mark.asyncio
async def test_parse_drops_keep_existing_policy_but_do_not_commit_unread_partitions() -> None:
    partition = TopicPartition("cp.boot", 0)
    ingestor = ClickHouseIngestor(Settings(clickhouse_ingestor_batch_size=1))
    bad = SimpleNamespace(value=b"malformed", topic="cp.boot", offset=10)
    consumer = _Consumer(ingestor, [{partition: [bad]}])
    writer = _Writer(consumer)
    ingestor._consumer, ingestor._conn = consumer, writer

    await ingestor.ingest_loop()

    assert writer.rows == {}
    assert consumer.commits == [{partition: 11}]


@pytest.mark.asyncio
async def test_native_insert_failure_reconnects_before_retrying_unacknowledged_table(
    no_retry_delay: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StickyCursor(_Cursor):
        async def executemany(self, sql: str, rows: list[dict[str, Any]]) -> None:
            if self.writer.closed or getattr(self.writer, "stuck", False):
                raise RuntimeError("some records have not been fetched")
            try:
                await super().executemany(sql, rows)
            except Exception:
                self.writer.stuck = True
                raise

    class StickyWriter(_Writer):
        def cursor(self) -> StickyCursor:
            return StickyCursor(self)

    boot, status = TopicPartition("cp.boot", 0), TopicPartition("cp.status", 0)
    ingestor = ClickHouseIngestor(
        Settings(clickhouse_ingestor_batch_size=2, clickhouse_ingestor_max_flush_failures=3)
    )
    consumer = _Consumer(
        ingestor, [{boot: [_record(10, "A")], status: [_record(20, "B", status=True)]}]
    )
    old = StickyWriter(consumer, failures={"cp_status": 1})
    fresh = _Writer(consumer)
    fresh.rows = old.rows
    ingestor._consumer, ingestor._conn = consumer, old
    monkeypatch.setattr("eveys_ocpp.clickhouse.ingestor.Connection", lambda **_: fresh)

    await ingestor.ingest_loop()

    assert old.closed and fresh.connected
    assert fresh.rows == {"cp_boot": ["A"], "cp_status": ["B"]}
    assert consumer.commits == [{boot: 11, status: 21}]
