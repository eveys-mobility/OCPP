"""Unit tests for the ClickHouse ingestor (E2-14, ADR-0020).

The end-to-end Kafka→CH round-trip is in tests/e2e/. These are the
pure-function tests that don't need Kafka or ClickHouse running:
the four row-extractors, the dispatch table, and the parse-failure
guards in `_process_record`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.clickhouse.ingestor import (
    _DISPATCH,
    ClickHouseIngestor,
    _envelope_meta,
    _row_cp_boot,
    _row_cp_meter,
    _row_cp_status,
    _row_tx_started,
)
from eveys_ocpp.settings import Settings


def _envelope(**payload_kwargs: Any) -> events_pb2.EventEnvelope:
    """Build an EventEnvelope with the standard metadata + a payload."""
    return events_pb2.EventEnvelope(
        event_id="evt-1",
        occurred_at="2026-05-01T00:00:00.000+00:00",
        cp_id="CP_TEST",
        schema_version="v1",
        trace_id="trace-1",
        **payload_kwargs,
    )


# ---- Row extractors --------------------------------------------------------


def test_envelope_meta_returns_all_metadata_columns() -> None:
    env = _envelope(cp_boot=events_pb2.CpBoot(vendor="ACME"))
    meta = _envelope_meta(env)
    assert meta == {
        "event_id": "evt-1",
        "occurred_at": "2026-05-01T00:00:00.000+00:00",
        "cp_id": "CP_TEST",
        "schema_version": "v1",
        "trace_id": "trace-1",
    }


def test_row_cp_boot_serializes_enum_to_name() -> None:
    """The proto enum becomes its string variant name."""
    env = _envelope(
        cp_boot=events_pb2.CpBoot(
            vendor="ACME",
            model="X1",
            firmware_version="1.0.0",
            serial_number="SN001",
            status=events_pb2.CP_BOOT_STATUS_ACCEPTED,
        )
    )
    row = _row_cp_boot(env)
    assert row["vendor"] == "ACME"
    assert row["model"] == "X1"
    assert row["status"] == "CP_BOOT_STATUS_ACCEPTED"


def test_row_cp_status_carries_optional_vendor_fields() -> None:
    env = _envelope(
        cp_status=events_pb2.CpStatus(
            connector_id=2,
            status="Charging",
            error_code="NoError",
            info="hello",
            vendor_id="ACME",
            vendor_error_code="V1",
            charger_reported_at="2026-05-01T00:00:00+00:00",
        )
    )
    row = _row_cp_status(env)
    assert row["connector_id"] == 2
    assert row["status"] == "Charging"
    assert row["info"] == "hello"
    assert row["vendor_id"] == "ACME"
    assert row["vendor_error_code"] == "V1"


def test_row_tx_started_carries_int64_meter() -> None:
    env = _envelope(
        tx_started=events_pb2.TxStarted(
            transaction_id=999,
            connector_id=1,
            id_tag="VALID_RFID",
            meter_start_wh=12345,
            charger_reported_at="2026-05-01T00:00:00+00:00",
        )
    )
    row = _row_tx_started(env)
    assert row["transaction_id"] == 999
    assert row["meter_start_wh"] == 12345
    assert row["id_tag"] == "VALID_RFID"


def test_row_cp_meter_flattens_repeated_to_nested_arrays() -> None:
    """SampledValue (repeated) lands as parallel arrays under
    `sampled_values.*` — the ClickHouse `Nested` insert shape."""
    env = _envelope(
        cp_meter=events_pb2.CpMeter(
            connector_id=1,
            transaction_id=42,
            charger_reported_at="2026-05-01T00:00:00+00:00",
            sampled_values=[
                events_pb2.SampledValue(
                    value="230.5",
                    measurand=events_pb2.MEASURAND_VOLTAGE,
                    unit=events_pb2.UNIT_V,
                ),
                events_pb2.SampledValue(
                    value="16.2",
                    measurand=events_pb2.MEASURAND_CURRENT_IMPORT,
                    unit=events_pb2.UNIT_A,
                    phase=events_pb2.PHASE_L1,
                ),
            ],
        )
    )
    row = _row_cp_meter(env)
    assert row["connector_id"] == 1
    assert row["transaction_id"] == 42
    assert row["sampled_values.value"] == ["230.5", "16.2"]
    assert row["sampled_values.measurand"] == [
        "MEASURAND_VOLTAGE",
        "MEASURAND_CURRENT_IMPORT",
    ]
    assert row["sampled_values.unit"] == ["UNIT_V", "UNIT_A"]
    # Default-zero values come back as the UNSPECIFIED variant name.
    assert row["sampled_values.phase"] == ["PHASE_UNSPECIFIED", "PHASE_L1"]


def test_row_cp_meter_with_no_sampled_values_yields_empty_arrays() -> None:
    """Empty repeated → empty arrays, not missing keys. Keeps the
    INSERT shape stable across batches."""
    env = _envelope(cp_meter=events_pb2.CpMeter(connector_id=0))
    row = _row_cp_meter(env)
    for nested_col in (
        "sampled_values.value",
        "sampled_values.context",
        "sampled_values.format",
        "sampled_values.measurand",
        "sampled_values.phase",
        "sampled_values.location",
        "sampled_values.unit",
    ):
        assert row[nested_col] == []


# ---- Dispatch table --------------------------------------------------------


def test_dispatch_table_covers_every_persisted_oneof() -> None:
    """The dispatch table maps every `oneof payload` variant we
    persist. `cp_connected` is intentionally missing (not telemetry —
    see ingestor.py module docstring)."""
    persisted = {"cp_meter", "cp_status", "cp_boot", "tx_started"}
    assert set(_DISPATCH.keys()) == persisted


def test_dispatch_uses_table_names_matching_ddl() -> None:
    """The (table, extractor) pair points at the table name in
    src/eveys_ocpp/clickhouse/ddl/, not the proto field name."""
    expected_tables = {"cp_meter", "cp_status", "cp_boot", "tx_started"}
    actual_tables = {table for (table, _extractor) in _DISPATCH.values()}
    assert actual_tables == expected_tables


# ---- Parse-failure guards in _process_record ------------------------------


@pytest.mark.asyncio
async def test_process_record_returns_none_on_parse_failure() -> None:
    """Garbage bytes → log + skip, never crash the loop."""
    ingestor = ClickHouseIngestor(Settings())
    record = SimpleNamespace(value=b"\x00\x00\x00garbage", topic="cp.meter", offset=0)
    row = await ingestor._process_record(record)
    assert row is None


@pytest.mark.asyncio
async def test_process_record_returns_none_on_unknown_payload_variant() -> None:
    """Envelope with no payload variant set → log + skip, never
    crash. Defensive guard in case a future producer ever sends an
    envelope without a payload (it shouldn't, but the consumer is
    not the right place to enforce that)."""
    ingestor = ClickHouseIngestor(Settings())
    env = events_pb2.EventEnvelope(
        event_id="evt-1",
        occurred_at="2026-05-01T00:00:00.000+00:00",
        cp_id="CP_TEST",
    )
    record = SimpleNamespace(value=env.SerializeToString(), topic="cp.meter", offset=0)
    row = await ingestor._process_record(record)
    assert row is None


@pytest.mark.asyncio
async def test_process_record_returns_a_row_for_a_well_formed_envelope() -> None:
    ingestor = ClickHouseIngestor(Settings())
    env = _envelope(
        cp_boot=events_pb2.CpBoot(vendor="ACME", status=events_pb2.CP_BOOT_STATUS_ACCEPTED)
    )
    record = SimpleNamespace(value=env.SerializeToString(), topic="cp.boot", offset=0)
    row = await ingestor._process_record(record)
    assert row is not None
    assert row.table == "cp_boot"
    assert row.columns["vendor"] == "ACME"
    assert row.columns["status"] == "CP_BOOT_STATUS_ACCEPTED"


# ---- Ingestor lifecycle (no real Kafka/CH) --------------------------------


@pytest.mark.asyncio
async def test_stop_is_safe_before_start() -> None:
    """Calling stop() on an ingestor that never started is a no-op,
    not a crash. Important for compose teardown when start() failed."""
    ingestor = ClickHouseIngestor(Settings())
    await ingestor.stop()  # should not raise


@pytest.mark.asyncio
async def test_stop_cleans_up_consumer_and_connection_when_started() -> None:
    """When both the consumer and CH connection are set, stop()
    awaits both shutdown calls."""
    ingestor = ClickHouseIngestor(Settings())
    fake_consumer = AsyncMock()
    fake_conn = AsyncMock()
    ingestor._consumer = fake_consumer
    ingestor._conn = fake_conn

    await ingestor.stop()

    fake_consumer.stop.assert_awaited_once()
    fake_conn.close.assert_awaited_once()
    assert ingestor._consumer is None
    assert ingestor._conn is None


# ---- start(): construction wiring ----------------------------------------
#
# The lifecycle path inside start() builds an AIOKafkaConsumer with a
# specific kwarg set (manual commit, earliest offset, all four topics
# subscribed) and an asynch.Connection. These are integration points
# we need to verify even without a real Kafka or ClickHouse — a wrong
# kwarg here means messages get auto-committed before INSERT (data
# loss) or the ingestor reads only future messages (missed events on
# a fresh deploy). Both are silent until production.


@pytest.mark.asyncio
async def test_start_constructs_kafka_consumer_with_at_least_once_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The consumer must use manual commit (`enable_auto_commit=False`)
    and `auto_offset_reset='earliest'`. Verify by capturing the
    constructor kwargs."""
    from eveys_ocpp.clickhouse import ingestor as ingestor_module

    captured_kwargs: dict[str, object] = {}

    def fake_consumer_ctor(*topics: str, **kwargs: object) -> object:
        captured_kwargs["topics"] = topics
        captured_kwargs.update(kwargs)
        fake = AsyncMock()
        return fake

    fake_conn = AsyncMock()
    fake_conn.connect = AsyncMock()

    monkeypatch.setattr(ingestor_module, "AIOKafkaConsumer", fake_consumer_ctor)
    monkeypatch.setattr(ingestor_module, "Connection", lambda **kw: fake_conn)

    settings = Settings(
        kafka_brokers="kafka:9092",
        kafka_topic_cp_meter="cp.meter",
        kafka_topic_cp_status="cp.status",
        kafka_topic_cp_boot="cp.boot",
        kafka_topic_tx_started="tx.started",
        clickhouse_ingestor_group="test-group",
    )
    ingestor = ClickHouseIngestor(settings)

    await ingestor.start()

    # All four event topics subscribed.
    assert set(captured_kwargs["topics"]) == {
        "cp.meter",
        "cp.status",
        "cp.boot",
        "tx.started",
    }
    # at-least-once: manual commit only after a successful INSERT.
    assert captured_kwargs["enable_auto_commit"] is False
    # Earliest so a brand-new deploy catches existing rows.
    assert captured_kwargs["auto_offset_reset"] == "earliest"
    assert captured_kwargs["bootstrap_servers"] == "kafka:9092"
    assert captured_kwargs["group_id"] == "test-group"

    # `connect()` was awaited on the asynch Connection.
    fake_conn.connect.assert_awaited_once()
    assert ingestor._consumer is not None
    assert ingestor._conn is fake_conn

    # Cleanup so other tests don't see the fakes leak.
    await ingestor.stop()


# ---- _flush_batch: per-table grouping + INSERT shape ---------------------


@pytest.mark.asyncio
async def test_flush_batch_groups_rows_by_table_and_executemany() -> None:
    """A batch with rows for multiple tables produces one INSERT per
    table, and the SQL uses the column names the row carries."""
    from eveys_ocpp.clickhouse.ingestor import _Row

    ingestor = ClickHouseIngestor(Settings())

    # Capture the SQL + batch passed to executemany. asynch's
    # cursor() is an async context manager.
    captured: list[tuple[str, list[dict[str, object]]]] = []

    class _FakeCursor:
        async def __aenter__(self) -> _FakeCursor:
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        async def executemany(self, sql: str, batch: list[dict[str, object]]) -> None:
            captured.append((sql, batch))

    fake_conn = MagicMock()
    fake_conn.cursor = lambda: _FakeCursor()
    ingestor._conn = fake_conn

    rows = [
        _Row(table="cp_boot", columns={"event_id": "e1", "vendor": "ACME"}),
        _Row(table="cp_meter", columns={"event_id": "e2", "connector_id": 1}),
        _Row(table="cp_boot", columns={"event_id": "e3", "vendor": "Foo"}),
    ]

    await ingestor._flush_batch(rows)

    # Two tables → two INSERT calls.
    assert len(captured) == 2

    by_table = {sql.split()[2]: (sql, batch) for sql, batch in captured}
    boot_sql, boot_batch = by_table["cp_boot"]
    _meter_sql, meter_batch = by_table["cp_meter"]

    # Column lists in the SQL match the keys in the row dicts.
    assert "event_id" in boot_sql
    assert "vendor" in boot_sql
    assert "%(event_id)s" in boot_sql  # parameterized placeholders
    # The two cp_boot rows arrive together; the cp_meter row stands alone.
    assert len(boot_batch) == 2
    assert len(meter_batch) == 1
    assert boot_batch[0]["event_id"] == "e1"
    assert boot_batch[1]["event_id"] == "e3"
    assert meter_batch[0]["connector_id"] == 1


@pytest.mark.asyncio
async def test_flush_batch_no_op_on_empty_input() -> None:
    """An empty `rows` iterable is a no-op (no cursor, no INSERTs).
    Avoids spurious INSERTs when a poll cycle produces only
    parse-failed messages."""
    ingestor = ClickHouseIngestor(Settings())
    fake_conn = MagicMock()
    fake_conn.cursor = MagicMock(side_effect=AssertionError("should not call cursor"))
    ingestor._conn = fake_conn

    await ingestor._flush_batch([])
    # If cursor() was called, AssertionError would have fired.


# ---- ingest_loop: poll → process → flush → commit -----------------------


@pytest.mark.asyncio
async def test_ingest_loop_commits_only_after_successful_flush(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The hot path: getmany returns one record, _process_record
    yields a row, _flush_batch succeeds, consumer.commit() is awaited.
    On the next iteration the shutdown event is set so the loop exits."""
    from eveys_ocpp.clickhouse.ingestor import _Row

    ingestor = ClickHouseIngestor(Settings())

    # Build a fake ConsumerRecord that _process_record can decode.
    env = events_pb2.EventEnvelope(
        event_id="evt-1",
        occurred_at="2026-05-04T00:00:00.000+00:00",
        cp_id="CP_TEST",
        cp_boot=events_pb2.CpBoot(vendor="ACME", status=events_pb2.CP_BOOT_STATUS_ACCEPTED),
    )
    record = SimpleNamespace(value=env.SerializeToString(), topic="cp.boot", offset=0)

    # Fake consumer: first getmany returns the record; subsequent
    # iterations would block, so we set the shutdown event after the
    # first round-trip via a side effect.
    poll_count = {"n": 0}

    async def fake_getmany(timeout_ms: int, max_records: int) -> dict[object, list[object]]:
        poll_count["n"] += 1
        if poll_count["n"] == 1:
            return {"part-0": [record]}
        # Signal shutdown after the first cycle so the loop exits.
        ingestor._shutdown.set()
        return {}

    fake_consumer = AsyncMock()
    fake_consumer.getmany = fake_getmany
    fake_consumer.commit = AsyncMock()
    ingestor._consumer = fake_consumer

    # Capture _flush_batch calls so we can verify "flush before commit".
    flushed_rows: list[list[_Row]] = []
    call_order: list[str] = []

    async def fake_flush(rows: list[_Row]) -> None:
        flushed_rows.append(list(rows))
        call_order.append("flush")

    async def fake_commit() -> None:
        call_order.append("commit")

    ingestor._flush_batch = fake_flush  # type: ignore[method-assign]
    fake_consumer.commit = fake_commit  # type: ignore[method-assign]

    await ingestor.ingest_loop()

    # The single record was decoded into one _Row and flushed.
    assert len(flushed_rows) == 1
    assert len(flushed_rows[0]) == 1
    assert flushed_rows[0][0].table == "cp_boot"
    # Commit happens AFTER flush — at-least-once guarantee.
    assert call_order == ["flush", "commit"]


@pytest.mark.asyncio
async def test_ingest_loop_skips_commit_when_flush_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If _flush_batch raises, offsets are NOT committed and the
    loop backs off, ready to retry on the next poll. Critical for
    at-least-once: a ClickHouse insert failure must not be masked by
    an early offset commit."""
    from eveys_ocpp.clickhouse.ingestor import _Row

    ingestor = ClickHouseIngestor(Settings())

    env = events_pb2.EventEnvelope(
        event_id="evt-1",
        occurred_at="2026-05-04T00:00:00.000+00:00",
        cp_id="CP_TEST",
        cp_boot=events_pb2.CpBoot(vendor="ACME"),
    )
    record = SimpleNamespace(value=env.SerializeToString(), topic="cp.boot", offset=0)

    poll_count = {"n": 0}

    async def fake_getmany(timeout_ms: int, max_records: int) -> dict[object, list[object]]:
        poll_count["n"] += 1
        if poll_count["n"] == 1:
            return {"part-0": [record]}
        ingestor._shutdown.set()
        return {}

    fake_consumer = AsyncMock()
    fake_consumer.getmany = fake_getmany
    commit_calls = {"n": 0}

    async def fake_commit() -> None:
        commit_calls["n"] += 1

    fake_consumer.commit = fake_commit  # type: ignore[method-assign]
    ingestor._consumer = fake_consumer

    async def failing_flush(rows: list[_Row]) -> None:
        raise RuntimeError("ClickHouse exploded")

    ingestor._flush_batch = failing_flush  # type: ignore[method-assign]

    # Patch sleep so the 1.0-s backoff doesn't slow the test.
    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("eveys_ocpp.clickhouse.ingestor.asyncio.sleep", fake_sleep)

    await ingestor.ingest_loop()

    # Flush failed → commit must NOT have been called.
    assert commit_calls["n"] == 0


@pytest.mark.asyncio
async def test_ingest_loop_handles_poll_failure_with_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A getmany() exception (e.g. broker disconnect) is caught,
    logged, and the loop backs off before retrying. Critical so a
    flailing broker doesn't turn into a hot loop."""
    ingestor = ClickHouseIngestor(Settings())

    poll_count = {"n": 0}

    async def fake_getmany(timeout_ms: int, max_records: int) -> dict[object, list[object]]:
        poll_count["n"] += 1
        if poll_count["n"] == 1:
            raise RuntimeError("broker disconnect")
        ingestor._shutdown.set()
        return {}

    fake_consumer = AsyncMock()
    fake_consumer.getmany = fake_getmany
    ingestor._consumer = fake_consumer

    sleep_count = {"n": 0}

    async def fake_sleep(_seconds: float) -> None:
        sleep_count["n"] += 1

    monkeypatch.setattr("eveys_ocpp.clickhouse.ingestor.asyncio.sleep", fake_sleep)

    await ingestor.ingest_loop()

    # The poll failure triggered the backoff sleep (poll_count[n]=1
    # raised; loop woke for shutdown on poll_count[n]=2).
    assert sleep_count["n"] >= 1


@pytest.mark.asyncio
async def test_ingest_loop_continues_when_poll_returns_no_records() -> None:
    """getmany returning an empty dict is the normal idle case
    (timeout with nothing on the broker). The loop must continue
    waiting; no flush, no commit, no crash."""
    ingestor = ClickHouseIngestor(Settings())

    poll_count = {"n": 0}

    async def fake_getmany(timeout_ms: int, max_records: int) -> dict[object, list[object]]:
        poll_count["n"] += 1
        if poll_count["n"] >= 3:
            ingestor._shutdown.set()
        return {}

    fake_consumer = AsyncMock()
    fake_consumer.getmany = fake_getmany
    fake_consumer.commit = AsyncMock()
    ingestor._consumer = fake_consumer

    flushed: list[object] = []

    async def fake_flush(rows: object) -> None:
        flushed.append(rows)

    ingestor._flush_batch = fake_flush  # type: ignore[method-assign]

    await ingestor.ingest_loop()

    # 3 polls, all empty → no flushes, no commits.
    assert flushed == []
    fake_consumer.commit.assert_not_called()
