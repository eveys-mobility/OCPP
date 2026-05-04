"""Kafka → ClickHouse ingestor sidecar (E2-14, ADR-0020).

A long-lived process that subscribes to all four event topics
(``cp.meter``, ``cp.boot``, ``cp.status``, ``tx.started``), parses
each ``EventEnvelope``, and INSERTs the matching row into the
matching ClickHouse table.

Why a sidecar (and not ClickHouse's Kafka Engine): full discussion
in ADR-0020. Short version: this is the path with the cleanest
debugging story and the cleanest place to branch on
``schema_version`` if/when we evolve the envelope.

Lifecycle:
    consumer.start() → asynch.Connection.connect()
    loop:
        wait up to ``batch_max_seconds`` for ``batch_size`` messages
        parse each → row dict
        per topic: INSERT ... VALUES (... batch ...)
        commit Kafka offsets (only if INSERT succeeded — at-least-once)
    on signal:
        flush in-flight batch
        consumer.stop() → asynch.Connection.close()

Operational notes:
- One process per ClickHouse instance / Kafka group. Multiple ingestor
  replicas would split the partition assignment automatically (Kafka
  consumer-group rebalance) but we run one for now (single-shard
  ClickHouse — see ADR-0020).
- At-least-once: a crash between INSERT-success and offset-commit
  re-inserts the next time. ``event_id`` in the envelope is the
  downstream dedup key.
- Batch size and time threshold are env-tunable; the defaults
  (500 / 5 s) are documented in ADR-0020.

Entrypoint: ``python -m eveys_ocpp.clickhouse.ingestor``.
"""

from __future__ import annotations

import asyncio
import signal
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from aiokafka import AIOKafkaConsumer
from asynch import Connection

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.observability import bind_contextvars, configure_logging, get_logger
from eveys_ocpp.settings import Settings, get_settings

if TYPE_CHECKING:
    from aiokafka.structs import ConsumerRecord

log = get_logger(__name__)


# Mapping from Kafka topic → (table name, payload-extractor function).
# The extractor pulls the right `oneof` variant from the envelope and
# returns a dict suitable for ClickHouse INSERT. Centralizing the
# topic→table relationship here means adding a new event-type later
# is a one-line edit (plus the proto + DDL + handler).


def _parse_occurred_at(value: str) -> datetime:
    """Parse the envelope's ISO-8601 ``occurred_at`` into a tz-aware
    ``datetime``. asynch's ``DateTime64`` column writer expects a
    Python ``datetime`` (it reads ``tzinfo``); a raw string trips an
    ``AttributeError`` deep inside its block writer. Always return UTC
    (the column's declared zone).
    """
    # ``datetime.fromisoformat`` handles the ``+00:00`` suffix from
    # producers since 3.11. Fall back to UTC if the producer omitted
    # the offset.
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _envelope_meta(env: events_pb2.EventEnvelope) -> dict[str, Any]:
    """Common envelope columns shared across every event-table."""
    return {
        "event_id": env.event_id,
        "occurred_at": _parse_occurred_at(env.occurred_at),
        "cp_id": env.cp_id,
        "schema_version": env.schema_version,
        "trace_id": env.trace_id,
    }


def _row_cp_meter(env: events_pb2.EventEnvelope) -> dict[str, Any]:
    payload = env.cp_meter
    sv = list(payload.sampled_values)
    # ClickHouse Nested expects parallel arrays under each sub-column.
    # MessageToDict gives us the proto enum *name* (e.g. "MEASURAND_VOLTAGE")
    # for every enum field, which is what the DDL stores as String.
    return {
        **_envelope_meta(env),
        "connector_id": payload.connector_id,
        "transaction_id": payload.transaction_id,
        "charger_reported_at": payload.charger_reported_at,
        "sampled_values.value": [v.value for v in sv],
        "sampled_values.context": [events_pb2.Context.Name(v.context) for v in sv],
        "sampled_values.format": [events_pb2.Format.Name(v.format) for v in sv],
        "sampled_values.measurand": [events_pb2.Measurand.Name(v.measurand) for v in sv],
        "sampled_values.phase": [events_pb2.Phase.Name(v.phase) for v in sv],
        "sampled_values.location": [events_pb2.Location.Name(v.location) for v in sv],
        "sampled_values.unit": [events_pb2.Unit.Name(v.unit) for v in sv],
    }


def _row_cp_status(env: events_pb2.EventEnvelope) -> dict[str, Any]:
    payload = env.cp_status
    return {
        **_envelope_meta(env),
        "connector_id": payload.connector_id,
        "status": payload.status,
        "error_code": payload.error_code,
        "info": payload.info,
        "vendor_id": payload.vendor_id,
        "vendor_error_code": payload.vendor_error_code,
        "charger_reported_at": payload.charger_reported_at,
    }


def _row_cp_boot(env: events_pb2.EventEnvelope) -> dict[str, Any]:
    payload = env.cp_boot
    return {
        **_envelope_meta(env),
        "vendor": payload.vendor,
        "model": payload.model,
        "firmware_version": payload.firmware_version,
        "serial_number": payload.serial_number,
        "status": events_pb2.CpBootStatus.Name(payload.status),
    }


def _row_tx_started(env: events_pb2.EventEnvelope) -> dict[str, Any]:
    payload = env.tx_started
    return {
        **_envelope_meta(env),
        "transaction_id": payload.transaction_id,
        "connector_id": payload.connector_id,
        "id_tag": payload.id_tag,
        "meter_start_wh": payload.meter_start_wh,
        "charger_reported_at": payload.charger_reported_at,
    }


# `oneof` field name → (table, extractor). The protobuf-generated
# `WhichOneof("payload")` returns the field name of the set variant.
_DISPATCH: dict[str, tuple[str, Any]] = {
    "cp_meter": ("cp_meter", _row_cp_meter),
    "cp_status": ("cp_status", _row_cp_status),
    "cp_boot": ("cp_boot", _row_cp_boot),
    "tx_started": ("tx_started", _row_tx_started),
    # Note: cp_connected is intentionally not ingested — that variant
    # is a registry-presence event, not telemetry. If a future ADR
    # decides we want it in CH, add a row here.
}


@dataclass(frozen=True)
class _Row:
    table: str
    columns: dict[str, Any]


class ClickHouseIngestor:
    """Long-lived Kafka→ClickHouse pipeline.

    One instance per process; managed by ``main()``. Designed to be
    shutdown-safe: the running ``ingest_loop`` honors a shutdown event
    and flushes the in-flight batch before exit.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._consumer: AIOKafkaConsumer | None = None
        self._conn: Connection | None = None
        self._shutdown = asyncio.Event()

    async def start(self) -> None:
        topics = (
            self._settings.kafka_topic_cp_meter,
            self._settings.kafka_topic_cp_status,
            self._settings.kafka_topic_cp_boot,
            self._settings.kafka_topic_tx_started,
        )
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self._settings.kafka_brokers,
            group_id=self._settings.clickhouse_ingestor_group,
            client_id="eveys-ocpp-clickhouse-ingestor",
            # Manual commit: only commit after the batch INSERTs.
            # at-least-once. ADR-0020.
            enable_auto_commit=False,
            # Earliest so a brand-new ingestor catches existing rows on
            # first deploy. Production deploys can rewind by deleting
            # and recreating the consumer group.
            auto_offset_reset="earliest",
        )
        await consumer.start()
        self._consumer = consumer

        conn = Connection(
            host=self._settings.clickhouse_host,
            port=self._settings.clickhouse_port,
            database=self._settings.clickhouse_db,
        )
        await conn.connect()
        self._conn = conn

        log.info(
            "clickhouse.ingestor.started",
            kafka_brokers=self._settings.kafka_brokers,
            clickhouse_host=self._settings.clickhouse_host,
            clickhouse_db=self._settings.clickhouse_db,
            topics=list(topics),
        )

    async def stop(self) -> None:
        self._shutdown.set()
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
        log.info("clickhouse.ingestor.stopped")

    async def _flush_batch(self, rows: Iterable[_Row]) -> None:
        """INSERT a batch into ClickHouse, grouped by table.

        Uses one ``INSERT INTO <table> VALUES`` per table touched in
        the batch. ``executemany`` lets asynch send all rows in one
        round-trip. Failure raises; the caller does NOT commit
        offsets, so the next poll re-delivers the batch.
        """
        assert self._conn is not None  # narrowed by start()

        # Group by table. Order within a table is preserved.
        by_table: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            by_table.setdefault(r.table, []).append(r.columns)

        for table, batch in by_table.items():
            if not batch:
                continue
            # Column list order must be stable across the batch — pull
            # from the first row, every other row's keys must match.
            cols = list(batch[0].keys())
            # asynch (clickhouse-driver under the hood) uses ClickHouse's
            # native binary protocol for INSERTs, not SQL placeholder
            # substitution. The query is "INSERT INTO t (cols) VALUES"
            # (no inline placeholders, no trailing parens) and the driver
            # ships the rows as a column block over the wire. Adding
            # ``%(name)s`` placeholders here would make CH try to parse
            # them as SQL — see Code: 62 errors in early E2-14 testing.
            sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES"
            async with self._conn.cursor() as cursor:
                await cursor.executemany(sql, batch)
            log.info(
                "clickhouse.ingestor.batch_inserted",
                table=table,
                rows=len(batch),
            )

    async def _process_record(self, record: ConsumerRecord) -> _Row | None:
        """Decode one Kafka record. Returns None on parse failure
        (logged; the message is dropped — at-least-once is at the
        batch level, not the per-message level)."""
        try:
            envelope = events_pb2.EventEnvelope()
            envelope.ParseFromString(record.value)
        except Exception as exc:
            log.warning(
                "clickhouse.ingestor.parse_failed",
                topic=record.topic,
                offset=record.offset,
                error=str(exc),
            )
            return None

        which = envelope.WhichOneof("payload")
        dispatch = _DISPATCH.get(which) if which else None
        if dispatch is None:
            log.warning(
                "clickhouse.ingestor.unknown_payload",
                topic=record.topic,
                offset=record.offset,
                which=which,
            )
            return None

        table, extractor = dispatch
        bind_contextvars(cp_id=envelope.cp_id, event_id=envelope.event_id)
        try:
            return _Row(table=table, columns=extractor(envelope))
        except Exception as exc:
            log.exception(
                "clickhouse.ingestor.extract_failed",
                topic=record.topic,
                offset=record.offset,
                error=str(exc),
            )
            return None

    async def ingest_loop(self) -> None:
        """Main loop: poll, batch, INSERT, commit. Runs until
        ``stop()`` is called or the consumer raises a non-recoverable
        error."""
        assert self._consumer is not None  # narrowed by start()

        batch_size = self._settings.clickhouse_ingestor_batch_size
        batch_seconds = self._settings.clickhouse_ingestor_batch_max_seconds

        while not self._shutdown.is_set():
            # `getmany` returns up to `max_records` messages or waits
            # `timeout_ms` for at least one. Both bounds together
            # implement the "500 rows OR 5 s" batch policy.
            try:
                records = await self._consumer.getmany(
                    timeout_ms=int(batch_seconds * 1000),
                    max_records=batch_size,
                )
            except Exception as exc:
                log.exception("clickhouse.ingestor.poll_failed", error=str(exc))
                await asyncio.sleep(1.0)
                continue

            rows: list[_Row] = []
            for _topic_partition, partition_records in records.items():
                for record in partition_records:
                    row = await self._process_record(record)
                    if row is not None:
                        rows.append(row)

            if not rows:
                continue  # poll timed out with nothing — go back to waiting

            try:
                await self._flush_batch(rows)
            except Exception as exc:
                # ClickHouse INSERT failed. Don't commit offsets — the
                # next poll re-delivers. Backoff briefly to avoid a hot
                # loop against a misbehaving broker/CH.
                log.exception("clickhouse.ingestor.flush_failed", error=str(exc))
                await asyncio.sleep(1.0)
                continue

            # Commit only after the batch landed successfully.
            try:
                await self._consumer.commit()
            except Exception as exc:
                log.exception("clickhouse.ingestor.commit_failed", error=str(exc))


async def _run() -> None:
    settings = get_settings()
    configure_logging(level=settings.log_level, json=settings.log_json)
    ingestor = ClickHouseIngestor(settings)
    await ingestor.start()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, ingestor._shutdown.set)

    try:
        await ingestor.ingest_loop()
    finally:
        await ingestor.stop()


def main() -> None:
    """CLI entry point: ``python -m eveys_ocpp.clickhouse.ingestor``."""
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()


# Re-export for tests + make __all__ explicit.
__all__ = ["ClickHouseIngestor", "main"]
