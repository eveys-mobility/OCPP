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
- At-least-once: a crash or ambiguous INSERT acknowledgement before
  offset-commit can re-insert rows. The current MergeTree schemas do
  not enforce uniqueness of ``event_id``. In-memory acknowledgements
  avoid retrying tables known to have succeeded in this process only.
- Batch size and time threshold are env-tunable; the defaults
  (500 / 5 s) are documented in ADR-0020.

Entrypoint: ``python -m eveys_ocpp.clickhouse.ingestor``.
"""

from __future__ import annotations

import asyncio
import json
import signal
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from aiokafka import AIOKafkaConsumer
from aiokafka.abc import ConsumerRebalanceListener
from asynch import Connection

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.metrics.server import MetricsServer
from eveys_ocpp.observability import bind_contextvars, configure_logging, get_logger
from eveys_ocpp.settings import Settings, get_settings

if TYPE_CHECKING:
    from aiokafka.structs import ConsumerRecord

log = get_logger(__name__)


class IngestorFatalError(RuntimeError):
    """Raised by the ingest loop when a misconfiguration is wedging
    the pipeline — too many consecutive INSERT failures, schema
    mismatch, etc. Surfaces a non-zero exit code so the orchestrator
    (docker compose, kubernetes) restarts the process and the operator
    sees a CrashLoopBackOff instead of a green container that's
    silently broken."""


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


def _row_cp_offline_duration(env: events_pb2.EventEnvelope) -> dict[str, Any]:
    payload = env.cp_offline_duration
    return {
        **_envelope_meta(env),
        "went_offline_at": _parse_occurred_at(payload.went_offline_at),
        "came_online_at": _parse_occurred_at(payload.came_online_at),
        "offline_seconds": payload.offline_seconds,
        "prior_pod_id": payload.prior_pod_id,
        "prior_reason": payload.prior_reason,
    }


def _extract_transaction_id(raw_payload: str) -> int | None:
    """Pull ``transactionId`` out of an OCPP frame's JSON when present.

    OCPP-J frames are a 4-element JSON array:
        [messageTypeId, messageId, action, payload]   # CALL
        [messageTypeId, messageId, payload]           # CALLRESULT / CALLERROR

    The ``transactionId`` field shows up inside the payload object on
    a small set of message types (StartTransaction.conf,
    StopTransaction.req, MeterValues.req, RemoteStartTransaction.req
    when the central system pins the id, …). Historical outbound
    audits used ``transaction_id`` before exact wire capture; accept
    that spelling too. Already persisted NULL indexes are not backfilled.

    Returns ``None`` for: unparseable JSON, unexpected frame shape, no
    ``transactionId`` key, or a value that isn't coercible to int.
    Built to never raise; the ingestor must not crash on a malformed
    frame, just store the row without the tx index hint.
    """
    if not raw_payload:
        return None
    try:
        frame = json.loads(raw_payload)
    except (ValueError, TypeError):
        return None
    if not isinstance(frame, list) or len(frame) < 3:
        return None
    # Payload is the last element regardless of CALL vs RESULT/ERROR.
    body = frame[-1]
    if not isinstance(body, dict):
        return None
    value = body.get("transactionId")
    if value is None:
        # Pre-fix outbound audits reconstructed dataclasses without the
        # library's camelCase serialization. Preserve historical indexing.
        value = body.get("transaction_id")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _row_cp_ocpp_frame(env: events_pb2.EventEnvelope) -> dict[str, Any]:
    payload = env.cp_ocpp_frame
    return {
        **_envelope_meta(env),
        "direction": payload.direction,
        "raw_payload": payload.raw_payload,
        "message_id": payload.message_id,
        "action": payload.action,
        "message_type": payload.message_type,
        "ocpp_version": payload.ocpp_version,
        "transaction_id": _extract_transaction_id(payload.raw_payload),
    }


# `oneof` field name → (table, extractor). The protobuf-generated
# `WhichOneof("payload")` returns the field name of the set variant.
_DISPATCH: dict[str, tuple[str, Any]] = {
    "cp_meter": ("cp_meter", _row_cp_meter),
    "cp_status": ("cp_status", _row_cp_status),
    "cp_boot": ("cp_boot", _row_cp_boot),
    "tx_started": ("tx_started", _row_tx_started),
    "cp_offline_duration": ("cp_offline_duration", _row_cp_offline_duration),
    "cp_ocpp_frame": ("cp_ocpp_frames", _row_cp_ocpp_frame),
    # Note: cp_connected is intentionally not ingested — that variant
    # is a registry-presence event, not telemetry. If a future ADR
    # decides we want it in CH, add a row here.
}


@dataclass(frozen=True)
class _Row:
    table: str
    columns: dict[str, Any]


class _ProgressListener(ConsumerRebalanceListener):
    """Invalidate process-local observations; never alter pending delivery."""

    def __init__(self, ingestor: ClickHouseIngestor) -> None:
        self._ingestor = ingestor

    async def on_partitions_revoked(self, revoked: Iterable[Any]) -> None:
        self._ingestor._reset_progress(revoked)

    async def on_partitions_assigned(self, assigned: Iterable[Any]) -> None:
        self._ingestor._reset_progress(assigned)


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
        # Counter of consecutive `_flush_batch` failures, reset on every
        # successful INSERT. When the count reaches
        # ``settings.clickhouse_ingestor_max_flush_failures`` the loop
        # raises ``IngestorFatalError`` so compose / k8s restart the
        # process and the misconfiguration becomes visible as a
        # CrashLoopBackOff. Without this the process logs forever and
        # silently fails to deliver events — see issue #24.
        self._consecutive_flush_failures = 0
        self._acknowledged_offsets: dict[Any, int] = {}
        self._observed_partitions: set[Any] = set()
        self._last_poll_timestamp = 0.0
        self._assignment_epoch = 0

    def _reset_progress(self, partitions: Iterable[Any]) -> None:
        self._assignment_epoch += 1
        for tp in partitions:
            self._acknowledged_offsets.pop(tp, None)
            labels = (tp.topic, str(tp.partition))
            metrics_registry.CLICKHOUSE_INGESTOR_CONSUMER_LAG_MESSAGES.labels(*labels).set(-1)
            metrics_registry.CLICKHOUSE_INGESTOR_CONSUMER_LAG_KNOWN.labels(*labels).set(0)
            metrics_registry.CLICKHOUSE_INGESTOR_LAG_SAMPLE_TIMESTAMP_SECONDS.labels(*labels).set(0)

    def _observe_progress(self) -> None:
        """Read only cached assignment/highwater; never make a metadata RPC.

        Initial/renewed assignments have unknown lag until this process
        observes a commit ACK. Consumer position can advance before INSERT
        and is deliberately never used as the delivered watermark.
        """
        if self._consumer is None:
            return
        gauges = (
            metrics_registry.CLICKHOUSE_INGESTOR_CONSUMER_LAG_MESSAGES,
            metrics_registry.CLICKHOUSE_INGESTOR_CONSUMER_LAG_KNOWN,
            metrics_registry.CLICKHOUSE_INGESTOR_LAG_SAMPLE_TIMESTAMP_SECONDS,
        )
        try:
            assigned = set(self._consumer.assignment())
            for tp in self._observed_partitions - assigned:
                for gauge in gauges:
                    gauge.remove(tp.topic, str(tp.partition))
            for tp in self._acknowledged_offsets.keys() - assigned:
                self._acknowledged_offsets.pop(tp, None)
            self._observed_partitions = assigned
            for tp in assigned:
                highwater = self._consumer.highwater(tp)
                fetched_at_ms = self._consumer.last_poll_timestamp(tp)
                acknowledged = self._acknowledged_offsets.get(tp)
                known = (
                    acknowledged is not None
                    and highwater is not None
                    and highwater >= acknowledged
                    and fetched_at_ms is not None
                    and fetched_at_ms > 0
                )
                labels = (tp.topic, str(tp.partition))
                gauges[0].labels(*labels).set(highwater - acknowledged if known else -1)
                gauges[1].labels(*labels).set(int(known))
                # This timestamp accompanies the cached broker FetchResponse.
                # A later local/empty getmany must not make old highwater fresh.
                gauges[2].labels(*labels).set(fetched_at_ms / 1000 if known else 0)
        except Exception as exc:
            # Optional observation must not alter INSERT/commit ordering.
            for tp in self._observed_partitions:
                gauges[0].labels(tp.topic, str(tp.partition)).set(-1)
                gauges[1].labels(tp.topic, str(tp.partition)).set(0)
                gauges[2].labels(tp.topic, str(tp.partition)).set(0)
            metrics_registry.CLICKHOUSE_INGESTOR_ERRORS_TOTAL.labels(stage="observe").inc()
            log.warning("clickhouse.ingestor.progress_unknown", error_class=type(exc).__name__)

    async def start(self) -> None:
        self._shutdown.clear()
        self._acknowledged_offsets.clear()
        topics = (
            self._settings.kafka_topic_cp_meter,
            self._settings.kafka_topic_cp_status,
            self._settings.kafka_topic_cp_boot,
            self._settings.kafka_topic_tx_started,
            self._settings.kafka_topic_cp_offline_duration,
            self._settings.kafka_topic_cp_ocpp_frames,
        )
        consumer = AIOKafkaConsumer(
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
        consumer.subscribe(topics, listener=_ProgressListener(self))
        self._consumer = consumer
        try:
            await consumer.start()
            await self._connect()
        except BaseException:
            # A failed/cancelled CH connection must not leave a Kafka
            # consumer running while this process exits or is retried.
            try:
                await self.stop()
            except BaseException as cleanup_error:
                log.warning(
                    "clickhouse.ingestor.start_cleanup_failed",
                    error_class=type(cleanup_error).__name__,
                )
            raise
        metrics_registry.CLICKHOUSE_INGESTOR_RUNNING.set(1)

        log.info(
            "clickhouse.ingestor.started",
            kafka_brokers=self._settings.kafka_brokers,
            clickhouse_host=self._settings.clickhouse_host,
            clickhouse_db=self._settings.clickhouse_db,
            topics=list(topics),
        )

    async def _connect(self) -> None:
        conn = Connection(
            host=self._settings.clickhouse_host,
            port=self._settings.clickhouse_port,
            database=self._settings.clickhouse_db,
        )
        self._conn = conn
        await conn.connect()

    async def _discard_connection(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                await asyncio.wait_for(conn.close(), timeout=1.0)
            except Exception as exc:
                log.warning("clickhouse.ingestor.close_failed", error_class=type(exc).__name__)

    async def stop(self) -> None:
        self._shutdown.set()
        metrics_registry.CLICKHOUSE_INGESTOR_RUNNING.set(0)
        consumer, self._consumer = self._consumer, None
        try:
            if consumer is not None:
                try:
                    await asyncio.wait_for(consumer.stop(), timeout=2.0)
                except Exception as exc:
                    log.warning(
                        "clickhouse.ingestor.consumer_close_failed",
                        error_class=type(exc).__name__,
                    )
        finally:
            await self._discard_connection()
        log.info("clickhouse.ingestor.stopped")

    async def _flush_batch(
        self, rows: Iterable[_Row], *, completed_tables: set[str] | None = None
    ) -> None:
        """INSERT a batch into ClickHouse, grouped by table.

        Uses one ``INSERT INTO <table> VALUES`` per table touched in
        the batch. ``executemany`` lets asynch send all rows in one
        round-trip. Remember table acknowledgements while retrying the
        same in-memory batch; a later table failure must not reinsert a
        table already acknowledged. This cannot deduplicate a crash or
        an INSERT whose server acknowledgement was lost.
        """
        if completed_tables is None:
            completed_tables = set()

        # Group by table. Order within a table is preserved.
        by_table: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            by_table.setdefault(r.table, []).append(r.columns)

        for table, batch in by_table.items():
            if not batch or table in completed_tables:
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
            try:
                if self._conn is None:
                    await self._connect()
                assert self._conn is not None
                async with self._conn.cursor() as cursor:
                    await cursor.executemany(sql, batch)
            except BaseException:
                # Failed/cancelled INSERTs can leave the native driver's
                # query flag set. Retry the pending batch on a new socket.
                await self._discard_connection()
                raise
            completed_tables.add(table)
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

    def _now(self) -> float:
        """Monotonic clock backing the batch deadline.

        A method rather than a direct `loop.time()` call so tests can
        drive the linger window without sleeping on the wall clock.
        """
        return asyncio.get_running_loop().time()

    async def ingest_loop(self) -> None:
        """Main loop: accumulate, INSERT, commit. Runs until ``stop()``
        is called or the consumer raises a non-recoverable error.

        Rows are accumulated ACROSS polls until `batch_size` rows or the
        `batch_max_seconds` deadline, whichever comes first.

        A single `getmany` does not implement that policy. Its
        `timeout_ms` bounds only the wait for the *first* record — it is
        not a linger, so the call returns as soon as anything is
        buffered. Under a steady trickle every poll came back with one
        message and we wrote one row per INSERT; in ClickHouse one
        INSERT is one part, so a ~1.5 events/sec stream produced
        thousands of single-row parts an hour and left the merge
        scheduler doing all the work.

        A poll advances the consumer position, so failed batches must
        stay in memory: never poll a later batch until this one is
        written and its explicit per-partition next offsets committed.
        """
        assert self._consumer is not None  # narrowed by start()

        batch_size = self._settings.clickhouse_ingestor_batch_size
        batch_seconds = self._settings.clickhouse_ingestor_batch_max_seconds

        while not self._shutdown.is_set():
            rows: list[_Row] = []
            offsets: dict[Any, int] = {}
            deadline = self._now() + batch_seconds
            poll_failed = False

            # ---- accumulate until full, out of time, or shutting down
            while not self._shutdown.is_set() and len(rows) < batch_size:
                remaining = deadline - self._now()
                if remaining <= 0:
                    break
                try:
                    records = await self._consumer.getmany(
                        # Never block past the batch deadline. `max(1, …)`
                        # because `timeout_ms=0` is a non-blocking poll,
                        # which would hot-spin the last millisecond away.
                        timeout_ms=max(1, int(remaining * 1000)),
                        max_records=batch_size - len(rows),
                    )
                except Exception as exc:
                    metrics_registry.CLICKHOUSE_INGESTOR_ERRORS_TOTAL.labels(stage="poll").inc()
                    log.exception("clickhouse.ingestor.poll_failed", error=str(exc))
                    poll_failed = True
                    break

                self._last_poll_timestamp = time.time()
                metrics_registry.CLICKHOUSE_INGESTOR_LAST_POLL_TIMESTAMP_SECONDS.set(
                    self._last_poll_timestamp
                )
                self._observe_progress()

                for topic_partition, partition_records in records.items():
                    for record in partition_records:
                        # Kafka commit offsets identify the NEXT record.
                        # Include intentionally dropped malformed records
                        # under the existing parse/drop policy, but never
                        # consumer positions outside this batch.
                        offsets[topic_partition] = max(
                            offsets.get(topic_partition, 0), record.offset + 1
                        )
                        row = await self._process_record(record)
                        if row is not None:
                            rows.append(row)

            # ---- flush, then (only then) commit ---------------------
            #
            # Shutdown breaks the inner loop rather than discarding
            # what it collected, so a partial batch is still flushed
            # and committed on the way out.
            if offsets:
                completed_tables: set[str] = set()
                written = False
                commit_failures = 0
                limit = self._settings.clickhouse_ingestor_max_flush_failures
                while True:
                    if not written:
                        try:
                            if rows:
                                await self._flush_batch(rows, completed_tables=completed_tables)
                        except Exception as exc:
                            metrics_registry.CLICKHOUSE_INGESTOR_ERRORS_TOTAL.labels(
                                stage="flush"
                            ).inc()
                            self._consecutive_flush_failures += 1
                            log.exception(
                                "clickhouse.ingestor.flush_failed",
                                error=str(exc),
                                consecutive_failures=self._consecutive_flush_failures,
                                max_failures=limit,
                            )
                            if self._consecutive_flush_failures >= limit:
                                raise IngestorFatalError(
                                    f"flush failed {self._consecutive_flush_failures} "
                                    f"times in a row (limit {limit}); aborting"
                                ) from exc
                            if self._shutdown.is_set():
                                return  # keep offsets uncommitted for restart
                            await asyncio.sleep(1.0)
                            continue
                        written = True
                        self._consecutive_flush_failures = 0

                    try:
                        commit_epoch = self._assignment_epoch
                        await self._consumer.commit(offsets)
                    except Exception as exc:
                        metrics_registry.CLICKHOUSE_INGESTOR_ERRORS_TOTAL.labels(
                            stage="commit"
                        ).inc()
                        commit_failures += 1
                        log.exception(
                            "clickhouse.ingestor.commit_failed",
                            error=str(exc),
                            consecutive_failures=commit_failures,
                        )
                        if commit_failures >= limit:
                            raise IngestorFatalError(
                                f"commit failed {commit_failures} times in a row "
                                f"(limit {limit}); aborting"
                            ) from exc
                        if self._shutdown.is_set():
                            return
                        await asyncio.sleep(1.0)
                        continue  # retry commit without another INSERT or poll
                    if commit_epoch == self._assignment_epoch:
                        self._acknowledged_offsets.update(offsets)
                    metrics_registry.CLICKHOUSE_INGESTOR_LAST_SUCCESSFUL_COMMIT_TIMESTAMP_SECONDS.set(
                        time.time()
                    )
                    self._observe_progress()
                    break

            if poll_failed:
                await asyncio.sleep(1.0)


async def _run() -> None:
    settings = get_settings()
    configure_logging(level=settings.log_level, json=settings.log_json)
    ingestor = ClickHouseIngestor(settings)
    metrics_server = (
        MetricsServer(
            host=settings.metrics_host,
            port=settings.metrics_port,
            include_python_collectors=settings.metrics_include_python_collectors,
        )
        if settings.metrics_enabled
        else None
    )
    try:
        if metrics_server is not None:
            await metrics_server.start()
        await ingestor.start()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, ingestor._shutdown.set)
        await ingestor.ingest_loop()
    finally:
        try:
            await ingestor.stop()
        finally:
            if metrics_server is not None:
                await metrics_server.stop()


def main() -> None:
    """CLI entry point: ``python -m eveys_ocpp.clickhouse.ingestor``."""
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        sys.exit(0)
    except IngestorFatalError as exc:
        # Non-zero exit so docker compose / kubernetes restart us.
        # Already logged in the loop with the rolling counter; here we
        # just emit a single line so an operator tailing the container
        # sees the cause without scrolling.
        log.error("clickhouse.ingestor.exit_fatal", error=str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()


# Re-export for tests + make __all__ explicit.
__all__ = ["ClickHouseIngestor", "IngestorFatalError", "main"]
