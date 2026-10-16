"""ClickHouse ingestor progress, observed from the gateway process.

The ingestor commits its Kafka offsets only after the matching INSERT
succeeded (ADR-0020), so the committed offsets of its consumer group are
the archive watermark: ``log-end - committed`` is what has been
published but has not reached ClickHouse yet.

The gateway samples that watermark in the background with read-only
calls: Metadata for the ingested topics, OffsetFetch for the ingestor's
group, ListOffsets for the partitions. It never joins the ingestor's
group and never commits on its behalf. `/api/v1/health` only reads the
last sample, so a slow or unreachable broker cannot delay the endpoint.

States reported by :meth:`IngestorProgressProbe.snapshot`:

- ``ok``       every partition is within the lag threshold and moving.
- ``lagging``  total lag above ``health_ingestor_lag_threshold_messages``.
- ``stalled``  a partition has pending messages and its committed
               offset has not moved for ``health_ingestor_stall_seconds``.
- ``unknown``  no fresh sample (broker unreachable, probe just started).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, NamedTuple, Protocol

from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import UnknownTopicOrPartitionError, for_code
from aiokafka.structs import TopicPartition

from eveys_ocpp.observability import get_logger
from eveys_ocpp.settings import Settings

log = get_logger(__name__)

# One sample (metadata + offset fetch + two list-offsets) must finish
# within this budget. Runs in the background, never inside a request.
PROBE_TIMEOUT_SECONDS = 5.0
# A sample older than this many intervals no longer describes the
# archive; the snapshot falls back to `unknown`.
STALE_AFTER_INTERVALS = 3

IngestorState = Literal["ok", "lagging", "stalled", "unknown"]


def ingested_topics(settings: Settings) -> tuple[str, ...]:
    """Topics the ingestor archives into ClickHouse.

    ``tx.stopped`` is deliberately absent. It exists for webhook delivery;
    Postgres ``transactions`` is the durable record of a Stop, and the raw
    StopTransaction frame is archived through ``cp.ocpp_frames``. There is
    no ``tx_stopped`` ClickHouse table (see #166).
    """
    return (
        settings.kafka_topic_cp_meter,
        settings.kafka_topic_cp_status,
        settings.kafka_topic_cp_boot,
        settings.kafka_topic_tx_started,
        settings.kafka_topic_cp_offline_duration,
        settings.kafka_topic_cp_ocpp_frames,
    )


class PartitionOffsets(NamedTuple):
    """Offsets of one partition. ``committed`` is None when the group has
    never committed there; the ingestor would then start at ``start``."""

    committed: int | None
    start: int
    end: int

    @property
    def lag(self) -> int:
        delivered = self.start if self.committed is None else max(self.committed, self.start)
        return max(0, self.end - delivered)


class OffsetSource(Protocol):
    async def read(
        self, group_id: str, topics: Sequence[str]
    ) -> Mapping[TopicPartition, PartitionOffsets]: ...

    async def reset(self) -> None: ...


class KafkaOffsetSource:
    """Read-only Kafka access: an admin client for the group's committed
    offsets and a group-less consumer for partition start/end offsets."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._admin: AIOKafkaAdminClient | None = None
        self._consumer: AIOKafkaConsumer | None = None

    async def _open(self) -> tuple[AIOKafkaAdminClient, AIOKafkaConsumer]:
        if self._admin is None or self._consumer is None:
            timeout_ms = int(PROBE_TIMEOUT_SECONDS * 1000)
            admin = AIOKafkaAdminClient(
                bootstrap_servers=self._settings.kafka_brokers,
                client_id="eveys-ocpp-health",
                request_timeout_ms=timeout_ms,
            )
            # group_id=None: the consumer never joins a group and never
            # commits; it is only used for ListOffsets.
            consumer = AIOKafkaConsumer(
                bootstrap_servers=self._settings.kafka_brokers,
                client_id="eveys-ocpp-health",
                group_id=None,
                enable_auto_commit=False,
                request_timeout_ms=timeout_ms,
            )
            self._admin, self._consumer = admin, consumer
            await admin.start()
            await consumer.start()
        return self._admin, self._consumer

    async def read(
        self, group_id: str, topics: Sequence[str]
    ) -> dict[TopicPartition, PartitionOffsets]:
        admin, consumer = await self._open()
        partitions: list[TopicPartition] = []
        for topic in await admin.describe_topics(list(topics)):
            error_code = topic["error_code"]
            if error_code == UnknownTopicOrPartitionError.errno:
                # Nothing was ever published there, so nothing to archive.
                continue
            if error_code:
                raise for_code(error_code)(f"metadata for {topic['topic']}")
            partitions.extend(
                TopicPartition(topic["topic"], p["partition"]) for p in topic["partitions"]
            )
        if not partitions:
            return {}
        committed = await admin.list_consumer_group_offsets(group_id, partitions=partitions)
        starts = await consumer.beginning_offsets(partitions)
        ends = await consumer.end_offsets(partitions)
        result: dict[TopicPartition, PartitionOffsets] = {}
        for tp in partitions:
            meta = committed.get(tp)
            offset = meta.offset if meta is not None and meta.offset >= 0 else None
            result[tp] = PartitionOffsets(committed=offset, start=starts[tp], end=ends[tp])
        return result

    async def reset(self) -> None:
        """Drop both clients; the next read reconnects."""
        admin, self._admin = self._admin, None
        consumer, self._consumer = self._consumer, None
        for name, close in (
            ("admin", admin.close if admin is not None else None),
            ("consumer", consumer.stop if consumer is not None else None),
        ):
            if close is None:
                continue
            try:
                await asyncio.wait_for(close(), timeout=2.0)
            except Exception as exc:
                log.warning(
                    "ingestor_progress.close_failed", client=name, error_class=type(exc).__name__
                )


@dataclass(frozen=True)
class IngestorProgress:
    state: IngestorState
    reason: str | None
    lag_messages: int | None
    last_commit_at: datetime | None
    observed_at: datetime | None
    error_class: str | None


@dataclass
class _PartitionTrack:
    committed: int | None
    # First sample that saw pending messages at the current committed
    # offset; None while the partition is caught up.
    pending_since: float | None


class IngestorProgressProbe:
    """Background sampler of the ingestor's consumer-group lag."""

    def __init__(
        self,
        settings: Settings,
        *,
        source: OffsetSource | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._group_id = settings.clickhouse_ingestor_group
        self._topics = ingested_topics(settings)
        self._interval = settings.health_ingestor_probe_interval_seconds
        self._lag_threshold = settings.health_ingestor_lag_threshold_messages
        self._stall_seconds = settings.health_ingestor_stall_seconds
        self._source: OffsetSource = source if source is not None else KafkaOffsetSource(settings)
        self._clock = clock
        self._task: asyncio.Task[None] | None = None
        self._tracks: dict[TopicPartition, _PartitionTrack] = {}
        self._observed_at: float | None = None
        self._lag: int | None = None
        self._last_commit_at: float | None = None
        self._error_class: str | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="ingestor_progress_probe")

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self._source.reset()

    async def _run(self) -> None:
        while True:
            await self.sample_once()
            await asyncio.sleep(self._interval)

    async def sample_once(self) -> None:
        try:
            async with asyncio.timeout(PROBE_TIMEOUT_SECONDS):
                offsets = await self._source.read(self._group_id, self._topics)
        except Exception as exc:
            self._error_class = type(exc).__name__
            log.warning("ingestor_progress.sample_failed", error_class=self._error_class)
            await self._source.reset()
            return
        self._record(offsets, self._clock())

    def _record(self, offsets: Mapping[TopicPartition, PartitionOffsets], now: float) -> None:
        tracks: dict[TopicPartition, _PartitionTrack] = {}
        for tp, current in offsets.items():
            previous = self._tracks.get(tp)
            moved = previous is not None and current.committed != previous.committed
            if (
                moved
                and previous is not None
                and current.committed is not None
                and (previous.committed is None or current.committed > previous.committed)
            ):
                # The commit landed between the previous sample and now.
                self._last_commit_at = now
            if current.lag == 0:
                pending_since = None
            elif previous is None or moved or previous.pending_since is None:
                pending_since = now
            else:
                pending_since = previous.pending_since
            tracks[tp] = _PartitionTrack(committed=current.committed, pending_since=pending_since)
        self._tracks = tracks
        self._lag = sum(o.lag for o in offsets.values())
        self._observed_at = now
        self._error_class = None

    def snapshot(self) -> IngestorProgress:
        now = self._clock()
        observed_at = self._observed_at
        stale = observed_at is None or now - observed_at > self._interval * STALE_AFTER_INTERVALS
        state: IngestorState
        reason: str | None
        if stale:
            state, reason = "unknown", "ingestor_progress_not_observed"
        elif any(
            t.pending_since is not None and now - t.pending_since >= self._stall_seconds
            for t in self._tracks.values()
        ):
            state, reason = "stalled", "consumer_commits_stalled"
        elif self._lag is not None and self._lag > self._lag_threshold:
            state, reason = "lagging", "consumer_lag_above_threshold"
        else:
            state, reason = "ok", None
        return IngestorProgress(
            state=state,
            reason=reason,
            lag_messages=self._lag,
            last_commit_at=_utc(self._last_commit_at),
            observed_at=_utc(observed_at),
            error_class=self._error_class,
        )


def _utc(ts: float | None) -> datetime | None:
    return None if ts is None else datetime.fromtimestamp(ts, tz=UTC)


__all__ = [
    "IngestorProgress",
    "IngestorProgressProbe",
    "KafkaOffsetSource",
    "PartitionOffsets",
    "ingested_topics",
]
