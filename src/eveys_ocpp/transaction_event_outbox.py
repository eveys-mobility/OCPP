"""Bounded, ACK-only publication of immutable physical Stop envelopes.

The row lock spans the bounded broker operation. ACK precedes the PG mark;
lost mark/commit repeats the same bytes and UUID, requiring downstream dedup.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import (
    defer_transaction_event,
    lock_ready_transaction_event,
    mark_transaction_event_published,
    transaction_outbox_snapshot,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.events import EventProducer
    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


class TransactionEventOutboxPublisher:
    def __init__(
        self,
        settings: Settings,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        producer: EventProducer,
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._producer = producer
        self._shutdown = asyncio.Event()

    async def stop(self) -> None:
        self._shutdown.set()

    async def publish_ready(self) -> int:
        """Attempt a bounded ready slice; never treat a drop producer as an ACK."""
        if self._producer.broker_acknowledged is not True:
            log.warning("transaction_outbox.ack_unavailable")
            await self._observe()
            return 0
        published = 0
        async with asyncio.timeout(self._settings.transaction_outbox_cycle_timeout_seconds):
            for _ in range(self._settings.transaction_outbox_batch_size):
                if self._shutdown.is_set():
                    break
                acknowledged = False
                async with session_scope(self._session_factory) as session:
                    row = await lock_ready_transaction_event(session, now=datetime.now(UTC))
                    if row is None:
                        break
                    try:
                        async with asyncio.timeout(
                            self._settings.transaction_outbox_publish_timeout_seconds
                        ):
                            await self._producer.publish(
                                topic=row.topic, key=row.cp_id, value=row.body
                            )
                    except Exception as exc:
                        metrics_registry.TRANSACTION_OUTBOX_ERRORS_TOTAL.labels(
                            stage="publish"
                        ).inc()
                        await defer_transaction_event(
                            session,
                            event_id=row.event_id,
                            next_attempt_at=datetime.now(UTC)
                            + timedelta(seconds=min(300, 2 ** min(row.attempts, 8))),
                            error_class=type(exc).__name__,
                        )
                        log.warning(
                            "transaction_outbox.publish_failed", error_class=type(exc).__name__
                        )
                    else:
                        await mark_transaction_event_published(
                            session, event_id=row.event_id, published_at=datetime.now(UTC)
                        )
                        acknowledged = True
                # A completed broker call alone is insufficient: count only
                # after the published mark committed. Cancellation rolls back.
                if acknowledged:
                    published += 1
                    metrics_registry.TRANSACTION_OUTBOX_PUBLISHED_TOTAL.inc()
        await self._observe()
        return published

    async def _observe(self) -> None:
        try:
            async with asyncio.timeout(self._settings.transaction_outbox_cycle_timeout_seconds):
                async with session_scope(self._session_factory) as session:
                    count, oldest = await transaction_outbox_snapshot(session)
            now = datetime.now(UTC)
            if oldest is not None and oldest.tzinfo is None:
                oldest = oldest.replace(tzinfo=UTC)
            metrics_registry.TRANSACTION_OUTBOX_PENDING.set(count)
            metrics_registry.TRANSACTION_OUTBOX_OLDEST_AGE_SECONDS.set(
                max(0, (now - oldest).total_seconds()) if oldest else 0
            )
            metrics_registry.TRANSACTION_OUTBOX_SAMPLE_TIMESTAMP_SECONDS.set(now.timestamp())
        except Exception as exc:
            metrics_registry.TRANSACTION_OUTBOX_ERRORS_TOTAL.labels(stage="observe").inc()
            log.warning("transaction_outbox.observe_failed", error_class=type(exc).__name__)

    async def serve_forever(self) -> None:
        while not self._shutdown.is_set():
            try:
                await self.publish_ready()
            except Exception as exc:
                metrics_registry.TRANSACTION_OUTBOX_ERRORS_TOTAL.labels(stage="cycle").inc()
                log.warning("transaction_outbox.cycle_failed", error_class=type(exc).__name__)
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    self._shutdown.wait(), timeout=self._settings.transaction_outbox_poll_seconds
                )
