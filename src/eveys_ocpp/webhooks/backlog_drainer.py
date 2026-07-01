"""Durable webhook delivery tail (E3-9 backlog).

The in-loop dispatcher (`webhooks/dispatcher.py`) exhausts its five
attempts (~12.6 min) and, if the backend still hasn't accepted the
envelope, inserts a row into `webhook_delivery_backlog`. Without a
tail, those rows would sit forever; this drainer is that tail.

The drainer is a long-lived asyncio task, sibling of the dispatcher
in the same `TaskGroup`. It polls the backlog table on a fixed
cadence (`webhook_backlog_poll_seconds`, default 30 s), POSTs
eligible rows through a bounded semaphore (`webhook_backlog_max_
concurrency`, default 8), classifies the response with the same
rules the dispatcher uses, and updates the row:

* 2xx    -> delete the row (`drained`)
* anything else (3xx, every 4xx including 429, 5xx, network,
  timeout, TLS error) -> bump `next_attempt_at` with backoff.
  If the row's `created_at` is older than the retention window at
  that point, mark dead instead. Retention aging is the ONLY path
  to `dead=true` — under the "only 2xx = accepted" contract even a
  400 keeps retrying until retention hits.

Backoff for backlog attempts is coarser than the dispatcher's
in-loop schedule — this is the tail, not the hot path. The schedule
is `5m, 15m, 30m, 1h, 2h, 4h, 6h`, then repeats at 6 h. Jitter
would help avoid herds when many rows were enqueued at the same
instant (backend outage) but at v1 we keep the schedule deterministic
for easier operator inspection.

Design non-goals:
* No admin UI. Operators replay dead rows via psql
  (`UPDATE webhook_delivery_backlog SET dead=false,
    next_attempt_at=now() WHERE dead=true AND event_type=...`).
* No priority queue. FIFO by `next_attempt_at` is correct here —
  the dispatcher already lost per-cp Kafka partition order when
  the in-loop retries stalled.
* No circuit breaker. Backoff already throttles a dead backend.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx

from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import get_logger
from eveys_ocpp.persistence.repositories import (
    bump_webhook_backlog_attempt,
    delete_webhook_backlog,
    fetch_ready_webhook_backlog,
    get_webhook_backlog_gauges,
    mark_webhook_backlog_dead,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.settings import Settings

log = get_logger(__name__)

# Backoff schedule for backlog attempts. Indexed by the row's
# `attempts` counter BEFORE the current try. Once we go past the end
# of the tuple, stay at the last value (6 h) until retention kicks in
# and marks the row dead.
_BACKLOG_BACKOFF_SECONDS: tuple[float, ...] = (
    300.0,  # 5 min
    900.0,  # 15 min
    1_800.0,  # 30 min
    3_600.0,  # 1 h
    7_200.0,  # 2 h
    14_400.0,  # 4 h
    21_600.0,  # 6 h
)


def _next_backoff(attempts: int) -> float:
    """Look up the sleep before attempt (attempts + 1). Clamps beyond
    the last step so a row that keeps failing keeps retrying at the
    coarsest cadence until retention kicks in."""
    if attempts < 0:
        attempts = 0
    if attempts >= len(_BACKLOG_BACKOFF_SECONDS):
        return _BACKLOG_BACKOFF_SECONDS[-1]
    return _BACKLOG_BACKOFF_SECONDS[attempts]


class WebhookBacklogDrainer:
    """Long-lived asyncio task that drains `webhook_delivery_backlog`.

    Lifecycle mirrors `WebhookDispatcher`:
      * `start()`   — open the shared httpx client
      * `serve_forever()` — poll loop, exits on `stop()`
      * `stop()`    — cancel the loop and close the client

    One instance per process. Multiple gateway pods can run in
    parallel — each pod's drainer picks up whatever rows it sees;
    `bump_webhook_backlog_attempt` uses an atomic
    `attempts = attempts + 1` update so the counter can't be
    clobbered by concurrent pods. The offset commit + row insert on
    the dispatcher side already handle enqueue idempotency via the
    UNIQUE `event_id` constraint.
    """

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._http: httpx.AsyncClient | None = None
        self._shutdown = asyncio.Event()
        self._retention: timedelta = timedelta(hours=settings.webhook_backlog_retention_hours)

    async def start(self) -> None:
        """Open the HTTP client. No Kafka involved on this leg — the
        drainer is Postgres-only."""
        if not self._settings.outbound_tls_verify:
            # Same rationale as the dispatcher: an operator scanning
            # for tls_verify_disabled finds every outbound site
            # without having to know they share config.
            log.warning(
                "webhook_backlog.tls_verify_disabled",
                detail=(
                    "EVEYS_OCPP_OUTBOUND_TLS_VERIFY=False — accepting "
                    "any TLS cert on the backlog drain leg. Acceptable "
                    "for local dev; never in production."
                ),
            )
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(self._settings.webhook_request_timeout_seconds),
            verify=self._settings.outbound_tls_verify,
        )
        log.info(
            "webhook_backlog.started",
            poll_seconds=self._settings.webhook_backlog_poll_seconds,
            batch_size=self._settings.webhook_backlog_batch_size,
            max_concurrency=self._settings.webhook_backlog_max_concurrency,
            retention_hours=self._settings.webhook_backlog_retention_hours,
        )

    async def stop(self) -> None:
        self._shutdown.set()
        if self._http is not None:
            await self._http.aclose()
            self._http = None
        log.info("webhook_backlog.stopped")

    async def serve_forever(self) -> None:
        """Poll loop. Exits on `stop()`; single-row failures don't
        tear the loop down."""
        if self._http is None:
            # `start()` wasn't called — nothing to do, stay parked so
            # the TaskGroup can await the shutdown event.
            await self._shutdown.wait()
            return

        try:
            while not self._shutdown.is_set():
                cycled = await self._drain_once()
                if cycled == 0:
                    # Nothing was ready — sleep so we don't hammer
                    # Postgres. The shutdown event short-circuits the
                    # sleep so stop() doesn't have to wait a full
                    # poll interval.
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(
                            self._shutdown.wait(),
                            timeout=self._settings.webhook_backlog_poll_seconds,
                        )
        except asyncio.CancelledError:
            log.info("webhook_backlog.cancelled")
            raise

    async def _drain_once(self) -> int:
        """One poll cycle. Returns the number of rows attempted so
        the caller knows whether to sleep or immediately loop again."""
        now = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                rows = await fetch_ready_webhook_backlog(
                    session,
                    now=now,
                    limit=self._settings.webhook_backlog_batch_size,
                )
                await self._sample_gauges(session, now)
        except Exception as exc:
            log.exception(
                "webhook_backlog.fetch_failed",
                error=str(exc),
            )
            return 0

        if not rows:
            return 0

        semaphore = asyncio.Semaphore(self._settings.webhook_backlog_max_concurrency)

        async def _drain(row: dict[str, Any]) -> None:
            async with semaphore:
                await self._drain_one(row, now)

        # gather with return_exceptions so one row's crash doesn't
        # cascade into the whole batch. Any exception raised here is
        # already logged inside _drain_one — we only need to keep
        # the loop alive.
        await asyncio.gather(*(_drain(row) for row in rows), return_exceptions=True)
        return len(rows)

    async def _drain_one(self, row: dict[str, Any], enqueue_now: datetime) -> None:
        """POST one row, classify, update. Response classification
        matches the dispatcher's exactly so operators can reason
        about outcomes the same way in both places.
        """
        assert self._http is not None  # start() was called before serve_forever()
        headers = {
            "Content-Type": "application/json",
            "X-Eveys-Signature": row["signature"],
            "X-Eveys-Event-Id": str(row["event_id"]),
            "X-Eveys-Event-Type": row["event_type"],
            "X-Eveys-Delivered-At": datetime.now(UTC).isoformat(),
            # Continue the attempt counter across dispatcher +
            # backlog so the backend sees a monotonic number.
            # Dispatcher makes at most `webhook_max_attempts` tries.
            "X-Eveys-Attempt": str(self._settings.webhook_max_attempts + row["attempts"] + 1),
        }
        started = time.perf_counter()
        try:
            response = await self._http.post(row["url"], content=row["body"], headers=headers)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            metrics_registry.WEBHOOK_DELIVERY_LATENCY_SECONDS.labels(
                event_type=row["event_type"]
            ).observe(time.perf_counter() - started)
            await self._on_retryable_failure(
                row=row,
                enqueue_now=enqueue_now,
                error=str(exc),
                log_extra={"kind": "network"},
            )
            return

        metrics_registry.WEBHOOK_DELIVERY_LATENCY_SECONDS.labels(
            event_type=row["event_type"]
        ).observe(time.perf_counter() - started)

        if 200 <= response.status_code < 300:
            await self._on_success(row)
            return

        # Non-2xx: retryable. The drainer treats every non-2xx code
        # the same as a 5xx / network error — reschedule with
        # backoff. Rows only flip to `dead=true` via retention
        # aging (see `_on_retryable_failure`); a bad-json 400 will
        # keep retrying until retention hits and the operator either
        # replays or purges it via the admin UI.
        await self._on_retryable_failure(
            row=row,
            enqueue_now=enqueue_now,
            error=f"http_{response.status_code}",
            log_extra={"status": response.status_code},
        )

    async def _on_success(self, row: dict[str, Any]) -> None:
        try:
            async with self._session_factory() as session:
                await delete_webhook_backlog(session, backlog_id=row["id"])
                await session.commit()
        except Exception as exc:
            log.exception(
                "webhook_backlog.delete_failed",
                backlog_id=str(row["id"]),
                event_id=str(row["event_id"]),
                error=str(exc),
            )
            return
        metrics_registry.WEBHOOK_BACKLOG_DRAIN_TOTAL.labels(outcome="drained").inc()
        log.info(
            "webhook_backlog.drained",
            backlog_id=str(row["id"]),
            event_id=str(row["event_id"]),
            event_type=row["event_type"],
            attempts=row["attempts"] + 1,
        )

    async def _on_permanent_reject(
        self,
        *,
        row: dict[str, Any],
        status: int,
        body_preview: str,
    ) -> None:
        try:
            async with self._session_factory() as session:
                await mark_webhook_backlog_dead(
                    session,
                    backlog_id=row["id"],
                    reason=f"http_{status}: {body_preview}"[:1000],
                )
                await session.commit()
        except Exception as exc:
            log.exception(
                "webhook_backlog.mark_dead_failed",
                backlog_id=str(row["id"]),
                event_id=str(row["event_id"]),
                error=str(exc),
            )
            return
        metrics_registry.WEBHOOK_BACKLOG_DRAIN_TOTAL.labels(outcome="dead").inc()
        metrics_registry.WEBHOOK_BACKLOG_DEADLETTER_TOTAL.labels(event_type=row["event_type"]).inc()
        log.error(
            "webhook_backlog.rejected",
            backlog_id=str(row["id"]),
            event_id=str(row["event_id"]),
            event_type=row["event_type"],
            status=status,
        )

    async def _on_retryable_failure(
        self,
        *,
        row: dict[str, Any],
        enqueue_now: datetime,
        error: str,
        log_extra: dict[str, Any],
    ) -> None:
        # Retention check: if the row was first created more than
        # `retention_hours` ago, don't reschedule — mark dead. The
        # `now` we compare against is the same `enqueue_now` used for
        # this drain cycle so multiple rows aged in the same batch
        # produce the same dead-cutoff behaviour.
        created_at = row["created_at"]
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=UTC)
        if enqueue_now - created_at >= self._retention:
            await self._on_permanent_reject(
                row=row,
                status=0,  # 0 = retention aging, not an HTTP status
                body_preview=f"retention_hit:last_error={error}",
            )
            return

        next_attempt = enqueue_now + timedelta(seconds=_next_backoff(row["attempts"] + 1))
        try:
            async with self._session_factory() as session:
                await bump_webhook_backlog_attempt(
                    session,
                    backlog_id=row["id"],
                    next_attempt_at=next_attempt,
                    last_error=error[:1000],
                )
                await session.commit()
        except Exception as exc:
            log.exception(
                "webhook_backlog.bump_failed",
                backlog_id=str(row["id"]),
                event_id=str(row["event_id"]),
                error=str(exc),
            )
            return
        metrics_registry.WEBHOOK_BACKLOG_DRAIN_TOTAL.labels(outcome="retried").inc()
        log.warning(
            "webhook_backlog.retry_scheduled",
            backlog_id=str(row["id"]),
            event_id=str(row["event_id"]),
            event_type=row["event_type"],
            attempts=row["attempts"] + 1,
            next_attempt_at=next_attempt.isoformat(),
            error=error,
            **log_extra,
        )

    async def _sample_gauges(self, session: AsyncSession, now: datetime) -> None:
        """Refresh the size + oldest-age gauges from the same session
        we just used to fetch the ready batch. One extra query per
        poll cycle, negligible cost."""
        try:
            size, age = await get_webhook_backlog_gauges(session, now=now)
        except Exception as exc:
            log.debug("webhook_backlog.gauge_sample_failed", error=str(exc))
            return
        metrics_registry.WEBHOOK_BACKLOG_SIZE.set(size)
        metrics_registry.WEBHOOK_BACKLOG_OLDEST_AGE_SECONDS.set(age)
