"""Database engine and session factory.

The engine is process-wide; sessions are per-request (per-handler-call).
Handlers acquire a session via `async with session_factory() as session:`.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from eveys_ocpp.metrics import registry as metrics_registry


def _classify_op(statement: str) -> str:
    """Bucket the SQL statement into one of the four labels the
    DB_QUERY_LATENCY_SECONDS histogram carries.

    Bounded — we never label by raw SQL (cardinality), just the verb.
    Anything we can't recognise (DDL, vendor-specific) gets `other`.
    """
    head = statement.lstrip().split(None, 1)[0].lower() if statement else ""
    if head == "select":
        return "select"
    if head == "insert":
        return "insert"
    if head == "update":
        return "update"
    if head == "delete":
        return "delete"
    return "other"


def _attach_query_timer(sync_engine: Any) -> None:
    """Wire SQLAlchemy `before_cursor_execute` / `after_cursor_execute`
    so every statement observes one latency sample.

    Events fire on the synchronous core engine (the async engine wraps
    it). Time is stored on the cursor's `info` mapping — SA's
    documented per-statement bag — to survive pool checkout/checkin.
    """

    @event.listens_for(sync_engine, "before_cursor_execute")
    def _before(
        _conn: Any,
        cursor: Any,
        _statement: str,
        _params: Any,
        _context: Any,
        _executemany: bool,
    ) -> None:
        cursor.info["_eveys_query_started"] = time.perf_counter()

    @event.listens_for(sync_engine, "after_cursor_execute")
    def _after(
        _conn: Any,
        cursor: Any,
        statement: str,
        _params: Any,
        _context: Any,
        _executemany: bool,
    ) -> None:
        started = cursor.info.pop("_eveys_query_started", None)
        if started is None:
            return  # mismatched pair (executemany re-entry); skip rather than mislabel
        metrics_registry.DB_QUERY_LATENCY_SECONDS.labels(op=_classify_op(statement)).observe(
            time.perf_counter() - started
        )


def _attach_pool_gauges(sync_engine: Any) -> None:
    """Update DB_POOL_IN_USE / DB_POOL_OVERFLOW on every checkout /
    checkin. Sampled rather than polled — cheap and correct."""

    pool = sync_engine.pool

    def _refresh_gauges() -> None:
        # SQLAlchemy's QueuePool exposes `checkedout()` (in use) and
        # `overflow()` (slots above pool_size currently allocated).
        # Both are O(1) integer reads guarded by the pool's lock.
        try:
            in_use = pool.checkedout()
            overflow = pool.overflow()
        except Exception:
            return
        metrics_registry.DB_POOL_IN_USE.set(float(in_use))
        # `overflow()` returns negative when slots are unused; clamp
        # to 0 so the gauge stays sane in dashboards.
        metrics_registry.DB_POOL_OVERFLOW.set(float(max(0, overflow)))

    @event.listens_for(pool, "checkout")
    def _on_checkout(*_args: Any) -> None:
        _refresh_gauges()

    @event.listens_for(pool, "checkin")
    def _on_checkin(*_args: Any) -> None:
        _refresh_gauges()


def make_engine(db_url: str, *, pool_size: int = 10, max_overflow: int = 20) -> AsyncEngine:
    """Create the process-wide async engine."""
    engine = create_async_engine(
        db_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        future=True,
    )
    # Phase 4 / E4-1: attach metrics to the sync core engine. Async
    # engines wrap a sync engine; `engine.sync_engine` is the public
    # attribute SA exposes. Listeners fire on every cursor execute,
    # regardless of which session opened them.
    _attach_query_timer(engine.sync_engine)
    _attach_pool_gauges(engine.sync_engine)
    return engine


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def session_scope(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """Yield a session, commit on clean exit, rollback on exception."""
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
