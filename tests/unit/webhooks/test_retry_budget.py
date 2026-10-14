"""Delivery budgets and durable ACK boundaries without network or service boot."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from aiokafka.structs import TopicPartition

from eveys_ocpp.settings import Settings
from eveys_ocpp.webhooks import dispatcher as module
from eveys_ocpp.webhooks.dispatcher import WebhookDispatcher


async def _deliver(dispatcher):
    await dispatcher._post_with_retry(
        url="https://test.invalid/stop",
        body_bytes=b"{}",
        signature="test",
        event_id=str(uuid4()),
        event_type="tx.stopped",
    )


def _dispatcher(monkeypatch, factory, **settings):
    d = WebhookDispatcher(Settings(_env_file=None, **settings), session_factory=factory)
    d._http = SimpleNamespace(post=AsyncMock(return_value=httpx.Response(503)))
    monkeypatch.setattr(module, "insert_webhook_backlog", AsyncMock())
    return d


@pytest.mark.asyncio
@pytest.mark.parametrize("max_attempts", range(1, 21))
async def test_every_allowed_attempt_count_can_reach_committed_backlog(
    monkeypatch, fake_session_factory, fake_session, max_attempts
):
    d = _dispatcher(monkeypatch, fake_session_factory, webhook_max_attempts=max_attempts)
    # Isolate the finite schedule's index safety from the separate real-budget
    # cases below. A large test-only budget and zero elapsed time reach all
    # allowed attempts; production's 55 s budget is not expanded.
    monkeypatch.setattr(module, "_delivery_now", lambda: 0.0, raising=False)
    monkeypatch.setattr(module, "_DELIVERY_HTTP_BUDGET_SECONDS", 1000.0, raising=False)
    monkeypatch.setattr(module.asyncio, "sleep", AsyncMock())
    await _deliver(d)
    assert d._http.post.await_count == max_attempts
    module.insert_webhook_backlog.assert_awaited_once()
    fake_session.commit.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("network_error", [False, True])
async def test_http_and_backoff_budget_reaches_sql_before_max_poll_interval(
    monkeypatch, fake_session_factory, fake_session, network_error
):
    d = _dispatcher(monkeypatch, fake_session_factory, webhook_max_attempts=20)
    elapsed, sleeps = 0.0, []

    async def post(*args, **kwargs):
        nonlocal elapsed
        elapsed += 1.0
        if network_error:
            raise httpx.ReadTimeout("test timeout")
        return httpx.Response(503)

    async def sleep(delay):
        nonlocal elapsed
        sleeps.append(delay)
        elapsed += delay

    d._http.post = AsyncMock(side_effect=post)
    monkeypatch.setattr(module, "_delivery_now", lambda: elapsed, raising=False)
    monkeypatch.setattr(module.asyncio, "sleep", sleep)
    await _deliver(d)
    assert elapsed <= 55.0
    assert sleeps == [1.0, 5.0, 15.0]
    assert d._http.post.await_count == 4
    fake_session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_allowed_120_second_http_profile_is_bounded_before_sql_commit(
    monkeypatch, fake_session_factory, fake_session
):
    d = _dispatcher(
        monkeypatch,
        fake_session_factory,
        webhook_request_timeout_seconds=120,
        webhook_max_attempts=20,
    )
    monkeypatch.setattr(module, "_DELIVERY_HTTP_BUDGET_SECONDS", 0.02, raising=False)
    cancelled = asyncio.Event()

    async def post(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    d._http.post = AsyncMock(side_effect=post)
    await asyncio.wait_for(_deliver(d), timeout=0.2)
    assert cancelled.is_set()
    assert d._http.post.await_count == 1
    fake_session.commit.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["disabled", "commit_failed", "commit_hung"])
async def test_failed_or_unbounded_backlog_cannot_produce_delivery_ack(
    monkeypatch, fake_session_factory, fake_session, failure
):
    d = _dispatcher(
        monkeypatch,
        fake_session_factory,
        webhook_max_attempts=1,
        webhook_backlog_enabled=failure != "disabled",
    )
    monkeypatch.setattr(module, "_BACKLOG_COMMIT_TIMEOUT_SECONDS", 0.02, raising=False)
    if failure == "commit_failed":
        fake_session.commit.side_effect = RuntimeError("test commit failure")
    elif failure == "commit_hung":
        async def commit():
            await asyncio.Event().wait()

        fake_session.commit.side_effect = commit
    with pytest.raises(RuntimeError):
        await asyncio.wait_for(_deliver(d), timeout=0.2)
    if failure == "disabled":
        module.insert_webhook_backlog.assert_not_awaited()
        fake_session.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("ownership_loss", [False, True])
async def test_cancelled_or_revoked_actual_http_cannot_enter_acknowledged_prefix(
    monkeypatch, fake_session_factory, fake_session, ownership_loss
):
    d = _dispatcher(monkeypatch, fake_session_factory, webhook_request_timeout_seconds=120)
    tp = TopicPartition("tx.stopped", 0)
    consumer = SimpleNamespace(assignment=lambda: {tp}, commit=AsyncMock())
    entered = asyncio.Event()

    async def post(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    d._http.post = AsyncMock(side_effect=post)

    async def deliver_and_ack():
        await d._while_owned(_deliver(d), consumer=consumer, epoch=0, partitions={tp})
        await consumer.commit({tp: 1})

    task = asyncio.create_task(deliver_and_ack())
    await entered.wait()
    if ownership_loss:
        d._assignment_changed({tp})
        with pytest.raises(module._OwnershipChanged):
            await task
    else:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    fake_session.commit.assert_not_awaited()
    module.insert_webhook_backlog.assert_not_awaited()
    consumer.commit.assert_not_awaited()
