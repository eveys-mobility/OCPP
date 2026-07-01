"""End-to-end integration tests for the webhook backlog tail.

Skips when Postgres isn't reachable (the ``postgres_session_factory``
fixture handles that). Uses ``respx`` to intercept the drainer's and
dispatcher's outbound httpx calls, so the tests never touch the
network — but Postgres is real, and the queries are the same ones
production runs.

Scenarios:

* **enqueue → drain-2xx → delete** — dispatcher exhausts against a
  503 route, row lands in Postgres. Flip respx to 200; the drainer's
  next ``_drain_once`` deletes the row.
* **enqueue → 4xx → dead** — dispatcher exhausts, drainer POSTs and
  the backend returns 400; row flips to ``dead=true`` and the
  deadletter counter fires.
* **retention aging** — seed a row with ``created_at`` older than the
  retention window; the drainer marks it dead on the very next
  attempt regardless of the response.
* **idempotent enqueue** — the same event_id enqueued twice produces
  one row (UNIQUE constraint).

The dispatcher tests here bypass Kafka — they call ``_post_with_retry``
directly with a stubbed ``_http``. Kafka is exercised by the e2e
tier; the enqueue-on-exhaust logic is what this file cares about.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
import pytest
import respx
import sqlalchemy as sa

from eveys_ocpp.persistence.models import WebhookDeliveryBacklog
from eveys_ocpp.persistence.repositories import (
    insert_webhook_backlog,
)
from eveys_ocpp.settings import Settings
from eveys_ocpp.webhooks.backlog_drainer import WebhookBacklogDrainer
from eveys_ocpp.webhooks.dispatcher import WebhookDispatcher


def _settings(**overrides: Any) -> Settings:
    """Test-focused settings. ``_env_file=None`` so a developer's local
    ``.env`` doesn't leak overrides into the test."""
    base: dict[str, Any] = {
        "webhook_base_url": "https://backend.test/webhooks",
        "webhook_secret": "shared-secret",
        "webhook_backlog_enabled": True,
        "webhook_backlog_retention_hours": 24,
        # Tight timeouts so a hung mock doesn't slow the suite.
        "webhook_request_timeout_seconds": 2.0,
        "webhook_backlog_poll_seconds": 1,
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)


async def _rows(session_factory: Any) -> list[dict[str, Any]]:
    """Read every backlog row via a plain SELECT so the test sees the
    ground truth Postgres actually holds."""
    async with session_factory() as session:
        result = await session.execute(sa.select(WebhookDeliveryBacklog))
        return [
            {
                "id": row.id,
                "event_id": row.event_id,
                "event_type": row.event_type,
                "dead": row.dead,
                "attempts": row.attempts,
                "last_error": row.last_error,
                "created_at": row.created_at,
                "next_attempt_at": row.next_attempt_at,
            }
            for row in result.scalars().all()
        ]


def _resp(status: int) -> MagicMock:
    r = MagicMock(spec=httpx.Response)
    r.status_code = status
    r.text = ""
    return r


# ---- Scenario 1: enqueue → drain-2xx → delete -----------------------------


@pytest.mark.asyncio
@respx.mock
async def test_enqueue_then_drain_success_deletes_row(
    postgres_session_factory: Any,
) -> None:
    """Dispatcher exhausts against 503 -> row lands.
    Flip respx to 200 -> drainer's next pass deletes it."""
    settings = _settings(webhook_max_attempts=1)  # one attempt, then straight to backlog

    # Dispatcher: enqueue path via a synthetic exhausted call.
    dispatcher = WebhookDispatcher(settings, session_factory=postgres_session_factory)
    dispatcher._http = MagicMock()
    dispatcher._http.post = AsyncMock(return_value=_resp(503))

    event_id = str(uuid4())
    with patch("eveys_ocpp.webhooks.dispatcher.asyncio.sleep", AsyncMock()):
        await dispatcher._post_with_retry(
            url="https://backend.test/webhooks/cp-boot",
            body_bytes=b'{"data":{"event_type":"cp.boot"}}',
            signature="sha256=deadbeef",
            event_id=event_id,
            event_type="cp.boot",
        )

    rows = await _rows(postgres_session_factory)
    assert len(rows) == 1
    assert rows[0]["dead"] is False
    assert str(rows[0]["event_id"]) == event_id
    assert rows[0]["attempts"] == 0

    # Nudge next_attempt_at to now-ish so the drainer picks it up
    # without waiting the poll interval.
    async with postgres_session_factory() as session:
        await session.execute(
            sa.update(WebhookDeliveryBacklog).values(next_attempt_at=datetime.now(UTC))
        )
        await session.commit()

    # Drainer: install respx 200 for the URL and drive one poll cycle.
    respx.post("https://backend.test/webhooks/cp-boot").mock(return_value=httpx.Response(200))
    drainer = WebhookBacklogDrainer(settings, session_factory=postgres_session_factory)
    await drainer.start()
    try:
        await drainer._drain_once()
    finally:
        await drainer.stop()

    rows_after = await _rows(postgres_session_factory)
    assert rows_after == [], "row should be deleted after 2xx drain"


# ---- Scenario 2: 4xx from backend → row stays live and reschedules -------


@pytest.mark.asyncio
@respx.mock
async def test_backend_400_reschedules_but_does_not_mark_dead(
    postgres_session_factory: Any,
) -> None:
    """Under the "only 2xx = accepted" contract, 4xx is retryable.
    The drainer bumps ``next_attempt_at`` and increments ``attempts``
    but leaves the row live; retention aging is the only path to
    ``dead=true``."""
    settings = _settings()

    # Seed one live row directly — we're only testing the drainer here,
    # not the dispatcher enqueue path (that's Scenario 1's job).
    event_id = uuid4()
    async with postgres_session_factory() as session:
        await insert_webhook_backlog(
            session,
            event_id=event_id,
            event_type="tx.stopped",
            url="https://backend.test/webhooks/tx-stopped",
            body=b'{"data":{"event_type":"tx.stopped"}}',
            signature="sha256=beef",
            next_attempt_at=datetime.now(UTC),
        )
        await session.commit()

    respx.post("https://backend.test/webhooks/tx-stopped").mock(
        return_value=httpx.Response(400, text="bad json"),
    )
    drainer = WebhookBacklogDrainer(settings, session_factory=postgres_session_factory)
    await drainer.start()
    try:
        await drainer._drain_once()
    finally:
        await drainer.stop()

    rows = await _rows(postgres_session_factory)
    assert len(rows) == 1
    assert rows[0]["dead"] is False, "4xx must not immediately mark dead"
    assert rows[0]["attempts"] == 1
    assert rows[0]["last_error"] == "http_400"
    # next_attempt_at moved into the future — the row will be picked up
    # again on a subsequent drain cycle.
    assert rows[0]["next_attempt_at"] > datetime.now(UTC)


# ---- Scenario 3: retention aging → row marked dead ------------------------


@pytest.mark.asyncio
@respx.mock
async def test_row_older_than_retention_gets_marked_dead(
    postgres_session_factory: Any,
) -> None:
    """A row whose ``created_at`` is past the retention window is
    marked dead even on a retryable failure (503) — the drainer's
    retention check fires before the backoff scheduler."""
    settings = _settings(webhook_backlog_retention_hours=1)

    # Insert directly with a stale created_at so the drainer's
    # ``now - created_at >= retention`` branch triggers.
    event_id = uuid4()
    old = datetime.now(UTC) - timedelta(hours=25)
    async with postgres_session_factory() as session:
        await session.execute(
            sa.insert(WebhookDeliveryBacklog).values(
                event_id=event_id,
                event_type="cp.status_changed",
                url="https://backend.test/webhooks/cp-status-changed",
                body=b"{}",
                signature="sha256=old",
                created_at=old,
                next_attempt_at=old,
                attempts=0,
                dead=False,
            )
        )
        await session.commit()

    respx.post("https://backend.test/webhooks/cp-status-changed").mock(
        return_value=httpx.Response(503),
    )
    drainer = WebhookBacklogDrainer(settings, session_factory=postgres_session_factory)
    await drainer.start()
    try:
        await drainer._drain_once()
    finally:
        await drainer.stop()

    rows = await _rows(postgres_session_factory)
    assert len(rows) == 1
    assert rows[0]["dead"] is True
    assert "retention_hit" in rows[0]["last_error"]


# ---- Scenario 4: double-enqueue is a no-op --------------------------------


@pytest.mark.asyncio
async def test_double_enqueue_is_idempotent(postgres_session_factory: Any) -> None:
    """A Kafka replay of the same envelope after the offset was already
    committed exercises the ``ON CONFLICT (event_id) DO NOTHING`` guard.
    Two enqueue calls -> one row."""
    settings = _settings()

    dispatcher = WebhookDispatcher(settings, session_factory=postgres_session_factory)
    event_id = str(uuid4())
    for _ in range(2):
        await dispatcher._enqueue_backlog(
            url="https://backend.test/webhooks/cp-boot",
            body_bytes=b"{}",
            signature="sha256=x",
            event_id=event_id,
            event_type="cp.boot",
        )

    rows = await _rows(postgres_session_factory)
    assert len(rows) == 1
    assert str(rows[0]["event_id"]) == event_id
