"""Tests for the webhook backlog drainer.

The drainer's poll loop and Postgres calls are covered indirectly
through `_drain_once` — the tests wire fake row-lists directly rather
than standing up a database, and patch the repository helpers so
each test asserts on the exact classifier branch it's exercising:

* 2xx      -> ``delete_webhook_backlog`` called
* 4xx≠429  -> ``mark_webhook_backlog_dead`` called + deadletter metric
* 5xx/429/network -> ``bump_webhook_backlog_attempt`` with the right
                     backoff on ``next_attempt_at``
* retention window hit -> ``mark_webhook_backlog_dead`` instead of bump
* the concurrency semaphore holds under N > max_concurrency rows

The response-classification logic mirrors the dispatcher's, so the
expectations mirror ``test_dispatcher.py`` too — same 429-is-retryable
carve-out, same permanent-reject metric labels.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import httpx
import pytest

from eveys_ocpp.settings import Settings
from eveys_ocpp.webhooks.backlog_drainer import (
    _BACKLOG_BACKOFF_SECONDS,
    WebhookBacklogDrainer,
    _next_backoff,
)


def _settings(**overrides: Any) -> Settings:
    base = {
        "webhook_base_url": "https://backend.example/webhooks",
        "webhook_secret": "shared-secret",
        # Tight defaults so tests don't accidentally sleep the poll
        # interval when the loop returns empty.
        "webhook_backlog_poll_seconds": 1,
        "webhook_backlog_batch_size": 50,
        "webhook_backlog_max_concurrency": 4,
        "webhook_backlog_retention_hours": 24,
    }
    base.update(overrides)
    # `_env_file=None` — same rationale as in test_dispatcher.py.
    return Settings(_env_file=None, **base)


def _factory_yielding(session: MagicMock) -> Any:
    """Build a session_factory whose async context yields ``session``."""

    class _Ctx:
        async def __aenter__(self) -> MagicMock:
            return session

        async def __aexit__(self, *exc: object) -> None:
            return None

    class _Factory:
        def __call__(self) -> _Ctx:
            return _Ctx()

    return _Factory()


def _row(
    *,
    event_type: str = "cp.boot",
    attempts: int = 0,
    created_at: datetime | None = None,
    next_attempt_at: datetime | None = None,
    event_id: UUID | None = None,
    url: str = "https://backend.example/webhooks/cp-boot",
) -> dict[str, Any]:
    """A single backlog row shaped the way the drainer expects (the
    same tuple ``_backlog_row_to_dict`` returns from repositories).

    `created_at` / `next_attempt_at` default to `datetime.now(UTC)` so
    a fresh row survives the drainer's retention-window check without
    each test having to remember to pass a live timestamp. Tests that
    need an aged row (retention aging path) override `created_at`
    explicitly."""
    now = datetime.now(UTC)
    return {
        "id": uuid4(),
        "event_id": event_id or uuid4(),
        "event_type": event_type,
        "url": url,
        "body": b'{"data":{}}',
        "signature": "sha256=deadbeef",
        "created_at": created_at or now,
        "next_attempt_at": next_attempt_at or now,
        "attempts": attempts,
    }


def _make_drainer(**setting_overrides: Any) -> tuple[WebhookBacklogDrainer, MagicMock]:
    """Return a drainer with an httpx stub and a session_factory backed
    by a single MagicMock session (assertable via ``session.commit``)."""
    session = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    drainer = WebhookBacklogDrainer(
        _settings(**setting_overrides), session_factory=_factory_yielding(session)
    )
    drainer._http = MagicMock()
    return drainer, session


def _resp(status: int, body: str = "") -> MagicMock:
    r = MagicMock(spec=httpx.Response)
    r.status_code = status
    r.text = body
    return r


# ---- _next_backoff --------------------------------------------------------


def test_next_backoff_walks_the_schedule() -> None:
    """First failed backlog attempt -> 5 min; second -> 15 min; …"""
    assert _next_backoff(0) == _BACKLOG_BACKOFF_SECONDS[0]
    assert _next_backoff(1) == _BACKLOG_BACKOFF_SECONDS[1]
    assert _next_backoff(len(_BACKLOG_BACKOFF_SECONDS) - 1) == _BACKLOG_BACKOFF_SECONDS[-1]


def test_next_backoff_clamps_past_the_schedule() -> None:
    """Rows that keep failing past the last step stay at 6 h."""
    assert _next_backoff(len(_BACKLOG_BACKOFF_SECONDS)) == _BACKLOG_BACKOFF_SECONDS[-1]
    assert _next_backoff(100) == _BACKLOG_BACKOFF_SECONDS[-1]


def test_next_backoff_clamps_negative() -> None:
    """Belt-and-braces: attempts should never go negative, but the
    helper still returns the first-step delay if it somehow does."""
    assert _next_backoff(-1) == _BACKLOG_BACKOFF_SECONDS[0]


# ---- _drain_once: empty backlog -------------------------------------------


@pytest.mark.asyncio
async def test_drain_once_returns_zero_when_no_rows() -> None:
    drainer, _session = _make_drainer()
    with (
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.fetch_ready_webhook_backlog",
            AsyncMock(return_value=[]),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.get_webhook_backlog_gauges",
            AsyncMock(return_value=(0, 0.0)),
        ),
    ):
        n = await drainer._drain_once()
    assert n == 0
    drainer._http.post.assert_not_called()  # type: ignore[union-attr]


# ---- 2xx: delete row + metric --------------------------------------------


@pytest.mark.asyncio
async def test_drain_deletes_row_on_2xx() -> None:
    drainer, session = _make_drainer()
    row = _row()
    drainer._http.post = AsyncMock(return_value=_resp(200))  # type: ignore[union-attr]
    with (
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.fetch_ready_webhook_backlog",
            AsyncMock(return_value=[row]),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.get_webhook_backlog_gauges",
            AsyncMock(return_value=(1, 0.0)),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.delete_webhook_backlog", AsyncMock()
        ) as mock_delete,
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.mark_webhook_backlog_dead",
            AsyncMock(),
        ) as mock_dead,
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.bump_webhook_backlog_attempt",
            AsyncMock(),
        ) as mock_bump,
    ):
        n = await drainer._drain_once()

    assert n == 1
    mock_delete.assert_awaited_once()
    assert mock_delete.await_args.kwargs["backlog_id"] == row["id"]
    mock_dead.assert_not_awaited()
    mock_bump.assert_not_awaited()
    # commit called for the fetch/gauges session AND the delete session
    assert session.commit.await_count >= 1


# ---- 4xx now treated as retryable ---------------------------------------


@pytest.mark.asyncio
async def test_drain_bumps_on_4xx() -> None:
    """Under the "only 2xx = accepted" contract, 4xx is retryable —
    the drainer reschedules with backoff rather than marking dead.
    Retention aging is now the only path to `dead=true`."""
    drainer, _session = _make_drainer()
    row = _row(event_type="tx.stopped", created_at=datetime.now(UTC))
    drainer._http.post = AsyncMock(return_value=_resp(400, "bad json"))  # type: ignore[union-attr]
    with (
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.fetch_ready_webhook_backlog",
            AsyncMock(return_value=[row]),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.get_webhook_backlog_gauges",
            AsyncMock(return_value=(1, 0.0)),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.mark_webhook_backlog_dead", AsyncMock()
        ) as mock_dead,
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.delete_webhook_backlog", AsyncMock()
        ) as mock_delete,
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.bump_webhook_backlog_attempt",
            AsyncMock(),
        ) as mock_bump,
    ):
        await drainer._drain_once()

    mock_bump.assert_awaited_once()
    assert mock_bump.await_args.kwargs["last_error"] == "http_400"
    mock_dead.assert_not_awaited()
    mock_delete.assert_not_awaited()


# ---- 429 / 5xx / network: retry with backoff -----------------------------


@pytest.mark.asyncio
async def test_drain_bumps_next_attempt_on_5xx() -> None:
    drainer, _session = _make_drainer()
    # created_at anchored just before the drain call so the retention
    # window doesn't kick in and drop us into the mark-dead branch.
    row = _row(attempts=0, created_at=datetime.now(UTC))
    drainer._http.post = AsyncMock(return_value=_resp(503))  # type: ignore[union-attr]
    before = datetime.now(UTC)
    with (
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.fetch_ready_webhook_backlog",
            AsyncMock(return_value=[row]),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.get_webhook_backlog_gauges",
            AsyncMock(return_value=(1, 0.0)),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.bump_webhook_backlog_attempt",
            AsyncMock(),
        ) as mock_bump,
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.mark_webhook_backlog_dead", AsyncMock()
        ) as mock_dead,
    ):
        await drainer._drain_once()
    after = datetime.now(UTC)

    mock_bump.assert_awaited_once()
    kwargs = mock_bump.await_args.kwargs
    # attempts=0 in the row -> next attempt uses backoff[1] = 15 min
    # (drainer looks up _next_backoff(row["attempts"] + 1)). The
    # anchor is the drainer's own wall clock inside _drain_once,
    # which sits between our `before` and `after` samples.
    step = timedelta(seconds=_BACKLOG_BACKOFF_SECONDS[1])
    assert before + step <= kwargs["next_attempt_at"] <= after + step
    assert kwargs["last_error"] == "http_503"
    mock_dead.assert_not_awaited()


@pytest.mark.asyncio
async def test_drain_treats_429_as_retryable() -> None:
    drainer, _session = _make_drainer()
    row = _row()
    drainer._http.post = AsyncMock(return_value=_resp(429))  # type: ignore[union-attr]
    with (
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.fetch_ready_webhook_backlog",
            AsyncMock(return_value=[row]),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.get_webhook_backlog_gauges",
            AsyncMock(return_value=(1, 0.0)),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.bump_webhook_backlog_attempt",
            AsyncMock(),
        ) as mock_bump,
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.mark_webhook_backlog_dead", AsyncMock()
        ) as mock_dead,
    ):
        await drainer._drain_once()

    mock_bump.assert_awaited_once()
    mock_dead.assert_not_awaited()


@pytest.mark.asyncio
async def test_drain_bumps_on_network_error() -> None:
    drainer, _session = _make_drainer()
    row = _row()
    drainer._http.post = AsyncMock(side_effect=httpx.ConnectError("refused"))  # type: ignore[union-attr]
    with (
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.fetch_ready_webhook_backlog",
            AsyncMock(return_value=[row]),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.get_webhook_backlog_gauges",
            AsyncMock(return_value=(1, 0.0)),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.bump_webhook_backlog_attempt",
            AsyncMock(),
        ) as mock_bump,
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.mark_webhook_backlog_dead", AsyncMock()
        ) as mock_dead,
    ):
        await drainer._drain_once()

    mock_bump.assert_awaited_once()
    assert "refused" in mock_bump.await_args.kwargs["last_error"]
    mock_dead.assert_not_awaited()


# ---- retention aging -----------------------------------------------------


@pytest.mark.asyncio
async def test_drain_marks_dead_when_row_ages_past_retention() -> None:
    """Row older than the retention window at drain time: mark dead
    even though the failure kind (503) would otherwise be retryable."""
    drainer, _session = _make_drainer(webhook_backlog_retention_hours=1)
    # Row was enqueued 2 h ago; retention is 1 h -> aged out.
    old_created = datetime.now(UTC) - timedelta(hours=2)
    row = _row(created_at=old_created, next_attempt_at=old_created)
    drainer._http.post = AsyncMock(return_value=_resp(503))  # type: ignore[union-attr]
    with (
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.fetch_ready_webhook_backlog",
            AsyncMock(return_value=[row]),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.get_webhook_backlog_gauges",
            AsyncMock(return_value=(1, 7200.0)),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.mark_webhook_backlog_dead", AsyncMock()
        ) as mock_dead,
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.bump_webhook_backlog_attempt",
            AsyncMock(),
        ) as mock_bump,
    ):
        await drainer._drain_once()

    mock_dead.assert_awaited_once()
    assert "retention_hit" in mock_dead.await_args.kwargs["reason"]
    mock_bump.assert_not_awaited()


# ---- concurrency ---------------------------------------------------------


@pytest.mark.asyncio
async def test_drain_bounded_by_max_concurrency() -> None:
    """Fifteen rows, semaphore of 3: at most 3 POSTs are ever in flight
    simultaneously. Asserts the semaphore is actually applied and not
    an unbounded gather."""
    drainer, _session = _make_drainer(webhook_backlog_max_concurrency=3)
    rows = [_row(event_type="cp.boot") for _ in range(15)]

    in_flight = 0
    max_in_flight = 0

    async def _slow_post(*_args: object, **_kwargs: object) -> MagicMock:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        # Yield so all coroutines get a chance to enter the semaphore
        # before the first one exits.
        import asyncio as _a

        await _a.sleep(0)
        in_flight -= 1
        return _resp(200)

    drainer._http.post = AsyncMock(side_effect=_slow_post)  # type: ignore[union-attr]
    with (
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.fetch_ready_webhook_backlog",
            AsyncMock(return_value=rows),
        ),
        patch(
            "eveys_ocpp.webhooks.backlog_drainer.get_webhook_backlog_gauges",
            AsyncMock(return_value=(15, 0.0)),
        ),
        patch("eveys_ocpp.webhooks.backlog_drainer.delete_webhook_backlog", AsyncMock()),
    ):
        await drainer._drain_once()

    assert max_in_flight <= 3


# ---- lifecycle ------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_opens_http_client_and_stop_closes_it() -> None:
    """start() gives us a real httpx client; stop() closes it and
    sets the shutdown event so serve_forever() would exit."""
    drainer, _session = _make_drainer()
    await drainer.start()
    assert isinstance(drainer._http, httpx.AsyncClient)
    await drainer.stop()
    assert drainer._http is None
    assert drainer._shutdown.is_set()


@pytest.mark.asyncio
async def test_serve_forever_exits_when_stop_called_without_start() -> None:
    """A drainer whose ``start()`` was never called should park on the
    shutdown event rather than blowing up — same convention the
    dispatcher follows when there are no enabled topics."""
    import asyncio

    # `_make_drainer` fills in a MagicMock ``_http`` so the sync-path
    # tests can drive ``_drain_once`` directly; for this lifecycle
    # test we want the pristine start()==never state, so wipe it.
    drainer, _session = _make_drainer()
    drainer._http = None
    task = asyncio.create_task(drainer.serve_forever())
    await asyncio.sleep(0)  # let the task park on _shutdown.wait()
    await drainer.stop()
    await asyncio.wait_for(task, timeout=1.0)
