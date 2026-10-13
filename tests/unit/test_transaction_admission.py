"""Admission regressions with real ORM queries on an in-memory database.

SQLite substitutes only Postgres' ID sequence; backend/Kafka remain fakes.
No application entrypoint, socket, external database, or worker is started.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from ocpp.v16.enums import AuthorizationStatus
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.api.transactions import _transaction_to_response
from eveys_ocpp.handlers.v16 import start_transaction
from eveys_ocpp.persistence import repositories
from eveys_ocpp.persistence.models import ChargePoint, Transaction
from eveys_ocpp.platform import BackendBusinessError, BackendUnavailableError
from eveys_ocpp.settings import Settings

REPORTED_START = "2026-10-13T08:00:00+00:00"


class _SessionAdapter:
    """Execute the production repository statements against local SQLite."""

    def __init__(self, session: Session) -> None:
        self.session = session
        self.commits = 0
        self.rollbacks = 0
        self.fail_commit = False

    async def __aenter__(self) -> _SessionAdapter:
        return self

    async def __aexit__(self, *exc: object) -> None:
        # AsyncSession closes on exit too, rolling back uncommitted writes,
        # including cancellation (which isn't an Exception in Python).
        self.session.close()

    def add(self, row: Any) -> None:
        self.session.add(row)

    async def flush(self) -> None:
        self.session.flush()

    async def execute(self, statement: Any) -> Any:
        return self.session.execute(statement)

    async def commit(self) -> None:
        if self.fail_commit:
            raise RuntimeError("commit failed")
        self.session.commit()
        self.commits += 1

    async def rollback(self) -> None:
        self.session.rollback()
        self.rollbacks += 1


class _Backend:
    def __init__(self, database: _SessionAdapter, decision: str | BaseException) -> None:
        self.database = database
        self.decision = decision
        self.requests: list[dict[str, Any]] = []
        self.commits_at_request: list[int] = []

    async def open_session(self, **request: Any) -> Any:
        self.requests.append(request)
        self.commits_at_request.append(self.database.commits)
        if isinstance(self.decision, BaseException):
            raise self.decision
        return SimpleNamespace(
            transaction_id=request["transaction_id"],
            id_tag_info=SimpleNamespace(status=self.decision),
        )


class _Producer:
    def __init__(self, database: _SessionAdapter) -> None:
        self.database = database
        self.events: list[events_pb2.EventEnvelope] = []
        self.commits_at_publish: list[int] = []

    async def publish(self, **request: Any) -> None:
        envelope = events_pb2.EventEnvelope()
        envelope.ParseFromString(request["value"])
        self.events.append(envelope)
        self.commits_at_publish.append(self.database.commits)


class _HangingBackend(_Backend):
    cancelled = False

    async def open_session(self, **request: Any) -> Any:
        self.requests.append(request)
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled = True


@pytest.fixture
def database() -> Iterator[_SessionAdapter]:
    engine = create_engine("sqlite://")
    ChargePoint.__table__.create(engine)
    Transaction.__table__.create(engine)
    session = Session(engine)
    session.add(ChargePoint(id=42, cp_id="CP_ADMISSION"))
    session.commit()
    sequence = 100

    @event.listens_for(session, "before_flush")
    def assign_sequence(session: Session, *_: object) -> None:
        nonlocal sequence
        # BigInteger PK doesn't use SQLite rowid; stand in for PG's sequence.
        for row in session.new:
            if isinstance(row, Transaction) and row.id is None:
                sequence += 1
                row.id = sequence

    yield _SessionAdapter(session)
    session.close()
    engine.dispose()


def _charge_point(database: _SessionAdapter, backend: _Backend | None) -> Any:
    return SimpleNamespace(
        id="CP_ADMISSION",
        settings=Settings(),
        session_factory=lambda: database,
        is_pending=False,
        backend_client=backend,
        event_producer=_Producer(database),
        # An old Accepted hint must never substitute for the Start decision.
        authorize_cache=SimpleNamespace(status="Accepted"),
    )


async def _start(cp: Any) -> Any:
    return await start_transaction.handle(
        cp,
        connector_id=2,
        id_tag="CUSTOMER_UID",
        meter_start=4500000,
        timestamp=REPORTED_START,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("decision", "expected"),
    [
        (None, AuthorizationStatus.invalid),
        (BackendUnavailableError("offline", error_code="NETWORK"), AuthorizationStatus.invalid),
        (BackendBusinessError("denied", error_code="DENIED"), AuthorizationStatus.invalid),
        ("Invalid", AuthorizationStatus.invalid),
        ("Blocked", AuthorizationStatus.blocked),
        ("Expired", AuthorizationStatus.expired),
        ("ConcurrentTx", AuthorizationStatus.concurrent_tx),
        ("unknown-future-status", AuthorizationStatus.invalid),
    ],
)
async def test_denied_new_start_keeps_closed_audit_without_start_event(
    database: _SessionAdapter, decision: str | BaseException | None, expected: AuthorizationStatus
) -> None:
    backend = None if decision is None else _Backend(database, decision)
    cp = _charge_point(database, backend)

    response = await _start(cp)

    assert response.id_tag_info.status == expected
    row = database.session.execute(select(Transaction)).scalar_one()
    assert response.transaction_id == row.transaction_id
    assert row.stopped_received_at is not None
    assert row.stop_reason == "DeAuthorized"
    assert row.stopped_reported_at is None  # No fabricated charger Stop.
    assert row.meter_stop_wh is None
    assert row.idempotency_key is None
    assert cp.event_producer.events == []
    assert await repositories.count_transactions(database, active=True) == 0


@pytest.mark.asyncio
async def test_backend_acceptance_commits_before_publishing_real_start(
    database: _SessionAdapter,
) -> None:
    backend = _Backend(database, "Accepted")
    cp = _charge_point(database, backend)

    response = await _start(cp)

    assert response.id_tag_info.status == AuthorizationStatus.accepted
    assert backend.commits_at_request == [0]
    assert backend.requests == [
        {
            "transaction_id": 101,
            "cp_id": "CP_ADMISSION",
            "connector_id": 2,
            "id_tag": "CUSTOMER_UID",
            "meter_start_wh": 4500000,
            "started_reported_at": REPORTED_START,
            "idempotency_key": "ocpp-session-open-101",
        }
    ]
    assert cp.event_producer.commits_at_publish == [1]
    assert len(cp.event_producer.events) == 1
    started = cp.event_producer.events[0].tx_started
    assert started.transaction_id == 101
    assert started.connector_id == 2
    assert started.charger_reported_at == REPORTED_START
    assert await repositories.count_transactions(database, active=True) == 1


@pytest.mark.asyncio
async def test_another_start_cannot_reuse_open_physical_or_cached_authorization(
    database: _SessionAdapter,
) -> None:
    backend = _Backend(database, "Accepted")
    cp = _charge_point(database, backend)
    cp.settings = Settings(backend_authorize_fallback="accept_offline")
    first = await _start(cp)
    backend.decision = "Invalid"

    second = await _start(cp)

    assert first.transaction_id == 101
    assert first.id_tag_info.status == AuthorizationStatus.accepted
    assert second.transaction_id == 102
    assert second.id_tag_info.status == AuthorizationStatus.invalid
    assert [request["transaction_id"] for request in backend.requests] == [101, 102]
    open_rows = await repositories.list_transactions(database, after_id=None, limit=10, active=True)
    assert [row["transaction_id"] for row in open_rows] == [101]
    assert [envelope.tx_started.transaction_id for envelope in cp.event_producer.events] == [101]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("callback failed"), asyncio.CancelledError()])
async def test_callback_error_or_cancellation_does_not_leave_phantom_open_row(
    database: _SessionAdapter, failure: BaseException
) -> None:
    cp = _charge_point(database, _Backend(database, failure))

    with pytest.raises(type(failure)):
        await _start(cp)

    assert database.session.execute(select(Transaction)).scalars().all() == []
    assert cp.event_producer.events == []


@pytest.mark.asyncio
async def test_backend_wall_clock_budget_closes_rejected_audit(database: _SessionAdapter) -> None:
    backend = _HangingBackend(database, "Accepted")
    cp = _charge_point(database, backend)
    cp.settings = Settings(
        backend_timeout_sessions_open_seconds=0.1, backend_retry_attempts_sessions_open=0
    )

    try:
        response = await asyncio.wait_for(_start(cp), timeout=0.5)
    except TimeoutError:
        pytest.fail("Start admission exceeded its 0.1s wall-clock budget")

    assert response.id_tag_info.status == AuthorizationStatus.invalid
    row = database.session.execute(select(Transaction)).scalar_one()
    assert row.stopped_received_at is not None
    assert row.stopped_reported_at is None
    assert row.stop_reason == "DeAuthorized"
    assert backend.cancelled is True
    assert cp.event_producer.events == []
    assert await repositories.count_transactions(database, active=True) == 0


@pytest.mark.asyncio
async def test_commit_failure_never_publishes_backend_accepted_start(
    database: _SessionAdapter,
) -> None:
    backend = _Backend(database, "Accepted")
    cp = _charge_point(database, backend)
    database.fail_commit = True

    with pytest.raises(RuntimeError, match="commit failed"):
        await _start(cp)

    assert len(backend.requests) == 1  # Backend reservation may need reconciliation.
    assert database.session.execute(select(Transaction)).scalars().all() == []
    assert cp.event_producer.events == []


@pytest.mark.asyncio
async def test_server_closed_audit_is_excluded_from_all_open_reads(
    database: _SessionAdapter,
) -> None:
    now = datetime(2026, 10, 13, 8, tzinfo=UTC)
    for transaction_id, reported_stop, received_stop in [
        (1, None, None),
        (2, now, None),  # Existing Stop path records reported time only.
        (3, None, now),  # Server rejection has no charger Stop.
    ]:
        database.session.add(
            Transaction(
                id=transaction_id,
                transaction_id=transaction_id,
                charge_point_id=42,
                connector_id=2,
                id_tag="CUSTOMER_UID",
                meter_start_wh=1000,
                started_reported_at=now,
                stopped_reported_at=reported_stop,
                stopped_received_at=received_stop,
            )
        )
    database.session.commit()

    for open_only, expected_ids in [(True, [1]), (False, [3, 2])]:
        per_cp = await repositories.list_transactions_by_cp(
            database, cp_id="CP_ADMISSION", after_id=None, limit=10, open_only=open_only
        )
        assert per_cp is not None
        assert [row["transaction_id"] for row in per_cp] == expected_ids
        assert await repositories.count_transactions_by_cp(
            database, cp_id="CP_ADMISSION", open_only=open_only
        ) == len(expected_ids)
        globally = await repositories.list_transactions(
            database, after_id=None, limit=10, active=open_only
        )
        assert [row["transaction_id"] for row in globally] == expected_ids
        assert await repositories.count_transactions(database, active=open_only) == len(
            expected_ids
        )

    rejected = await repositories.get_transaction_by_id(database, transaction_id=3)
    assert rejected is not None
    response = _transaction_to_response(rejected)
    assert response["open"] is False
    assert response["stopped_reported_at"] is None
    assert response["stopped_received_at"] is not None


@pytest.mark.asyncio
async def test_later_actual_stop_remains_an_audit_event_for_rejected_id(
    database: _SessionAdapter,
) -> None:
    cp = _charge_point(database, _Backend(database, "Invalid"))
    response = await _start(cp)
    before = await repositories.get_transaction_by_id(
        database, transaction_id=response.transaction_id
    )
    assert before is not None
    actual_stop = datetime(2026, 10, 13, 8, 1, tzinfo=UTC)

    applied = await repositories.stop_transaction(
        database,
        transaction_id=response.transaction_id,
        meter_stop_wh=4500010,
        stopped_reported_at=actual_stop,
        reason="Local",
        idempotency_key="CP_ADMISSION:101:4500010",
    )
    await database.commit()

    assert applied == 4500000
    after = await repositories.get_transaction_by_id(
        database, transaction_id=response.transaction_id
    )
    assert after is not None
    assert after["meter_stop_wh"] == 4500010
    assert after["stopped_reported_at"] == actual_stop.replace(tzinfo=None)  # SQLite strips tzinfo.
    assert after["stopped_received_at"] == before["stopped_received_at"]
    assert await repositories.count_transactions(database, active=True) == 0
