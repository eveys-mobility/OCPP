"""Stop durability and station ownership using real ORM statements on SQLite."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.handlers.v16 import stop_transaction
from eveys_ocpp.idempotency import IdempotencyCache
from eveys_ocpp.persistence.models import ChargePoint, Transaction
from eveys_ocpp.settings import Settings


class _Database:
    def __init__(self, session: Session):
        self.session = session
        self.commit_failure: BaseException | None = None

    async def __aenter__(self) -> _Database:
        return self

    async def __aexit__(self, *_: object) -> None:
        self.session.close()

    async def execute(self, statement: Any) -> Any:
        return self.session.execute(statement)

    async def commit(self) -> None:
        if self.commit_failure is not None:
            failure, self.commit_failure = self.commit_failure, None
            raise failure
        self.session.commit()

    async def rollback(self) -> None:
        self.session.rollback()


class _Redis:
    def __init__(self):
        self.keys: set[str] = set()

    async def set(self, key: str, _value: str, **_: Any) -> bool | None:
        if key in self.keys:
            return None
        self.keys.add(key)
        return True


class _Producer:
    def __init__(self):
        self.events: list[events_pb2.EventEnvelope] = []

    async def publish(self, **request: Any) -> None:
        self.events.append(events_pb2.EventEnvelope.FromString(request["value"]))


class _Backend:
    def __init__(self):
        self.requests: list[dict[str, Any]] = []

    async def close_session(self, **request: Any) -> Any:
        self.requests.append(request)
        return SimpleNamespace(id_tag_info=SimpleNamespace(status="Accepted"))


@pytest.fixture
def database() -> Iterator[_Database]:
    engine = create_engine("sqlite://")
    ChargePoint.__table__.create(engine)
    Transaction.__table__.create(engine)
    session = Session(engine)
    session.add_all([ChargePoint(id=1, cp_id="CP_OWNER"), ChargePoint(id=2, cp_id="CP_OTHER")])
    session.add(
        Transaction(
            id=4823,
            transaction_id=4823,
            charge_point_id=1,
            connector_id=1,
            id_tag="TEST_CUSTOMER",
            meter_start_wh=1000,
            started_reported_at=datetime(2026, 10, 13, 5, tzinfo=UTC),
        )
    )
    session.commit()
    yield _Database(session)
    session.close()
    engine.dispose()


def _cp(database: _Database, *, owner: str = "CP_OWNER") -> Any:
    settings = Settings()
    return SimpleNamespace(
        id=owner,
        settings=settings,
        is_pending=False,
        session_factory=lambda: database,
        idempotency=IdempotencyCache(_Redis(), settings=settings),
        event_producer=_Producer(),
        backend_client=_Backend(),
    )


async def _stop(
    cp: Any, *, transaction_id: int = 4823, meter_stop: int = 1200, reason: str = "PowerLoss"
) -> Any:
    return await stop_transaction.handle(
        cp,
        message_id="MSG_STOP",
        transaction_id=transaction_id,
        meter_stop=meter_stop,
        timestamp="2026-10-13T07:24:26+00:00",
        reason=reason,
    )


def _row(database: _Database) -> Transaction:
    return database.session.execute(select(Transaction)).scalar_one()


@pytest.mark.asyncio
async def test_owner_stop_records_receive_time_separately_from_reported_time(
    database: _Database,
) -> None:
    cp = _cp(database)
    before = datetime.now(UTC).replace(tzinfo=None)

    await _stop(cp)

    row = _row(database)
    assert row.stopped_received_at is not None
    assert before <= row.stopped_received_at <= datetime.now(UTC).replace(tzinfo=None)
    assert row.stopped_reported_at == datetime(2026, 10, 13, 7, 24, 26)
    assert row.stop_reason == "PowerLoss"
    assert row.meter_stop_wh == 1200
    assert cp.event_producer.events[0].tx_stopped.charger_reported_at == "2026-10-13T07:24:26+00:00"


@pytest.mark.asyncio
@pytest.mark.parametrize("owner,transaction_id", [("CP_OTHER", 4823), ("CP_OWNER", 9999)])
async def test_foreign_or_unknown_stop_cannot_close_or_notify_another_transaction(
    database: _Database,
    owner: str,
    transaction_id: int,
) -> None:
    cp = _cp(database, owner=owner)

    await _stop(cp, transaction_id=transaction_id)

    row = _row(database)
    assert row.meter_stop_wh is None
    assert row.stopped_reported_at is None
    assert row.stopped_received_at is None
    assert cp.event_producer.events == []
    assert cp.backend_client.requests == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("commit failed"), asyncio.CancelledError()])
async def test_failed_or_cancelled_commit_does_not_poison_same_message_retry(
    database: _Database,
    failure: BaseException,
) -> None:
    cp = _cp(database)
    database.commit_failure = failure
    with pytest.raises(type(failure)):
        await _stop(cp)
    assert _row(database).meter_stop_wh is None
    assert cp.event_producer.events == []

    await _stop(cp)

    assert _row(database).meter_stop_wh == 1200
    assert len(cp.event_producer.events) == 1
    assert len(cp.backend_client.requests) == 1


@pytest.mark.asyncio
async def test_duplicate_stop_does_not_rewrite_first_meter_reason_or_receive_time(
    database: _Database,
) -> None:
    cp = _cp(database)
    await _stop(cp)
    received_at = _row(database).stopped_received_at

    await _stop(cp, meter_stop=1300, reason="Remote")

    row = _row(database)
    assert row.meter_stop_wh == 1200
    assert row.stop_reason == "PowerLoss"
    assert row.stopped_received_at == received_at
    assert len(cp.event_producer.events) == 1
    assert len(cp.backend_client.requests) == 1
