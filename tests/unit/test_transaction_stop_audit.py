"""Stop durability and station ownership using real ORM statements on SQLite."""

from __future__ import annotations

import asyncio
import importlib.util
import io
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.handlers.v16 import stop_transaction
from eveys_ocpp.idempotency import IdempotencyCache
from eveys_ocpp.metrics import registry as metrics
from eveys_ocpp.persistence.models import (
    ChargePoint,
    SuspectStopTransaction,
    Transaction,
    TransactionEventOutbox,
)
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
    TransactionEventOutbox.__table__.create(engine)
    SuspectStopTransaction.__table__.create(engine)
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
    cp: Any,
    *,
    transaction_id: int = 4823,
    meter_stop: int = 1200,
    reason: str = "PowerLoss",
    timestamp: str = "2026-10-13T07:24:26+00:00",
    id_tag: str | None = None,
) -> Any:
    return await stop_transaction.handle(
        cp,
        message_id="MSG_STOP",
        transaction_id=transaction_id,
        meter_stop=meter_stop,
        timestamp=timestamp,
        reason=reason,
        id_tag=id_tag,
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
    event = database.session.execute(select(TransactionEventOutbox)).scalar_one()
    assert (
        events_pb2.EventEnvelope.FromString(event.body).tx_stopped.charger_reported_at
        == "2026-10-13T07:24:26+00:00"
    )
    assert cp.event_producer.events == []


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
    assert len(database.session.execute(select(TransactionEventOutbox)).scalars().all()) == 1
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
    assert len(database.session.execute(select(TransactionEventOutbox)).scalars().all()) == 1
    assert len(cp.backend_client.requests) == 1


def _suspects(database: _Database) -> list[SuspectStopTransaction]:
    return list(database.session.execute(select(SuspectStopTransaction)).scalars().all())


def _suspect_count(outcome: str) -> float:
    return metrics.STOP_TRANSACTIONS_SUSPECT_TOTAL.labels(outcome=outcome)._value.get()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stop,failed_checks",
    [
        ({"timestamp": "2026-10-13T04:59:59+00:00"}, "stopped_before_start"),
        ({"timestamp": "2026-10-13T07:59:59+03:00"}, "stopped_before_start"),
        (
            {"timestamp": "2026-10-13T04:00:00+00:00", "meter_stop": 999},
            "meter_stop_below_start,stopped_before_start",
        ),
        (
            {"timestamp": "2026-10-13T04:00:00+00:00", "id_tag": "OTHER_CUSTOMER"},
            "stopped_before_start,id_tag_mismatch",
        ),
    ],
)
async def test_first_stop_stamped_before_the_start_is_kept_as_suspect(
    database: _Database,
    stop: dict[str, Any],
    failed_checks: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cp = _cp(database)
    log = MagicMock()
    monkeypatch.setattr(stop_transaction, "log", log)
    held_before = _suspect_count("held")

    response = await _stop(cp, **stop)

    assert response.id_tag_info.status.value == "Accepted"
    row = _row(database)
    assert row.meter_stop_wh is None
    assert row.stopped_reported_at is None
    assert row.stopped_received_at is None
    assert row.idempotency_key is None
    assert database.session.execute(select(TransactionEventOutbox)).first() is None
    assert cp.backend_client.requests == []
    [suspect] = _suspects(database)
    assert suspect.transaction_pk == 4823
    assert suspect.cp_id == "CP_OWNER"
    assert suspect.transaction_id == 4823
    assert suspect.failed_checks == failed_checks
    assert _suspect_count("held") == held_before + 1
    [warning] = log.warning.call_args_list
    assert warning.args == ("stop_transaction.suspect",)
    assert warning.kwargs["held"] is True
    assert warning.kwargs["failed_checks"] == failed_checks


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stop,failed_checks",
    [
        # Register reset after PowerLoss: the charger's only, real Stop
        # (live: ratio tx 1918, meter 4942 -> 1053 Wh, stamped after the start).
        ({"meter_stop": 999, "reason": "PowerLoss"}, "meter_stop_below_start"),
        # OCPP 1.6 lets another card of the same group end the session.
        ({"id_tag": "OTHER_CARD_SAME_PARENT"}, "id_tag_mismatch"),
        (
            {"meter_stop": 999, "id_tag": "OTHER_CARD_SAME_PARENT"},
            "meter_stop_below_start,id_tag_mismatch",
        ),
    ],
)
async def test_lower_meter_or_other_tag_is_logged_but_the_stop_applies(
    database: _Database,
    stop: dict[str, Any],
    failed_checks: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cp = _cp(database)
    log = MagicMock()
    monkeypatch.setattr(stop_transaction, "log", log)
    held_before = _suspect_count("held")
    applied_before = _suspect_count("applied")

    await _stop(cp, **stop)

    meter_stop = stop.get("meter_stop", 1200)
    assert _row(database).meter_stop_wh == meter_stop
    event = database.session.execute(select(TransactionEventOutbox)).scalar_one()
    tx_stopped = events_pb2.EventEnvelope.FromString(event.body).tx_stopped
    assert tx_stopped.consumed_wh == meter_stop - 1000
    assert [request["meter_stop_wh"] for request in cp.backend_client.requests] == [meter_stop]
    assert _suspects(database) == []
    assert _suspect_count("held") == held_before
    assert _suspect_count("applied") == applied_before + 1
    [warning] = log.warning.call_args_list
    assert warning.args == ("stop_transaction.suspect",)
    assert warning.kwargs["held"] is False
    assert warning.kwargs["failed_checks"] == failed_checks


@pytest.mark.asyncio
async def test_replayed_old_stop_does_not_block_the_real_stop(database: _Database) -> None:
    """A charger replayed a days-old queued Stop under the open transaction id;
    the real Stop arrived seconds later and has to close the transaction."""
    cp = _cp(database)

    await _stop(
        cp,
        meter_stop=600,
        timestamp="2026-10-11T09:00:00+00:00",
        id_tag="OLD_CUSTOMER",
        reason="EVDisconnected",
    )
    await _stop(cp, meter_stop=1500, id_tag="TEST_CUSTOMER", reason="Remote")

    row = _row(database)
    assert row.meter_stop_wh == 1500
    assert row.stop_reason == "Remote"
    assert row.idempotency_key == "CP_OWNER:4823:1500"
    event = database.session.execute(select(TransactionEventOutbox)).scalar_one()
    tx_stopped = events_pb2.EventEnvelope.FromString(event.body).tx_stopped
    assert tx_stopped.meter_stop_wh == 1500
    assert tx_stopped.consumed_wh == 500
    assert [request["meter_stop_wh"] for request in cp.backend_client.requests] == [1500]
    [suspect] = _suspects(database)
    assert suspect.meter_stop_wh == 600
    assert suspect.id_tag == "OLD_CUSTOMER"
    assert suspect.stop_reason == "EVDisconnected"
    assert suspect.failed_checks == "meter_stop_below_start,stopped_before_start,id_tag_mismatch"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stop",
    [
        {"meter_stop": 1000, "timestamp": "2026-10-13T05:00:00+00:00"},
        {"timestamp": "2026-10-13T10:24:26+03:00"},
        {"id_tag": "test_customer"},
        {"id_tag": None},
        {"id_tag": ""},
    ],
)
async def test_stop_that_agrees_with_the_start_applies(
    database: _Database, stop: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    cp = _cp(database)
    log = MagicMock()
    monkeypatch.setattr(stop_transaction, "log", log)

    await _stop(cp, **stop)

    assert _row(database).meter_stop_wh == stop.get("meter_stop", 1200)
    assert len(cp.backend_client.requests) == 1
    assert _suspects(database) == []
    assert log.warning.call_args_list == []


@pytest.mark.asyncio
async def test_repeated_suspect_stop_is_stored_once(database: _Database) -> None:
    cp = _cp(database)

    await _stop(cp, meter_stop=999, timestamp="2026-10-12T07:00:00+00:00")
    await _stop(cp, meter_stop=999, timestamp="2026-10-12T07:00:00+00:00")

    assert len(_suspects(database)) == 1
    assert _row(database).meter_stop_wh is None


@pytest.mark.asyncio
async def test_checks_do_not_apply_once_the_transaction_is_stopped(database: _Database) -> None:
    cp = _cp(database)
    await _stop(cp)

    await _stop(cp, meter_stop=999, id_tag="OTHER_CUSTOMER", timestamp="2026-10-12T07:00:00+00:00")

    assert _row(database).meter_stop_wh == 1200
    assert _suspects(database) == []


def test_migration_0020_sets_a_lock_timeout_before_its_ddl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The FK to transactions takes a lock that queues behind an open Start
    admission, and every Start and Stop write would queue behind the
    migration. Give up after 5 s instead, so the deploy can retry."""
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    path = (
        Path(__file__).resolve().parents[2]
        / "alembic/versions/2026_10_19_0020-0020_suspect_stop_transactions.py"
    )
    spec = importlib.util.spec_from_file_location("suspect_stop_migration", path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    assert migration.revision == "0020" and migration.down_revision == "0019"
    output = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output}
    )
    monkeypatch.setattr(migration, "op", Operations(context))

    migration.upgrade()

    sql = output.getvalue()
    assert sql.lstrip().startswith("SET LOCAL lock_timeout = '5s';")
    assert sql.index("lock_timeout") < sql.index("CREATE TABLE suspect_stop_transactions")
