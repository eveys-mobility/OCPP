"""Durable physical Stop delivery using inert producer and real ORM statements."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, delete, select, text, update
from sqlalchemy.orm import Session

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.events import NullEventProducer
from eveys_ocpp.persistence import models
from eveys_ocpp.settings import Settings
from eveys_ocpp.webhooks.dispatcher import WebhookDispatcher


class _Database:
    def __init__(self, session: Session):
        self.session = session
        self.commit_failure: BaseException | None = None

    async def __aenter__(self) -> _Database:
        return self

    async def __aexit__(self, *_: Any) -> None:
        self.session.close()

    async def execute(self, stmt: Any) -> Any:
        return self.session.execute(stmt)

    async def commit(self) -> None:
        if self.commit_failure is not None:
            exc, self.commit_failure = self.commit_failure, None
            raise exc
        self.session.commit()

    async def rollback(self) -> None:
        self.session.rollback()


@pytest.fixture
def database():
    engine = create_engine("sqlite://")
    models.ChargePoint.__table__.create(engine)
    models.Transaction.__table__.create(engine)
    models.TransactionEventOutbox.__table__.create(engine)
    session = Session(engine)
    session.execute(text("PRAGMA foreign_keys=ON"))
    session.add(models.ChargePoint(id=1, cp_id="CP_OWNER"))
    session.add(
        models.Transaction(
            id=4823,
            transaction_id=4823,
            charge_point_id=1,
            connector_id=2,
            id_tag="TEST_ONLY",
            meter_start_wh=1000,
            started_reported_at=datetime(2026, 10, 13, 5, tzinfo=UTC),
        )
    )
    session.commit()
    yield _Database(session)
    session.close()
    engine.dispose()


def _cp(database: _Database):
    return SimpleNamespace(
        id="CP_OWNER",
        settings=Settings(_env_file=None),
        is_pending=False,
        session_factory=lambda: database,
        idempotency=None,
        event_producer=None,
        backend_client=None,
    )


async def _stop(cp: Any, **overrides: Any):
    from eveys_ocpp.handlers.v16.stop_transaction import handle

    return await handle(
        cp,
        transaction_id=4823,
        meter_stop=1200,
        timestamp="2026-10-13T07:24:26+00:00",
        reason="PowerLoss",
        **overrides,
    )


def _events(database: _Database):
    return database.session.execute(select(models.TransactionEventOutbox)).scalars().all()


@pytest.mark.asyncio
async def test_stop_without_producer_atomically_retains_authoritative_event(database):
    await _stop(_cp(database))
    row = _events(database)[0]
    envelope = events_pb2.EventEnvelope.FromString(row.body)
    physical = database.session.execute(select(models.Transaction)).scalar_one()
    assert row.transaction_pk == physical.id
    assert row.published_at is None
    assert envelope.event_id == str(row.event_id)
    assert envelope.tx_stopped.connector_id == 2
    assert (
        envelope.tx_stopped.stopped_received_at
        == physical.stopped_received_at.replace(tzinfo=UTC).isoformat()
    )
    assert envelope.tx_stopped.charger_reported_at == "2026-10-13T07:24:26+00:00"
    original = row.body
    await _stop(_cp(database), message_id="DIFFERENT_RETRY")
    assert [r.body for r in _events(database)] == [original]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("commit failed"), asyncio.CancelledError()])
async def test_failed_stop_commit_rolls_back_both_physical_stop_and_event(database, failure):
    database.commit_failure = failure
    with pytest.raises(type(failure)):
        await _stop(_cp(database))
    assert database.session.execute(select(models.Transaction.meter_stop_wh)).scalar_one() is None
    assert _events(database) == []
    await _stop(_cp(database))
    assert len(_events(database)) == 1


@pytest.mark.asyncio
async def test_null_producer_cannot_mark_outbox_delivered(database):
    from eveys_ocpp.transaction_event_outbox import TransactionEventOutboxPublisher

    await _stop(_cp(database))
    publisher = TransactionEventOutboxPublisher(
        Settings(_env_file=None), session_factory=lambda: database, producer=NullEventProducer()
    )
    assert await publisher.publish_ready() == 0
    assert _events(database)[0].published_at is None


def test_stop_contract_preserves_server_receive_time_and_omits_unknown_legacy_fields():
    dispatcher = WebhookDispatcher(Settings(_env_file=None))
    envelope = events_pb2.EventEnvelope(
        event_id=str(uuid4()),
        cp_id="CP_OWNER",
        occurred_at="2026-10-13T08:00:00+00:00",
        tx_stopped=events_pb2.TxStopped(
            transaction_id=4823,
            connector_id=2,
            stopped_received_at="2026-10-13T07:24:28+00:00",
            charger_reported_at="2026-10-13T07:24:26+00:00",
        ),
    )
    data = dispatcher._build_body(envelope)["data"]
    assert data["connector_id"] == 2
    assert data["stopped_received_at"] == "2026-10-13T07:24:28+00:00"
    assert data["occurred_at"] == "2026-10-13T08:00:00+00:00"
    envelope.tx_stopped.ClearField("connector_id")
    envelope.tx_stopped.ClearField("stopped_received_at")
    historical = dispatcher._build_body(envelope)["data"]
    assert "connector_id" not in historical
    assert "stopped_received_at" not in historical


class _AckProducer:
    broker_acknowledged = True

    def __init__(self, database):
        self.database = database
        self.bodies = []
        self.entered = asyncio.Event()
        self.hang = False

    async def publish(self, **request):
        assert _events(self.database)[0].published_at is None
        self.bodies.append(request["value"])
        self.entered.set()
        if self.hang:
            await asyncio.Event().wait()


def _publisher(database, producer, **settings):
    from eveys_ocpp.transaction_event_outbox import TransactionEventOutboxPublisher

    return TransactionEventOutboxPublisher(
        Settings(_env_file=None, **settings), session_factory=lambda: database, producer=producer
    )


@pytest.mark.asyncio
async def test_broker_ack_before_failed_pg_mark_replays_identical_event(database):
    await _stop(_cp(database))
    producer = _AckProducer(database)
    publisher = _publisher(database, producer)
    database.commit_failure = RuntimeError("mark commit lost")
    with pytest.raises(RuntimeError):
        await publisher.publish_ready()
    assert _events(database)[0].published_at is None
    assert await publisher.publish_ready() == 1
    assert len(producer.bodies) == 2
    assert producer.bodies[0] == producer.bodies[1]
    assert _events(database)[0].published_at is not None


@pytest.mark.asyncio
async def test_publish_timeout_retains_bytes_and_can_retry_after_backoff(database):
    await _stop(_cp(database))
    producer = _AckProducer(database)
    producer.hang = True
    publisher = _publisher(database, producer, transaction_outbox_publish_timeout_seconds=0.01)
    assert await publisher.publish_ready() == 0
    row = _events(database)[0]
    assert row.published_at is None
    assert row.attempts == 1
    assert row.last_error_class == "TimeoutError"
    database.session.execute(
        update(models.TransactionEventOutbox).values(next_attempt_at=datetime(2026, 1, 1))
    )
    database.session.commit()
    producer.hang = False
    assert await publisher.publish_ready() == 1
    assert producer.bodies[0] == producer.bodies[1]


@pytest.mark.asyncio
async def test_external_publish_cancellation_leaves_outbox_unmarked(database):
    await _stop(_cp(database))
    producer = _AckProducer(database)
    producer.hang = True
    publisher = _publisher(database, producer)
    task = asyncio.create_task(publisher.publish_ready())
    await producer.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert _events(database)[0].published_at is None
    producer.hang = False
    assert await publisher.publish_ready() == 1


@pytest.mark.asyncio
async def test_actual_stop_event_does_not_reuse_earlier_admission_rejection_time(database):
    rejected = datetime(2026, 10, 13, 5, 1)
    database.session.execute(
        update(models.Transaction).values(stopped_received_at=rejected, stop_reason="DeAuthorized")
    )
    database.session.commit()
    before = datetime.now(UTC)
    await _stop(_cp(database))
    physical = database.session.execute(select(models.Transaction)).scalar_one()
    assert physical.stopped_received_at == rejected
    event = events_pb2.EventEnvelope.FromString(_events(database)[0].body)
    assert datetime.fromisoformat(event.tx_stopped.stopped_received_at) >= before


@pytest.mark.asyncio
async def test_stop_owner_mismatch_does_not_create_outbox(database):
    cp = _cp(database)
    cp.id = "OTHER_CP"
    await _stop(cp)
    assert _events(database) == []


def test_pg_outbox_index_and_lock_query_compile_with_real_dialect():
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateIndex

    index = next(iter(models.TransactionEventOutbox.__table__.indexes))
    sql = str(CreateIndex(index).compile(dialect=postgresql.dialect()))
    assert "WHERE published_at IS NULL" in sql


@pytest.mark.asyncio
async def test_outbox_lock_is_a_bounded_skip_locked_pg_statement():
    from unittest.mock import AsyncMock, MagicMock

    from sqlalchemy.dialects import postgresql

    from eveys_ocpp.persistence.repositories import lock_ready_transaction_event

    result = MagicMock()
    result.first.return_value = None
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    assert await lock_ready_transaction_event(session, now=datetime.now(UTC)) is None
    sql = str(
        session.execute.await_args.args[0].compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    assert "LIMIT 1" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "published_at IS NULL" in sql


@pytest.mark.asyncio
async def test_outbox_metrics_distinguish_pending_from_committed_publication(database):
    from eveys_ocpp.metrics import registry as metrics

    await _stop(_cp(database))
    publisher = _publisher(database, NullEventProducer())
    assert await publisher.publish_ready() == 0
    assert metrics.TRANSACTION_OUTBOX_PENDING._value.get() == 1
    before = metrics.TRANSACTION_OUTBOX_PUBLISHED_TOTAL._value.get()
    assert await _publisher(database, _AckProducer(database)).publish_ready() == 1
    assert metrics.TRANSACTION_OUTBOX_PENDING._value.get() == 0
    assert metrics.TRANSACTION_OUTBOX_PUBLISHED_TOTAL._value.get() == before + 1
    assert metrics.TRANSACTION_OUTBOX_SAMPLE_TIMESTAMP_SECONDS._value.get() > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("published", [False, True])
async def test_operator_revoke_preserves_pending_and_published_event_ledger(database, published):
    await _stop(_cp(database))
    producer = _AckProducer(database)
    if published:
        assert await _publisher(database, producer).publish_ready() == 1
    before = _events(database)[0]
    identity, body, published_at = before.event_id, before.body, before.published_at
    database.session.execute(
        delete(models.ChargePoint).where(models.ChargePoint.cp_id == "CP_OWNER")
    )
    database.session.commit()
    assert database.session.execute(select(models.Transaction)).scalar_one_or_none() is None
    retained = _events(database)[0]
    assert retained.transaction_pk is None
    assert (retained.event_id, retained.body, retained.published_at) == (
        identity,
        body,
        published_at,
    )
    if not published:
        assert await _publisher(database, producer).publish_ready() == 1
        assert producer.bodies == [body]


def test_migration_0019_compiles_pg_ddl_without_database_or_application_boot(monkeypatch):
    import importlib.util
    import io
    from pathlib import Path

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    path = (
        Path(__file__).resolve().parents[2]
        / "alembic/versions/2026_10_13_0019-0019_transaction_event_outbox.py"
    )
    spec = importlib.util.spec_from_file_location("outbox_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    assert migration.revision == "0019" and migration.down_revision == "0018"
    output = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output}
    )
    monkeypatch.setattr(migration, "op", Operations(context))
    migration.upgrade()
    sql = output.getvalue()
    assert "body BYTEA NOT NULL" in sql
    assert "ON DELETE SET NULL" in sql
    assert "UNIQUE (transaction_pk, topic)" in sql
    assert "WHERE published_at IS NULL" in sql


@pytest.mark.parametrize(
    "field",
    [
        "transaction_outbox_poll_seconds",
        "transaction_outbox_batch_size",
        "transaction_outbox_publish_timeout_seconds",
        "transaction_outbox_cycle_timeout_seconds",
        "ocpp_frame_publish_timeout_seconds",
    ],
)
@pytest.mark.parametrize("value", [0, -1])
def test_delivery_deadlines_and_batch_bounds_reject_nonpositive_settings(field, value):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: value})


def test_fire_and_forget_kafka_cannot_claim_broker_ack():
    from eveys_ocpp.events import KafkaEventProducer

    assert KafkaEventProducer(bootstrap_servers="inert", acks="0").broker_acknowledged is False
    assert KafkaEventProducer(bootstrap_servers="inert", acks="all").broker_acknowledged is True


@pytest.mark.asyncio
async def test_outbox_insert_failure_rolls_back_real_stop_update(database, monkeypatch):
    from unittest.mock import AsyncMock

    from eveys_ocpp.handlers.v16 import stop_transaction

    monkeypatch.setattr(
        stop_transaction,
        "insert_transaction_event",
        AsyncMock(side_effect=RuntimeError("outbox write failed")),
    )
    with pytest.raises(RuntimeError):
        await _stop(_cp(database))
    assert database.session.execute(select(models.Transaction.meter_stop_wh)).scalar_one() is None
    assert _events(database) == []


@pytest.mark.asyncio
async def test_outbox_background_lifecycle_stops_before_a_new_attempt(database):
    await _stop(_cp(database))
    producer = _AckProducer(database)
    publisher = _publisher(database, producer)
    original_publish = producer.publish

    async def publish(**request):
        await original_publish(**request)
        await publisher.stop()

    producer.publish = publish
    await asyncio.wait_for(publisher.serve_forever(), timeout=1)
    assert len(producer.bodies) == 1
    assert _events(database)[0].published_at is not None
