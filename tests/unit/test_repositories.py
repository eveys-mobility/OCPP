"""Unit tests for the persistence repository functions.

These tests mock `session.execute` to verify the right SQLAlchemy
constructs are generated. End-to-end correctness against real Postgres
is covered by E1-13 integration tests (run via `make smoke`).
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from eveys_ocpp.persistence import repositories


@pytest.mark.asyncio
async def test_upsert_charge_point_boot_returns_row() -> None:
    sentinel_row = object()
    result_obj = MagicMock()
    result_obj.scalar_one.return_value = sentinel_row

    session = AsyncMock()
    session.execute = AsyncMock(return_value=result_obj)

    row = await repositories.upsert_charge_point_boot(
        session,
        cp_id="CP1",
        vendor="ACME",
        model="X1",
        firmware_version="1.0",
        serial_number="SN",
        boot_at=datetime.now(UTC),
    )

    assert row is sentinel_row
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_heartbeat_executes_update() -> None:
    session = AsyncMock()
    session.execute = AsyncMock()

    await repositories.update_heartbeat(session, cp_id="CP1", at=datetime.now(UTC))

    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_status_executes_update() -> None:
    session = AsyncMock()
    session.execute = AsyncMock()

    await repositories.update_status(session, cp_id="CP1", status="Available")

    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_charge_point_pk_returns_scalar() -> None:
    result_obj = MagicMock()
    result_obj.scalar_one_or_none.return_value = 42

    session = AsyncMock()
    session.execute = AsyncMock(return_value=result_obj)

    pk = await repositories.get_charge_point_pk(session, cp_id="CP1")
    assert pk == 42


@pytest.mark.asyncio
async def test_get_charge_point_pk_unknown_returns_none() -> None:
    result_obj = MagicMock()
    result_obj.scalar_one_or_none.return_value = None

    session = AsyncMock()
    session.execute = AsyncMock(return_value=result_obj)

    assert await repositories.get_charge_point_pk(session, cp_id="UNK") is None


@pytest.mark.asyncio
async def test_stop_transaction_skips_replays() -> None:
    """If the idempotency key already exists, return False without updating."""
    existing = MagicMock()
    existing.scalar_one_or_none.return_value = 1  # row exists
    session = AsyncMock()
    session.execute = AsyncMock(return_value=existing)

    applied = await repositories.stop_transaction(
        session,
        transaction_id=1,
        meter_stop_wh=100,
        stopped_reported_at=datetime.now(UTC),
        reason="Local",
        idempotency_key="dup",
    )
    assert applied is False
    # Only one execute (the lookup); no UPDATE on replay.
    assert session.execute.await_count == 1


@pytest.mark.asyncio
async def test_stop_transaction_applies_when_no_replay() -> None:
    not_existing = MagicMock()
    not_existing.scalar_one_or_none.return_value = None
    update_result = MagicMock()
    update_result.rowcount = 1

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=[not_existing, update_result])

    applied = await repositories.stop_transaction(
        session,
        transaction_id=1,
        meter_stop_wh=100,
        stopped_reported_at=datetime.now(UTC),
        reason="Local",
        idempotency_key="new",
    )
    assert applied is True
    assert session.execute.await_count == 2


@pytest.mark.asyncio
async def test_insert_transaction_assigns_transaction_id_to_id() -> None:
    """The repository sets `transaction_id = id` after the first flush."""
    session = MagicMock()
    session.add = MagicMock()  # synchronous in real SQLAlchemy

    async def fake_flush() -> None:
        # Simulate Postgres assigning the surrogate id on the first flush.
        added = session.add.call_args.args[0]
        if added.id is None:
            added.id = 7

    session.flush = AsyncMock(side_effect=fake_flush)

    transaction_id = await repositories.insert_transaction(
        session,
        charge_point_pk=1,
        connector_id=1,
        id_tag="ABC",
        meter_start_wh=0,
        started_reported_at=datetime.now(UTC),
    )
    assert transaction_id == 7
    # Two flushes: one to populate id, one to commit transaction_id = id.
    assert session.flush.await_count == 2
