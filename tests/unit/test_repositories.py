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
async def test_update_diagnostics_status_executes_update() -> None:
    """E2-1F — `last_diagnostics_status` latest-wins update."""
    session = AsyncMock()
    session.execute = AsyncMock()

    await repositories.update_diagnostics_status(session, cp_id="CP1", status="Uploading")

    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_firmware_status_executes_update() -> None:
    """E2-1F — `last_firmware_status` latest-wins update."""
    session = AsyncMock()
    session.execute = AsyncMock()

    await repositories.update_firmware_status(session, cp_id="CP1", status="Installed")

    session.execute.assert_awaited_once()


# ---- LocalAuthList (E2-1B) ------------------------------------------------


@pytest.mark.asyncio
async def test_get_local_auth_list_version_returns_value_when_present() -> None:
    """Returns the integer list_version when the join finds a row."""
    result_obj = MagicMock()
    result_obj.scalar_one_or_none.return_value = 7

    session = AsyncMock()
    session.execute = AsyncMock(return_value=result_obj)

    version = await repositories.get_local_auth_list_version(session, cp_id="CP1")
    assert version == 7
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_local_auth_list_version_returns_none_when_absent() -> None:
    """Charger without a local_auth_lists row yields None — the
    per-RPC translator turns that into the OCPP -1 sentinel."""
    result_obj = MagicMock()
    result_obj.scalar_one_or_none.return_value = None

    session = AsyncMock()
    session.execute = AsyncMock(return_value=result_obj)

    assert await repositories.get_local_auth_list_version(session, cp_id="CP_NONE") is None


@pytest.mark.asyncio
async def test_replace_local_auth_list_raises_when_charger_missing() -> None:
    """LocalAuthList writes always follow a successful charger reply,
    so the charger row must exist. A missing charger here means a
    pushed list to a charger we've never seen — we surface it loudly
    rather than silently create state."""
    session = AsyncMock()
    session.scalar = AsyncMock(return_value=None)  # ChargePoint lookup misses

    with pytest.raises(LookupError, match="unknown charger"):
        await repositories.replace_local_auth_list(
            session,
            cp_id="GHOST",
            list_version=1,
            entries=[],
            full_replace_at=datetime.now(UTC),
        )


@pytest.mark.asyncio
async def test_id_tag_info_columns_handles_iso_string_expiry() -> None:
    """The repo accepts both `datetime` and ISO-string `expiry_date`
    so callers (proto translators in particular) don't have to parse."""
    cols = repositories._id_tag_info_columns(
        {
            "status": "Accepted",
            "parent_id_tag": "PARENT",
            "expiry_date": "2026-12-31T23:59:59+00:00",
        }
    )
    assert cols["status"] == "Accepted"
    assert cols["parent_id_tag"] == "PARENT"
    assert isinstance(cols["expiry_date"], datetime)


@pytest.mark.asyncio
async def test_id_tag_info_columns_falls_back_on_unexpected_input() -> None:
    """A non-dict (e.g. None smuggled in by a caller's bug) gets a
    safe default so a single bad entry doesn't poison a Differential."""
    cols = repositories._id_tag_info_columns(None)
    assert cols == {"status": "Invalid", "parent_id_tag": None, "expiry_date": None}


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


# ---- Reservations (E2-1C) -------------------------------------------------


@pytest.mark.asyncio
async def test_insert_pending_reservation_returns_assigned_id() -> None:
    """The repo flushes after `session.add` and returns the surrogate
    id Postgres assigned via the autoincrement column."""
    cp_row = MagicMock()
    cp_row.id = 12

    session = MagicMock()
    session.scalar = AsyncMock(return_value=cp_row)
    session.add = MagicMock()

    async def fake_flush() -> None:
        added = session.add.call_args.args[0]
        if added.id is None:
            added.id = 99

    session.flush = AsyncMock(side_effect=fake_flush)

    rid = await repositories.insert_pending_reservation(
        session,
        cp_id="CP1",
        connector_id=2,
        id_tag="TAG",
        parent_id_tag=None,
        expiry_date=datetime(2026, 12, 31, tzinfo=UTC),
    )
    assert rid == 99
    added = session.add.call_args.args[0]
    assert added.status == "Pending"
    assert added.charge_point_id == 12
    assert added.connector_id == 2


@pytest.mark.asyncio
async def test_insert_pending_reservation_raises_when_charger_missing() -> None:
    """Operator can't reserve a connector on a charger we've never
    seen — surface it as `LookupError` rather than write an orphan
    row (FK would error at flush, but this is the friendly path)."""
    session = MagicMock()
    session.scalar = AsyncMock(return_value=None)

    with pytest.raises(LookupError, match="unknown charger"):
        await repositories.insert_pending_reservation(
            session,
            cp_id="GHOST",
            connector_id=1,
            id_tag="X",
            parent_id_tag=None,
            expiry_date=datetime.now(UTC),
        )


@pytest.mark.asyncio
async def test_activate_reservation_runs_update() -> None:
    session = AsyncMock()
    session.execute = AsyncMock()
    await repositories.activate_reservation(session, reservation_id=42)
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_reservation_runs_delete() -> None:
    session = AsyncMock()
    session.execute = AsyncMock()
    await repositories.delete_reservation(session, reservation_id=42)
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancel_reservation_returns_true_when_row_updated() -> None:
    """Active row → update returns rowcount=1 → True."""
    result_obj = MagicMock()
    result_obj.rowcount = 1

    session = AsyncMock()
    session.execute = AsyncMock(return_value=result_obj)

    assert await repositories.cancel_reservation(session, reservation_id=42) is True


@pytest.mark.asyncio
async def test_cancel_reservation_returns_false_when_already_cancelled() -> None:
    """Row already Cancelled (or non-existent) → rowcount=0 → False.
    Mirrors the OCPP-level Rejected outcome."""
    result_obj = MagicMock()
    result_obj.rowcount = 0

    session = AsyncMock()
    session.execute = AsyncMock(return_value=result_obj)

    assert await repositories.cancel_reservation(session, reservation_id=42) is False
