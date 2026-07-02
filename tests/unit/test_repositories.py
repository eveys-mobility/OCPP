"""Unit tests for the persistence repository functions.

These tests mock `session.execute` to verify the right SQLAlchemy
constructs are generated. End-to-end correctness against real Postgres
is covered by E1-13 integration tests (run via `make smoke`).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
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
    """If the idempotency key already exists, return None without updating.
    The non-None vs None distinction (a meter-start reading on apply) is
    the same applied-vs-replay signal callers used to switch on; replay
    means no row mutated and no `meter_start_wh` to return."""
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
    assert applied is None
    # Only one execute (the lookup); no UPDATE on replay.
    assert session.execute.await_count == 1


@pytest.mark.asyncio
async def test_stop_transaction_applies_when_no_replay() -> None:
    """On apply the repo returns the row's `meter_start_wh` so the
    handler can compute `consumed_wh` for the `tx.stopped` envelope
    without a second SELECT."""
    not_existing = MagicMock()
    not_existing.scalar_one_or_none.return_value = None
    update_result = MagicMock()
    update_result.first.return_value = (4_500_000,)  # meter_start_wh from RETURNING

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
    assert applied == 4_500_000
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


# ---- Smart Charging (E2-1E) -----------------------------------------------


def _make_profile(
    profile_id: int = 1, periods: int = 2
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Build a wire-shape profile dict + period dicts."""
    profile = {
        "charging_profile_id": profile_id,
        "stack_level": 0,
        "charging_profile_purpose": "TxDefaultProfile",
        "charging_profile_kind": "Absolute",
        "transaction_id": None,
        "recurrency_kind": None,
        "valid_from": None,
        "valid_to": None,
        "charging_schedule": {
            "duration": 3600,
            "charging_rate_unit": "W",
            "min_charging_rate": None,
            "start_schedule": None,
        },
    }
    period_list = [
        {"start_period": i * 600, "limit": 11000.0 + i * 100, "number_phases": 3}
        for i in range(periods)
    ]
    return profile, period_list


@pytest.mark.asyncio
async def test_upsert_charging_profile_inserts_new_row_when_absent() -> None:
    """Charger has no existing profile with this id → insert path."""
    cp_row = MagicMock()
    cp_row.id = 12

    session = MagicMock()
    session.scalar = AsyncMock(side_effect=[cp_row, None])  # ChargePoint, ChargingProfile (absent)
    session.add = MagicMock()
    session.add_all = MagicMock()
    session.execute = AsyncMock()

    flush_calls = {"n": 0}

    async def fake_flush() -> None:
        flush_calls["n"] += 1
        # Populate the inserted profile's id on first flush.
        added_obj = session.add.call_args.args[0]
        if added_obj.id is None:
            added_obj.id = 100

    session.flush = AsyncMock(side_effect=fake_flush)

    profile, periods = _make_profile(profile_id=42, periods=3)
    rid = await repositories.upsert_charging_profile(
        session, cp_id="CP1", connector_id=1, profile=profile, schedule_periods=periods
    )
    assert rid == 100
    # session.add was called once for the parent; add_all once for the periods.
    session.add.assert_called_once()
    session.add_all.assert_called_once()
    period_rows = session.add_all.call_args.args[0]
    assert len(period_rows) == 3
    assert period_rows[0].start_period == 0
    assert period_rows[1].start_period == 600


@pytest.mark.asyncio
async def test_upsert_charging_profile_updates_when_existing() -> None:
    """Same `(cp_id, charging_profile_id)` → update existing row +
    delete existing periods + insert new ones (wholesale replace)."""
    cp_row = MagicMock()
    cp_row.id = 12
    existing = MagicMock()
    existing.id = 999
    existing.connector_id = 0  # will get overwritten

    session = MagicMock()
    session.scalar = AsyncMock(side_effect=[cp_row, existing])
    session.add = MagicMock()
    session.add_all = MagicMock()
    session.execute = AsyncMock()
    session.flush = AsyncMock()

    profile, periods = _make_profile(profile_id=42, periods=2)
    rid = await repositories.upsert_charging_profile(
        session, cp_id="CP1", connector_id=2, profile=profile, schedule_periods=periods
    )
    assert rid == 999
    # Existing-row branch: no `session.add` for the parent (only for periods).
    session.add.assert_not_called()
    session.add_all.assert_called_once()
    # Delete-old-periods statement was issued.
    session.execute.assert_awaited()
    # Connector and status reflected on the existing row.
    assert existing.connector_id == 2
    assert existing.status == "Active"


@pytest.mark.asyncio
async def test_clear_charging_profiles_returns_rowcount() -> None:
    """`Cleared` flip on rows matching the filter set."""
    cp_row = MagicMock()
    cp_row.id = 12

    result_obj = MagicMock()
    result_obj.rowcount = 3

    session = AsyncMock()
    session.scalar = AsyncMock(return_value=cp_row)
    session.execute = AsyncMock(return_value=result_obj)

    n = await repositories.clear_charging_profiles(
        session,
        cp_id="CP1",
        profile_id=None,
        connector_id=1,
        purpose="TxProfile",
        stack_level=None,
    )
    assert n == 3


@pytest.mark.asyncio
async def test_clear_charging_profiles_unknown_charger_raises() -> None:
    """Same lookup-error pattern as the rest of the family."""
    session = AsyncMock()
    session.scalar = AsyncMock(return_value=None)

    with pytest.raises(LookupError, match="unknown charger"):
        await repositories.clear_charging_profiles(
            session,
            cp_id="GHOST",
            profile_id=None,
            connector_id=None,
            purpose=None,
            stack_level=None,
        )


def test_charging_profile_fields_handles_optional_iso_dates() -> None:
    cols = repositories._charging_profile_fields(
        {
            "stack_level": 1,
            "charging_profile_purpose": "TxDefaultProfile",
            "charging_profile_kind": "Recurring",
            "recurrency_kind": "Daily",
            "valid_from": "2026-12-31T23:59:59+00:00",
            "valid_to": None,
            "transaction_id": None,
            "charging_schedule": {
                "duration": 3600,
                "charging_rate_unit": "W",
                "min_charging_rate": "5.5",
                "start_schedule": "2026-12-31T22:00:00+00:00",
            },
        }
    )
    assert cols["recurrency_kind"] == "Daily"
    assert isinstance(cols["valid_from"], datetime)
    assert cols["valid_to"] is None
    assert cols["min_charging_rate"] is not None
    assert cols["schedule_duration"] == 3600
    assert isinstance(cols["start_schedule"], datetime)


def test_charging_profile_fields_handles_missing_schedule() -> None:
    """Schedule dict missing entirely → safe defaults."""
    cols = repositories._charging_profile_fields(
        {
            "stack_level": 0,
            "charging_profile_purpose": "TxProfile",
            "charging_profile_kind": "Relative",
        }
    )
    assert cols["recurrency_kind"] is None
    assert cols["min_charging_rate"] is None
    assert cols["schedule_duration"] is None


def test_charge_points_filter_conditions_includes_ocpp_version_filter() -> None:
    """Wiring check for the new fleet-list filter (#218 follow-up).

    Operators flipping between fleets of 1.6 and 2.0.1 chargers want
    to scope the list view to one protocol. The route layer passes
    `ocpp_version` through `filter_kwargs`; the conditions helper
    must render it as a SQL equality predicate so both
    `list_charge_points` and `count_charge_points` honour it."""
    conditions = repositories._charge_points_filter_conditions(
        vendor=None,
        ocpp_version="ocpp1.6",
    )
    rendered = [str(c) for c in conditions]
    assert any("ocpp_version" in r for r in rendered), rendered


# ---------------------------------------------------------------------------
# aggregate_transactions (B1 of eveys-console#192)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aggregate_transactions_rejects_unknown_bucket() -> None:
    session = AsyncMock()
    with pytest.raises(ValueError, match="unsupported bucket"):
        await repositories.aggregate_transactions(
            session,
            window_from=datetime(2026, 5, 1, tzinfo=UTC),
            window_to=datetime(2026, 5, 10, tzinfo=UTC),
            bucket="quarter",
            group_by="none",
        )


@pytest.mark.asyncio
async def test_aggregate_transactions_rejects_unknown_group_by() -> None:
    session = AsyncMock()
    with pytest.raises(ValueError, match="unsupported group_by"):
        await repositories.aggregate_transactions(
            session,
            window_from=datetime(2026, 5, 1, tzinfo=UTC),
            window_to=datetime(2026, 5, 10, tzinfo=UTC),
            bucket="day",
            group_by="charger_make",
        )


@pytest.mark.asyncio
async def test_aggregate_transactions_emits_group_when_split_by_cp_id() -> None:
    """Verifies the project shape — `group` is populated when `group_by`
    is non-trivial. The mock returns `mappings()` as the structural
    contract; the helper has to copy `group_value` into the `group` key."""
    rows_mapping = [
        {
            "bucket_at": datetime(2026, 5, 5, tzinfo=UTC),
            "session_count": 1,
            "consumed_wh_total": 1_000,
            "duration_seconds_total": 60,
            "group_value": "CP_BERLIN_017",
        },
    ]
    result_obj = MagicMock()
    result_obj.mappings.return_value.all.return_value = rows_mapping
    session = AsyncMock()
    session.execute = AsyncMock(return_value=result_obj)

    out = await repositories.aggregate_transactions(
        session,
        window_from=datetime(2026, 5, 1, tzinfo=UTC),
        window_to=datetime(2026, 5, 10, tzinfo=UTC),
        bucket="day",
        group_by="cp_id",
    )

    assert len(out) == 1
    assert out[0]["group"] == "CP_BERLIN_017"
    assert out[0]["session_count"] == 1
    assert out[0]["consumed_wh_total"] == 1_000
    assert out[0]["duration_seconds_total"] == 60


# ---- Device authorizations (Redis pending + charge_points delete) ---------


@pytest.mark.asyncio
async def test_delete_charge_point_returns_true_when_row_deleted() -> None:
    """`rowcount > 0` means Postgres actually removed a row — that's
    the "yes, revoke landed" signal the REST layer reads."""
    from sqlalchemy.engine import CursorResult

    cursor = MagicMock(spec=CursorResult)
    cursor.rowcount = 1
    session = AsyncMock()
    session.execute = AsyncMock(return_value=cursor)

    assert await repositories.delete_charge_point(session, cp_id="CP_A") is True


@pytest.mark.asyncio
async def test_delete_charge_point_returns_false_when_row_missing() -> None:
    """Idempotent-in-the-False-case: revoking a cp_id that was never
    in the fleet is still a valid client action (the REST layer maps
    False to 404)."""
    from sqlalchemy.engine import CursorResult

    cursor = MagicMock(spec=CursorResult)
    cursor.rowcount = 0
    session = AsyncMock()
    session.execute = AsyncMock(return_value=cursor)

    assert await repositories.delete_charge_point(session, cp_id="GHOST") is False


@pytest.mark.asyncio
async def test_aggregate_transactions_omits_group_when_group_by_none() -> None:
    rows_mapping = [
        {
            "bucket_at": datetime(2026, 5, 5, tzinfo=UTC),
            "session_count": 5,
            "consumed_wh_total": 12_345,
            "duration_seconds_total": 600,
        },
    ]
    result_obj = MagicMock()
    result_obj.mappings.return_value.all.return_value = rows_mapping
    session = AsyncMock()
    session.execute = AsyncMock(return_value=result_obj)

    out = await repositories.aggregate_transactions(
        session,
        window_from=datetime(2026, 5, 1, tzinfo=UTC),
        window_to=datetime(2026, 5, 10, tzinfo=UTC),
        bucket="day",
        group_by="none",
    )

    assert len(out) == 1
    assert "group" not in out[0]
