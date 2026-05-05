"""Query functions used by handlers.

Each function takes an `AsyncSession` (caller owns the transaction) and
returns plain values, never ORM objects with session-bound state. This
keeps the handler/persistence boundary narrow.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    ChargePoint,
    ChargingProfile,
    ChargingSchedulePeriod,
    LocalAuthList,
    LocalAuthListEntry,
    Reservation,
    Transaction,
)


async def upsert_charge_point_boot(
    session: AsyncSession,
    *,
    cp_id: str,
    vendor: str | None,
    model: str | None,
    firmware_version: str | None,
    serial_number: str | None,
    boot_at: datetime,
) -> ChargePoint:
    """Insert-or-update a charger row on BootNotification.

    Implements upsert via Postgres `ON CONFLICT (cp_id) DO UPDATE`. This is
    the idempotency anchor for BootNotification (AGENTS rule 3): replays
    update the row instead of inserting duplicates.
    """
    stmt = (
        pg_insert(ChargePoint)
        .values(
            cp_id=cp_id,
            vendor=vendor,
            model=model,
            firmware_version=firmware_version,
            serial_number=serial_number,
            last_boot_at=boot_at,
        )
        .on_conflict_do_update(
            index_elements=[ChargePoint.cp_id],
            set_={
                "vendor": vendor,
                "model": model,
                "firmware_version": firmware_version,
                "serial_number": serial_number,
                "last_boot_at": boot_at,
            },
        )
        .returning(ChargePoint)
    )
    result = await session.execute(stmt)
    return result.scalar_one()


async def update_heartbeat(session: AsyncSession, *, cp_id: str, at: datetime) -> None:
    """Refresh `last_heartbeat_at` for a charger. No-op if charger unknown."""
    await session.execute(
        update(ChargePoint).where(ChargePoint.cp_id == cp_id).values(last_heartbeat_at=at)
    )


async def update_status(session: AsyncSession, *, cp_id: str, status: str) -> None:
    """Record the latest StatusNotification status string."""
    await session.execute(
        update(ChargePoint).where(ChargePoint.cp_id == cp_id).values(last_status=status)
    )


async def update_diagnostics_status(session: AsyncSession, *, cp_id: str, status: str) -> None:
    """Record the latest DiagnosticsStatusNotification (E2-1F)."""
    await session.execute(
        update(ChargePoint).where(ChargePoint.cp_id == cp_id).values(last_diagnostics_status=status)
    )


async def update_firmware_status(session: AsyncSession, *, cp_id: str, status: str) -> None:
    """Record the latest FirmwareStatusNotification (E2-1F)."""
    await session.execute(
        update(ChargePoint).where(ChargePoint.cp_id == cp_id).values(last_firmware_status=status)
    )


async def get_charge_point_pk(session: AsyncSession, *, cp_id: str) -> int | None:
    """Look up the surrogate `id` for a charger by its `cp_id`."""
    result = await session.execute(select(ChargePoint.id).where(ChargePoint.cp_id == cp_id))
    return result.scalar_one_or_none()


async def get_charge_point_status(
    session: AsyncSession, *, cp_id: str
) -> tuple[str | None, datetime | None] | None:
    """Return `(last_status, last_heartbeat_at)` for a charger, or None if unknown.

    Used by the gRPC `GetChargerStatus` RPC (E2-6) to answer cached
    state queries without an OCPP round-trip. None means the cp_id
    has never sent a BootNotification.
    """
    result = await session.execute(
        select(ChargePoint.last_status, ChargePoint.last_heartbeat_at).where(
            ChargePoint.cp_id == cp_id
        )
    )
    row = result.one_or_none()
    return None if row is None else (row[0], row[1])


async def insert_transaction(
    session: AsyncSession,
    *,
    charge_point_pk: int,
    connector_id: int,
    id_tag: str,
    meter_start_wh: int,
    started_reported_at: datetime,
) -> int:
    """Insert a new transaction row, return its `transaction_id`.

    OCPP 1.6 expects the CSMS to assign `transaction_id`. We use the
    auto-increment surrogate `id` for that — guaranteed unique per CSMS.
    """
    tx = Transaction(
        transaction_id=0,  # placeholder; updated below to id
        charge_point_id=charge_point_pk,
        connector_id=connector_id,
        id_tag=id_tag,
        meter_start_wh=meter_start_wh,
        started_reported_at=started_reported_at,
    )
    session.add(tx)
    await session.flush()  # populate tx.id
    tx.transaction_id = tx.id
    await session.flush()
    return tx.transaction_id


async def stop_transaction(
    session: AsyncSession,
    *,
    transaction_id: int,
    meter_stop_wh: int,
    stopped_reported_at: datetime,
    reason: str | None,
    idempotency_key: str,
) -> bool:
    """Mark a transaction stopped. Returns True if applied, False if already stopped.

    Idempotency: keyed on `idempotency_key` (typically the OCPP message_id of
    the inbound StopTransaction). A replay with the same key is a no-op.
    """
    existing = await session.execute(
        select(Transaction.id).where(Transaction.idempotency_key == idempotency_key)
    )
    if existing.scalar_one_or_none() is not None:
        return False

    result = await session.execute(
        update(Transaction)
        .where(Transaction.transaction_id == transaction_id, Transaction.meter_stop_wh.is_(None))
        .values(
            meter_stop_wh=meter_stop_wh,
            stopped_reported_at=stopped_reported_at,
            stop_reason=reason,
            idempotency_key=idempotency_key,
        )
        .execution_options(synchronize_session=False)
    )
    rowcount: int = result.rowcount  # type: ignore[attr-defined]
    return rowcount > 0


# ---- LocalAuthList (E2-1B) -------------------------------------------------


async def get_local_auth_list_version(session: AsyncSession, *, cp_id: str) -> int | None:
    """Return the gateway-side mirror of the charger's listVersion.

    ``None`` when the charger has no list on the gateway side (e.g.
    fresh charger). The OCPP spec says ``GetLocalListVersion`` should
    return ``-1`` in that case; the per-RPC translator handles the
    sentinel — repos return Python-typed nullables.
    """
    stmt = (
        select(LocalAuthList.list_version)
        .join(ChargePoint, LocalAuthList.charge_point_id == ChargePoint.id)
        .where(ChargePoint.cp_id == cp_id)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def replace_local_auth_list(
    session: AsyncSession,
    *,
    cp_id: str,
    list_version: int,
    entries: list[dict[str, object]],
    full_replace_at: datetime,
) -> None:
    """Apply a Full SendLocalList: clear existing entries, write new ones,
    bump ``list_version``.

    ``entries`` is a list of dicts shaped like ``AuthorizationData``:
    ``{"id_tag": str, "id_tag_info": {"status": str, "parent_id_tag":
    str | None, "expiry_date": datetime | None} | None}``. Entries with
    ``id_tag_info=None`` are silently skipped on a Full replace (a Full
    that lists deletion sentinels is malformed; the spec's Differential
    update is the correct shape for delete).
    """
    cp_row = await _resolve_charge_point(session, cp_id)
    list_row = await _ensure_local_auth_list(session, cp_row.id)

    list_row.list_version = list_version
    list_row.last_full_replace_at = full_replace_at

    # Wipe existing entries — Full replace.
    await session.execute(
        delete(LocalAuthListEntry).where(LocalAuthListEntry.local_auth_list_id == list_row.id)
    )

    new_rows = [
        LocalAuthListEntry(
            local_auth_list_id=list_row.id,
            id_tag=str(entry["id_tag"]),
            **_id_tag_info_columns(entry.get("id_tag_info")),
        )
        for entry in entries
        if entry.get("id_tag_info") is not None
    ]
    session.add_all(new_rows)
    await session.flush()


async def apply_local_auth_list_differential(
    session: AsyncSession,
    *,
    cp_id: str,
    list_version: int,
    entries: list[dict[str, object]],
) -> None:
    """Apply a Differential SendLocalList: per-tag upsert/delete.

    Per OCPP 1.6 spec, an entry with present ``id_tag_info`` is an
    upsert; an entry with omitted/null ``id_tag_info`` is a delete of
    that ``id_tag``. ``list_version`` is bumped to the new value
    regardless (the charger has already accepted the bump by the time
    we persist).
    """
    cp_row = await _resolve_charge_point(session, cp_id)
    list_row = await _ensure_local_auth_list(session, cp_row.id)

    list_row.list_version = list_version

    for entry in entries:
        id_tag = str(entry["id_tag"])
        info = entry.get("id_tag_info")
        if info is None:
            await session.execute(
                delete(LocalAuthListEntry).where(
                    LocalAuthListEntry.local_auth_list_id == list_row.id,
                    LocalAuthListEntry.id_tag == id_tag,
                )
            )
            continue
        # Upsert on (list_id, id_tag).
        cols = _id_tag_info_columns(info)
        stmt = (
            pg_insert(LocalAuthListEntry)
            .values(local_auth_list_id=list_row.id, id_tag=id_tag, **cols)
            .on_conflict_do_update(
                constraint="uq_local_auth_list_entries_list_tag",
                set_=cols,
            )
        )
        await session.execute(stmt)
    await session.flush()


async def _resolve_charge_point(session: AsyncSession, cp_id: str) -> ChargePoint:
    """Look up a charger row by ``cp_id``, raising if absent.

    LocalAuthList writes always follow a successful charger reply, so
    the charger row must exist (BootNotification creates it). Missing
    here means the operator pushed a list to a charger we've never seen
    — surface it loudly rather than create a half-state row.
    """
    cp_row = await session.scalar(select(ChargePoint).where(ChargePoint.cp_id == cp_id))
    if cp_row is None:
        raise LookupError(f"unknown charger: {cp_id}")
    return cp_row


async def _ensure_local_auth_list(session: AsyncSession, charge_point_id: int) -> LocalAuthList:
    """Get-or-create the LocalAuthList row for a charger."""
    list_row = await session.scalar(
        select(LocalAuthList).where(LocalAuthList.charge_point_id == charge_point_id)
    )
    if list_row is not None:
        return list_row
    list_row = LocalAuthList(charge_point_id=charge_point_id, list_version=0)
    session.add(list_row)
    await session.flush()
    return list_row


def _id_tag_info_columns(info: object) -> dict[str, object]:
    """Translate an `id_tag_info` dict into LocalAuthListEntry kwargs."""
    if not isinstance(info, dict):
        # Defensive — caller already filters None on Full replace.
        return {"status": "Invalid", "parent_id_tag": None, "expiry_date": None}
    expiry: object = info.get("expiry_date")
    # Repository accepts both datetime and ISO string for ergonomics.
    if isinstance(expiry, str):
        expiry = datetime.fromisoformat(expiry)
    return {
        "status": str(info.get("status") or "Invalid"),
        "parent_id_tag": info.get("parent_id_tag"),
        "expiry_date": expiry,
    }


# ---- Reservations (E2-1C) --------------------------------------------------


async def insert_pending_reservation(
    session: AsyncSession,
    *,
    cp_id: str,
    connector_id: int,
    id_tag: str,
    parent_id_tag: str | None,
    expiry_date: datetime,
) -> int:
    """Insert a `Pending` reservation row and return the assigned ID.

    The ID is what the gateway forwards to the charger as the OCPP
    ``reservation_id`` (per ADR-0021). Caller flips the row to
    ``Active`` on charger Accepted, or deletes it via
    ``delete_reservation`` on any other reply.
    """
    cp_row = await _resolve_charge_point(session, cp_id)
    row = Reservation(
        charge_point_id=cp_row.id,
        connector_id=connector_id,
        id_tag=id_tag,
        parent_id_tag=parent_id_tag,
        expiry_date=expiry_date,
        status="Pending",
    )
    session.add(row)
    await session.flush()
    return row.id


async def activate_reservation(session: AsyncSession, *, reservation_id: int) -> None:
    """Flip a Pending reservation to Active. No-op if the row is gone
    (a concurrent CancelReservation could have raced)."""
    await session.execute(
        update(Reservation)
        .where(Reservation.id == reservation_id, Reservation.status == "Pending")
        .values(status="Active")
        .execution_options(synchronize_session=False)
    )


async def delete_reservation(session: AsyncSession, *, reservation_id: int) -> None:
    """Drop a reservation row. Used when the charger rejects the
    initial ReserveNow — the row was inserted as Pending solely to
    allocate the ID, so it never came alive on the charger side."""
    await session.execute(delete(Reservation).where(Reservation.id == reservation_id))


async def cancel_reservation(session: AsyncSession, *, reservation_id: int) -> bool:
    """Mark a reservation Cancelled. Returns True if the row was
    Active (or Pending) and got updated; False if the row is gone or
    already Cancelled — same semantics as the OCPP charger reply
    (``Rejected`` for unknown / already-cancelled reservations).
    """
    result = await session.execute(
        update(Reservation)
        .where(
            Reservation.id == reservation_id,
            Reservation.status.in_(("Active", "Pending")),
        )
        .values(status="Cancelled")
        .execution_options(synchronize_session=False)
    )
    rowcount: int = result.rowcount  # type: ignore[attr-defined]
    return rowcount > 0


# ---- Smart Charging (E2-1E, ADR-0022) -------------------------------------


async def upsert_charging_profile(
    session: AsyncSession,
    *,
    cp_id: str,
    connector_id: int,
    profile: dict[str, Any],
    schedule_periods: list[dict[str, Any]],
) -> int:
    """Insert-or-replace a profile mirror after the charger Accepts.

    Upsert key is `(charge_point_id, charging_profile_id)`. The
    operator-supplied `chargingProfileId` is the natural identifier
    on the OCPP wire; replacing a profile with the same ID
    wholesale-replaces the schedule (delete + reinsert children).

    Returns the gateway-side row PK.
    """
    cp_row = await _resolve_charge_point(session, cp_id)
    profile_id = int(profile["charging_profile_id"])

    existing = await session.scalar(
        select(ChargingProfile).where(
            ChargingProfile.charge_point_id == cp_row.id,
            ChargingProfile.charging_profile_id == profile_id,
        )
    )

    if existing is None:
        row = ChargingProfile(
            charge_point_id=cp_row.id,
            connector_id=connector_id,
            charging_profile_id=profile_id,
            status="Active",
            **_charging_profile_fields(profile),
        )
        session.add(row)
        await session.flush()
    else:
        # Wipe child rows, update parent fields, set Active.
        await session.execute(
            delete(ChargingSchedulePeriod).where(
                ChargingSchedulePeriod.charging_profile_id == existing.id
            )
        )
        existing.connector_id = connector_id
        existing.status = "Active"
        for k, v in _charging_profile_fields(profile).items():
            setattr(existing, k, v)
        row = existing
        await session.flush()

    period_rows = [
        ChargingSchedulePeriod(
            charging_profile_id=row.id,
            start_period=int(p["start_period"]),
            limit=Decimal(str(p["limit"])),
            number_phases=int(p["number_phases"]) if p.get("number_phases") is not None else None,
        )
        for p in schedule_periods
    ]
    session.add_all(period_rows)
    await session.flush()
    return row.id


async def clear_charging_profiles(
    session: AsyncSession,
    *,
    cp_id: str,
    profile_id: int | None,
    connector_id: int | None,
    purpose: str | None,
    stack_level: int | None,
) -> int:
    """Mark profiles matching the filter as `Cleared`. Returns the
    number of rows updated. Each filter is optional — None means "any".

    Mirrors the OCPP `ClearChargingProfile` semantics: charger removes
    every profile that matches all set filters; gateway flips the same
    set in its mirror.
    """
    cp_row = await _resolve_charge_point(session, cp_id)
    stmt = update(ChargingProfile).where(
        ChargingProfile.charge_point_id == cp_row.id,
        ChargingProfile.status == "Active",
    )
    if profile_id is not None:
        stmt = stmt.where(ChargingProfile.charging_profile_id == profile_id)
    if connector_id is not None:
        stmt = stmt.where(ChargingProfile.connector_id == connector_id)
    if purpose is not None:
        stmt = stmt.where(ChargingProfile.charging_profile_purpose == purpose)
    if stack_level is not None:
        stmt = stmt.where(ChargingProfile.stack_level == stack_level)
    result = await session.execute(
        stmt.values(status="Cleared").execution_options(synchronize_session=False)
    )
    rowcount: int = result.rowcount  # type: ignore[attr-defined]
    return rowcount


def _charging_profile_fields(profile: dict[str, Any]) -> dict[str, Any]:
    """Translate a wire-shape profile dict into ChargingProfile column kwargs.

    The dict shape comes from the gRPC translator: it carries OCPP
    field names verbatim except the schedule, which is split into
    parent fields (`charging_rate_unit`, `min_charging_rate`,
    `schedule_duration`, `start_schedule`) and the period list (passed
    separately to ``upsert_charging_profile``).
    """
    schedule = profile.get("charging_schedule") or {}
    if not isinstance(schedule, dict):
        schedule = {}
    return {
        "stack_level": int(profile.get("stack_level", 0) or 0),
        "charging_profile_purpose": str(profile.get("charging_profile_purpose") or ""),
        "charging_profile_kind": str(profile.get("charging_profile_kind") or ""),
        "recurrency_kind": (
            str(profile["recurrency_kind"]) if profile.get("recurrency_kind") else None
        ),
        "valid_from": _coerce_optional_datetime(profile.get("valid_from")),
        "valid_to": _coerce_optional_datetime(profile.get("valid_to")),
        "transaction_id": (
            int(profile["transaction_id"]) if profile.get("transaction_id") is not None else None
        ),
        "charging_rate_unit": str(schedule.get("charging_rate_unit") or ""),
        "min_charging_rate": (
            Decimal(str(schedule["min_charging_rate"]))
            if schedule.get("min_charging_rate") is not None
            else None
        ),
        "schedule_duration": (
            int(schedule["duration"]) if schedule.get("duration") is not None else None
        ),
        "start_schedule": _coerce_optional_datetime(schedule.get("start_schedule")),
    }


def _coerce_optional_datetime(value: object) -> datetime | None:
    """Accept either a datetime, an ISO-8601 string, or None."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return None
