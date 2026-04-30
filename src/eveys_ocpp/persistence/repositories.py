"""Query functions used by handlers.

Each function takes an `AsyncSession` (caller owns the transaction) and
returns plain values, never ORM objects with session-bound state. This
keeps the handler/persistence boundary narrow.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from .models import ChargePoint, Transaction


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
