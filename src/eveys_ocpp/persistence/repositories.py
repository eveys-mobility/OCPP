"""Query functions used by handlers.

Each function takes an `AsyncSession` (caller owns the transaction) and
returns plain values, never ORM objects with session-bound state. This
keeps the handler/persistence boundary narrow.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from .models import (
    ChargePoint,
    ChargePointCertificate,
    ChargePointCredential,
    ChargingProfile,
    ChargingSchedulePeriod,
    LocalAuthList,
    LocalAuthListEntry,
    PendingCertificateSigning,
    Reservation,
    SecurityEvent,
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


async def update_log_status(session: AsyncSession, *, cp_id: str, status: str) -> None:
    """Record the latest LogStatusNotification (TC_079, OCPP 1.6
    Security Whitepaper §4.6)."""
    await session.execute(
        update(ChargePoint).where(ChargePoint.cp_id == cp_id).values(last_log_status=status)
    )


async def upsert_charge_point_certificate(
    session: AsyncSession,
    *,
    cp_id: str,
    certificate_type: str,
    sha256_hash: str,
    pem: str,
) -> None:
    """Mirror an Accepted `InstallCertificate` to the
    `charge_point_certificates` table (TC_075). Idempotent —
    re-installing the same cert (same charger + same SHA-256) is a
    no-op via `ON CONFLICT DO NOTHING`. Charger remains the source
    of truth; this row exists for operator-UI listing.

    Caller passes the already-computed SHA-256 (we don't re-hash
    here so the hash computation lives at the gRPC boundary, where
    the cryptography import is contained)."""
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        raise ValueError(f"unknown cp_id for certificate install: {cp_id!r}")
    stmt = (
        pg_insert(ChargePointCertificate)
        .values(
            charge_point_id=cp_pk,
            certificate_type=certificate_type,
            sha256_hash=sha256_hash,
            pem=pem,
        )
        .on_conflict_do_nothing(constraint="uq_charge_point_certificates_cp_hash")
    )
    await session.execute(stmt)


async def insert_pending_certificate_signing(
    session: AsyncSession,
    *,
    cp_id: str,
    csr: str,
) -> int:
    """Persist a charger-submitted CSR (OCPP 1.6 Security Whitepaper
    §4.13 SignCertificate) for operator review. The actual signing
    pipeline is deferred (#187); this function only writes the
    `pending` row. Returns the row id so the caller can include it
    in the emitted Kafka envelope.

    Charger-side retries: chargers re-submit if no `CertificateSigned`
    reply arrives. We accept duplicates — the operator queue can
    coalesce identical CSRs at review time. Cheaper than a unique
    constraint over a TEXT column.
    """
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        raise ValueError(f"unknown cp_id for sign_certificate: {cp_id!r}")
    row = PendingCertificateSigning(charge_point_id=cp_pk, csr=csr)
    session.add(row)
    await session.flush()
    return row.id


def _pending_csr_to_dict(row: PendingCertificateSigning, cp_id: str) -> dict[str, Any]:
    """Operator-facing shape. Includes the CSR text so the operator
    UI / API consumer can inspect it before approving."""
    return {
        "id": row.id,
        "cp_id": cp_id,
        "csr": row.csr,
        "received_at": row.received_at,
        "status": row.status,
        "signed_at": row.signed_at,
        "approved_by": row.approved_by,
        "rejected_at": row.rejected_at,
        "rejected_reason": row.rejected_reason,
    }


async def list_pending_certificate_signings_by_cp(
    session: AsyncSession,
    *,
    cp_id: str,
    after_id: int | None,
    limit: int,
    status: str | None = None,
) -> list[dict[str, Any]] | None:
    """List pending-CSR rows for a charger, cursor-paginated.

    Returns up to `limit + 1` rows (caller uses the extra row to
    decide `next_cursor`). Returns `None` when the charger doesn't
    exist — distinguishes "no charger" from "no rows" so the REST
    layer can answer 404 vs an empty list.
    """
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        return None
    stmt = select(PendingCertificateSigning).where(
        PendingCertificateSigning.charge_point_id == cp_pk
    )
    if status is not None:
        stmt = stmt.where(PendingCertificateSigning.status == status)
    if after_id is not None:
        stmt = stmt.where(PendingCertificateSigning.id > after_id)
    stmt = stmt.order_by(PendingCertificateSigning.id.asc()).limit(limit + 1)
    result = await session.execute(stmt)
    return [_pending_csr_to_dict(row, cp_id) for row in result.scalars().all()]


async def get_pending_certificate_signing(
    session: AsyncSession,
    *,
    cp_id: str,
    pending_id: int,
) -> dict[str, Any] | None:
    """Fetch one row scoped to a charger. Returns None when either
    the charger or the row doesn't exist; the REST layer collapses
    both to 404."""
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        return None
    stmt = select(PendingCertificateSigning).where(
        and_(
            PendingCertificateSigning.id == pending_id,
            PendingCertificateSigning.charge_point_id == cp_pk,
        )
    )
    row = (await session.execute(stmt)).scalar_one_or_none()
    if row is None:
        return None
    return _pending_csr_to_dict(row, cp_id)


async def mark_pending_certificate_signing_signed(
    session: AsyncSession,
    *,
    cp_id: str,
    pending_id: int,
    signed_chain: str,
    approved_by: str | None,
) -> bool:
    """Transition a `pending` row to `signed`. Returns False when the
    row doesn't exist OR isn't pending — the REST layer returns 404 /
    409 accordingly. Idempotent against double-submits at the SQL
    layer: only `pending` rows match the WHERE clause."""
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        return False
    stmt = (
        update(PendingCertificateSigning)
        .where(
            and_(
                PendingCertificateSigning.id == pending_id,
                PendingCertificateSigning.charge_point_id == cp_pk,
                PendingCertificateSigning.status == "pending",
            )
        )
        .values(
            status="signed",
            signed_at=datetime.now(UTC),
            signed_chain=signed_chain,
            approved_by=approved_by,
        )
    )
    result = await session.execute(stmt)
    rowcount = getattr(result, "rowcount", 0)
    return bool(rowcount and rowcount > 0)


async def mark_pending_certificate_signing_rejected(
    session: AsyncSession,
    *,
    cp_id: str,
    pending_id: int,
    reason: str,
) -> bool:
    """Transition a `pending` row to `rejected`. Same return shape as
    `mark_signed`."""
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        return False
    stmt = (
        update(PendingCertificateSigning)
        .where(
            and_(
                PendingCertificateSigning.id == pending_id,
                PendingCertificateSigning.charge_point_id == cp_pk,
                PendingCertificateSigning.status == "pending",
            )
        )
        .values(
            status="rejected",
            rejected_at=datetime.now(UTC),
            rejected_reason=reason,
        )
    )
    result = await session.execute(stmt)
    rowcount = getattr(result, "rowcount", 0)
    return bool(rowcount and rowcount > 0)


async def get_certificate_pem_by_hash(
    session: AsyncSession,
    *,
    cp_id: str,
    sha256_hash: str,
) -> str | None:
    """Return the stored PEM for a given (charger, sha256_hash) pair
    so the gRPC boundary can rebuild the OCPP §5.1 hash_data Dict
    at delete time. Returns None when the operator passes a hash we
    never recorded — caller should map to a clean validation
    error rather than dispatching a useless DeleteCertificate."""
    result = await session.execute(
        select(ChargePointCertificate.pem).where(
            ChargePointCertificate.charge_point_id == ChargePoint.id,
            ChargePoint.cp_id == cp_id,
            ChargePointCertificate.sha256_hash == sha256_hash,
        )
    )
    return result.scalar_one_or_none()


async def delete_charge_point_certificate(
    session: AsyncSession,
    *,
    cp_id: str,
    sha256_hash: str,
) -> bool:
    """Remove the mirror row after a charger Accepted a
    DeleteCertificate. Returns True when a row was deleted, False
    when the row was already absent (operator deleting a cert the
    mirror never recorded — possible after a manual charger-side
    cert removal). Idempotent in the False case."""
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        return False
    # CursorResult.rowcount is documented but mypy sees the generic
    # `Result[Any]` — narrow at the call site rather than ignore at
    # every use.
    from sqlalchemy.engine import CursorResult

    result = await session.execute(
        delete(ChargePointCertificate).where(
            ChargePointCertificate.charge_point_id == cp_pk,
            ChargePointCertificate.sha256_hash == sha256_hash,
        )
    )
    if isinstance(result, CursorResult):
        return bool(result.rowcount)
    return False  # pragma: no cover — DELETE always yields CursorResult


async def record_security_event(
    session: AsyncSession,
    *,
    cp_id: str,
    event_type: str,
    reported_at: datetime,
    tech_info: str | None,
) -> None:
    """Insert a row into `security_events` for an inbound
    SecurityEventNotification (TC_077 / TC_078, OCPP 1.6 Security
    Whitepaper §4).

    Append-only — operators read the table via the audit query
    surface; we never UPDATE or DELETE here. If the charger doesn't
    yet have a row in `charge_points`, the FK fails — that's
    intentional, callers should make sure the BootNotification
    handler has run first.
    """
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        # Defensive: a SecurityEventNotification before BootNotification
        # would orphan the row. Fail loud — this is a charger
        # protocol error, not a routine retry.
        raise ValueError(f"unknown cp_id for security event: {cp_id!r}")
    session.add(
        SecurityEvent(
            charge_point_id=cp_pk,
            event_type=event_type,
            reported_at=reported_at,
            tech_info=tech_info,
        )
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
) -> int | None:
    """Mark a transaction stopped. Returns the row's `meter_start_wh`
    (Wh) on a real first-time apply, ``None`` if already stopped.

    The non-None vs None distinction is the existing applied-vs-replay
    signal callers used to switch on. Returning the start reading on
    success lets the StopTransaction handler emit a `tx.stopped`
    webhook with `consumed_wh` pre-computed without a second SELECT.

    Idempotency: keyed on `idempotency_key` (typically the OCPP
    message_id of the inbound StopTransaction). A replay with the same
    key is a no-op.
    """
    existing = await session.execute(
        select(Transaction.id).where(Transaction.idempotency_key == idempotency_key)
    )
    if existing.scalar_one_or_none() is not None:
        return None

    # `RETURNING` on the UPDATE so we get the start reading in one
    # round-trip; SQLAlchemy renders `UPDATE ... RETURNING` on Postgres.
    result = await session.execute(
        update(Transaction)
        .where(Transaction.transaction_id == transaction_id, Transaction.meter_stop_wh.is_(None))
        .values(
            meter_stop_wh=meter_stop_wh,
            stopped_reported_at=stopped_reported_at,
            stop_reason=reason,
            idempotency_key=idempotency_key,
        )
        .returning(Transaction.meter_start_wh)
        .execution_options(synchronize_session=False)
    )
    row = result.first()
    if row is None:
        return None
    meter_start_wh: int = row[0]
    return meter_start_wh


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


# ---- E3-7: REST API read functions -----------------------------------------
#
# Cursor-paginated lists + detail fetches for the gateway-side REST API.
# Cursors are keyset-paginated on the surrogate `id` (BigInteger PK).
# Caller (route handler) is responsible for translating typed values to
# the REST envelope; these functions return plain dicts to keep the
# repo↔handler boundary narrow per the existing pattern in this file.


def _charge_point_to_dict(cp: ChargePoint) -> dict[str, Any]:
    """Project a `ChargePoint` ORM row to the REST shape.

    `online` and `pod_id` come from Redis (the Registry), NOT Postgres
    — the route handler enriches the dict with those fields. This
    function only returns Postgres-known data."""
    return {
        "id": cp.id,
        "cp_id": cp.cp_id,
        "vendor": cp.vendor,
        "model": cp.model,
        "firmware_version": cp.firmware_version,
        "serial_number": cp.serial_number,
        "last_boot_at": cp.last_boot_at,
        "last_heartbeat_at": cp.last_heartbeat_at,
        "last_status": cp.last_status,
        "last_diagnostics_status": cp.last_diagnostics_status,
        "last_firmware_status": cp.last_firmware_status,
    }


def _charge_points_filter_conditions(
    *,
    vendor: str | None,
    model: str | None = None,
    firmware_version: str | None = None,
    last_status: str | None = None,
    last_firmware_status: str | None = None,
    last_diagnostics_status: str | None = None,
    last_log_status: str | None = None,
    last_boot_after: datetime | None = None,
    last_boot_before: datetime | None = None,
    last_heartbeat_after: datetime | None = None,
    last_heartbeat_before: datetime | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    cp_id_prefix: str | None = None,
    cp_id_contains: str | None = None,
    cp_ids_in: list[str] | None = None,
    cp_ids_not_in: list[str] | None = None,
) -> list[Any]:
    """Shared WHERE clauses for `list_charge_points` and
    `count_charge_points`. Excludes the cursor/offset boundary.

    `cp_ids_in` / `cp_ids_not_in` are how the route layer pushes the
    Redis online registry into the SQL filter when the operator
    selects online / offline. Pre-computed by the route so the
    count + page math stays consistent. An empty `cp_ids_in` list
    forces a no-match condition (so the page renders 0 of 0) rather
    than being silently ignored.
    """
    conditions: list[Any] = []
    if vendor is not None:
        conditions.append(ChargePoint.vendor == vendor)
    if model is not None:
        conditions.append(ChargePoint.model == model)
    if firmware_version is not None:
        conditions.append(ChargePoint.firmware_version == firmware_version)
    if last_status is not None:
        conditions.append(ChargePoint.last_status == last_status)
    if last_firmware_status is not None:
        conditions.append(ChargePoint.last_firmware_status == last_firmware_status)
    if last_diagnostics_status is not None:
        conditions.append(ChargePoint.last_diagnostics_status == last_diagnostics_status)
    if last_log_status is not None:
        conditions.append(ChargePoint.last_log_status == last_log_status)
    if last_boot_after is not None:
        conditions.append(ChargePoint.last_boot_at >= last_boot_after)
    if last_boot_before is not None:
        conditions.append(ChargePoint.last_boot_at <= last_boot_before)
    if last_heartbeat_after is not None:
        conditions.append(ChargePoint.last_heartbeat_at >= last_heartbeat_after)
    if last_heartbeat_before is not None:
        conditions.append(ChargePoint.last_heartbeat_at <= last_heartbeat_before)
    if created_after is not None:
        conditions.append(ChargePoint.created_at >= created_after)
    if created_before is not None:
        conditions.append(ChargePoint.created_at <= created_before)
    if cp_id_prefix:
        # Operator search: "all CP_ACME_*". Postgres LIKE; we escape
        # `%` / `_` inside the user-supplied prefix so a literal `_`
        # is treated as a literal, not a wildcard.
        safe = cp_id_prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        conditions.append(ChargePoint.cp_id.like(f"{safe}%", escape="\\"))
    if cp_id_contains:
        # Free-text substring match for the operator search box. ILIKE
        # so a user typing "617B" matches "cp_617b…" without caring
        # about case. Same `%` / `_` escaping as `cp_id_prefix`.
        safe = cp_id_contains.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        conditions.append(ChargePoint.cp_id.ilike(f"%{safe}%", escape="\\"))
    if cp_ids_in is not None:
        # Empty list means "no charger is online" — the IN clause would
        # be a SQL error, so we synthesise a false predicate that
        # returns zero rows for both list_ and count_.
        if not cp_ids_in:
            conditions.append(ChargePoint.id == -1)
        else:
            conditions.append(ChargePoint.cp_id.in_(cp_ids_in))
    if cp_ids_not_in:
        conditions.append(ChargePoint.cp_id.not_in(cp_ids_not_in))
    return conditions


async def list_charge_points(
    session: AsyncSession,
    *,
    after_id: int | None,
    limit: int,
    vendor: str | None = None,
    model: str | None = None,
    firmware_version: str | None = None,
    last_status: str | None = None,
    last_firmware_status: str | None = None,
    last_diagnostics_status: str | None = None,
    last_log_status: str | None = None,
    last_boot_after: datetime | None = None,
    last_boot_before: datetime | None = None,
    last_heartbeat_after: datetime | None = None,
    last_heartbeat_before: datetime | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    cp_id_prefix: str | None = None,
    cp_id_contains: str | None = None,
    cp_ids_in: list[str] | None = None,
    cp_ids_not_in: list[str] | None = None,
    offset: int | None = None,
) -> list[dict[str, Any]]:
    """List chargers, ordered by surrogate `id`.

    Two pagination modes — never both:

    - **Cursor mode** (`after_id` set, `offset` None): keyset paginate.
      `after_id` is the exclusive lower bound. Returns up to `limit+1`
      rows so the route handler can detect whether a next page exists
      without an extra COUNT — it trims the extra row and sets
      `next_cursor` from the last kept row.
    - **Offset mode** (`offset` set, `after_id` None): standard
      `OFFSET N LIMIT M`. Returns exactly up to `limit` rows. The
      caller is expected to use `count_charge_points` separately for
      the `total`.

    `vendor` filters by exact match. Presence lives in Redis, so the
    route handler resolves `online=true|false` to a set of cp_ids and
    passes it as `cp_ids_in` / `cp_ids_not_in`. That way count + page
    math both respect the filter (an unbounded post-page filter would
    return wrong totals and shrink pages below `limit`).
    """
    stmt = select(ChargePoint).order_by(ChargePoint.id)
    for cond in _charge_points_filter_conditions(
        vendor=vendor,
        model=model,
        firmware_version=firmware_version,
        last_status=last_status,
        last_firmware_status=last_firmware_status,
        last_diagnostics_status=last_diagnostics_status,
        last_log_status=last_log_status,
        last_boot_after=last_boot_after,
        last_boot_before=last_boot_before,
        last_heartbeat_after=last_heartbeat_after,
        last_heartbeat_before=last_heartbeat_before,
        created_after=created_after,
        created_before=created_before,
        cp_id_prefix=cp_id_prefix,
        cp_id_contains=cp_id_contains,
        cp_ids_in=cp_ids_in,
        cp_ids_not_in=cp_ids_not_in,
    ):
        stmt = stmt.where(cond)
    if offset is not None:
        stmt = stmt.offset(offset).limit(limit)
    else:
        if after_id is not None:
            stmt = stmt.where(ChargePoint.id > after_id)
        stmt = stmt.limit(limit + 1)
    result = await session.execute(stmt)
    return [_charge_point_to_dict(cp) for cp in result.scalars().all()]


async def count_charge_points(
    session: AsyncSession,
    *,
    vendor: str | None = None,
    model: str | None = None,
    firmware_version: str | None = None,
    last_status: str | None = None,
    last_firmware_status: str | None = None,
    last_diagnostics_status: str | None = None,
    last_log_status: str | None = None,
    last_boot_after: datetime | None = None,
    last_boot_before: datetime | None = None,
    last_heartbeat_after: datetime | None = None,
    last_heartbeat_before: datetime | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    cp_id_prefix: str | None = None,
    cp_id_contains: str | None = None,
    cp_ids_in: list[str] | None = None,
    cp_ids_not_in: list[str] | None = None,
) -> int:
    """`SELECT COUNT(*)` over the same filter chain as
    `list_charge_points`. Used by the offset-pagination path to
    populate `pagination.total`."""
    stmt = select(func.count()).select_from(ChargePoint)
    for cond in _charge_points_filter_conditions(
        vendor=vendor,
        model=model,
        firmware_version=firmware_version,
        last_status=last_status,
        last_firmware_status=last_firmware_status,
        last_diagnostics_status=last_diagnostics_status,
        last_log_status=last_log_status,
        last_boot_after=last_boot_after,
        last_boot_before=last_boot_before,
        last_heartbeat_after=last_heartbeat_after,
        last_heartbeat_before=last_heartbeat_before,
        created_after=created_after,
        created_before=created_before,
        cp_id_prefix=cp_id_prefix,
        cp_id_contains=cp_id_contains,
        cp_ids_in=cp_ids_in,
        cp_ids_not_in=cp_ids_not_in,
    ):
        stmt = stmt.where(cond)
    result = await session.execute(stmt)
    return int(result.scalar_one() or 0)


async def get_charge_point_detail(session: AsyncSession, *, cp_id: str) -> dict[str, Any] | None:
    """Single-charger detail, with active reservations + profiles eager-
    loaded.

    Returns `None` when the charger has never sent a BootNotification
    (the route handler maps that to `404 UNKNOWN_CP_ID`).

    "Active reservation" = `status='Active'`. "Active charging profile"
    is everything in the table for v0; we don't have an end-time column
    that lets us narrow further at the SQL layer. Operators reading
    this list see what's currently registered with the gateway, which
    matches the contract's "active" semantics for v0.
    """
    stmt = (
        select(ChargePoint)
        .where(ChargePoint.cp_id == cp_id)
        .options(
            selectinload(ChargePoint.reservations),
            selectinload(ChargePoint.charging_profiles),
        )
    )
    result = await session.execute(stmt)
    cp = result.scalar_one_or_none()
    if cp is None:
        return None
    base = _charge_point_to_dict(cp)
    # Reservation's OCPP-visible id is the surrogate `id` itself
    # (ADR-0021: gateway assigns reservation_id via INSERT ... RETURNING id).
    base["active_reservations"] = [
        {
            "reservation_id": r.id,
            "connector_id": r.connector_id,
            "id_tag": r.id_tag,
            "expiry_date": r.expiry_date,
            "status": r.status,
        }
        for r in cp.reservations
        if r.status == "Active"
    ]
    base["active_charging_profiles"] = [
        {
            "charging_profile_id": p.charging_profile_id,
            "connector_id": p.connector_id,
            "stack_level": p.stack_level,
            "purpose": p.charging_profile_purpose,
            "kind": p.charging_profile_kind,
        }
        for p in cp.charging_profiles
        if p.status != "Cleared"
    ]
    return base


def _transaction_to_dict(tx: Transaction) -> dict[str, Any]:
    """Project a `Transaction` ORM row to the REST shape."""
    consumed: int | None = None
    if tx.meter_stop_wh is not None:
        consumed = tx.meter_stop_wh - tx.meter_start_wh
    return {
        "id": tx.id,
        "transaction_id": tx.transaction_id,
        "charge_point_id": tx.charge_point_id,
        "connector_id": tx.connector_id,
        "id_tag": tx.id_tag,
        "meter_start_wh": tx.meter_start_wh,
        "meter_stop_wh": tx.meter_stop_wh,
        "consumed_wh": consumed,
        "started_reported_at": tx.started_reported_at,
        "started_received_at": tx.started_received_at,
        "stopped_reported_at": tx.stopped_reported_at,
        "stopped_received_at": tx.stopped_received_at,
        "stop_reason": tx.stop_reason,
    }


def _transactions_by_cp_filter_conditions(
    *,
    cp_pk: int,
    id_tag: str | None,
    open_only: bool | None,
    started_from: datetime | None,
    started_to: datetime | None,
    connector_id: int | None = None,
    stop_reason: str | None = None,
    stopped_from: datetime | None = None,
    stopped_to: datetime | None = None,
    min_consumed_wh: int | None = None,
    max_consumed_wh: int | None = None,
) -> list[Any]:
    conditions: list[Any] = [Transaction.charge_point_id == cp_pk]
    if id_tag is not None:
        conditions.append(Transaction.id_tag == id_tag)
    if open_only is True:
        conditions.append(Transaction.stopped_reported_at.is_(None))
    elif open_only is False:
        conditions.append(Transaction.stopped_reported_at.is_not(None))
    if started_from is not None:
        conditions.append(Transaction.started_reported_at >= started_from)
    if started_to is not None:
        conditions.append(Transaction.started_reported_at <= started_to)
    if connector_id is not None:
        conditions.append(Transaction.connector_id == connector_id)
    if stop_reason is not None:
        conditions.append(Transaction.stop_reason == stop_reason)
    if stopped_from is not None:
        conditions.append(Transaction.stopped_reported_at >= stopped_from)
    if stopped_to is not None:
        conditions.append(Transaction.stopped_reported_at <= stopped_to)
    if min_consumed_wh is not None:
        # `consumed_wh` is `meter_stop_wh - meter_start_wh`. Open
        # transactions have no `meter_stop_wh` so the expression is
        # NULL and the row is excluded — the spec for this filter is
        # "rows where we know the consumed energy".
        conditions.append(
            (Transaction.meter_stop_wh - Transaction.meter_start_wh) >= min_consumed_wh
        )
    if max_consumed_wh is not None:
        conditions.append(
            (Transaction.meter_stop_wh - Transaction.meter_start_wh) <= max_consumed_wh
        )
    return conditions


async def list_transactions_by_cp(
    session: AsyncSession,
    *,
    cp_id: str,
    after_id: int | None,
    limit: int,
    id_tag: str | None = None,
    open_only: bool | None = None,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
    connector_id: int | None = None,
    stop_reason: str | None = None,
    stopped_from: datetime | None = None,
    stopped_to: datetime | None = None,
    min_consumed_wh: int | None = None,
    max_consumed_wh: int | None = None,
    offset: int | None = None,
) -> list[dict[str, Any]] | None:
    """Transactions for a charger.

    Pagination modes (mutually exclusive at the caller):

    - **Cursor mode** (`after_id`): keyset-paginate; returns `limit+1`
      rows so the route can detect a next page.
    - **Offset mode** (`offset`): `OFFSET N LIMIT M`; returns exactly
      up to `limit`.

    Returns `None` when the charger is unknown (caller maps to
    `UNKNOWN_CP_ID`); empty list = known charger with no matching txns.

    Filters: `id_tag` exact match; `open_only=True` keeps txns whose
    `stopped_reported_at IS NULL` (currently charging); `False` keeps
    only stopped txns; `None` returns both. Time window matches on
    `started_reported_at`.
    """
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        return None

    conditions = _transactions_by_cp_filter_conditions(
        cp_pk=cp_pk,
        id_tag=id_tag,
        open_only=open_only,
        started_from=started_from,
        started_to=started_to,
        connector_id=connector_id,
        stop_reason=stop_reason,
        stopped_from=stopped_from,
        stopped_to=stopped_to,
        min_consumed_wh=min_consumed_wh,
        max_consumed_wh=max_consumed_wh,
    )
    stmt = select(Transaction).where(and_(*conditions)).order_by(Transaction.id)
    if offset is not None:
        stmt = stmt.offset(offset).limit(limit)
    else:
        if after_id is not None:
            stmt = stmt.where(Transaction.id > after_id)
        stmt = stmt.limit(limit + 1)
    result = await session.execute(stmt)
    return [_transaction_to_dict(tx) for tx in result.scalars().all()]


async def count_transactions_by_cp(
    session: AsyncSession,
    *,
    cp_id: str,
    id_tag: str | None = None,
    open_only: bool | None = None,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
    connector_id: int | None = None,
    stop_reason: str | None = None,
    stopped_from: datetime | None = None,
    stopped_to: datetime | None = None,
    min_consumed_wh: int | None = None,
    max_consumed_wh: int | None = None,
) -> int | None:
    """`SELECT COUNT(*)` matching `list_transactions_by_cp`'s filters.
    Returns `None` for unknown `cp_id` (same semantics as the list)."""
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        return None
    conditions = _transactions_by_cp_filter_conditions(
        cp_pk=cp_pk,
        id_tag=id_tag,
        open_only=open_only,
        started_from=started_from,
        started_to=started_to,
        connector_id=connector_id,
        stop_reason=stop_reason,
        stopped_from=stopped_from,
        stopped_to=stopped_to,
        min_consumed_wh=min_consumed_wh,
        max_consumed_wh=max_consumed_wh,
    )
    stmt = select(func.count()).select_from(Transaction).where(and_(*conditions))
    result = await session.execute(stmt)
    return int(result.scalar_one() or 0)


def _transactions_filter_conditions(
    *,
    cp_id: str | None,
    id_tag: str | None,
    active: bool | None,
    started_from: datetime | None,
    started_to: datetime | None,
    connector_id: int | None = None,
    stop_reason: str | None = None,
    stopped_from: datetime | None = None,
    stopped_to: datetime | None = None,
    min_consumed_wh: int | None = None,
    max_consumed_wh: int | None = None,
) -> list[Any]:
    conditions: list[Any] = []
    if cp_id is not None:
        conditions.append(ChargePoint.cp_id == cp_id)
    if id_tag is not None:
        conditions.append(Transaction.id_tag == id_tag)
    if active is True:
        conditions.append(Transaction.stopped_reported_at.is_(None))
    elif active is False:
        conditions.append(Transaction.stopped_reported_at.is_not(None))
    if started_from is not None:
        conditions.append(Transaction.started_reported_at >= started_from)
    if started_to is not None:
        conditions.append(Transaction.started_reported_at <= started_to)
    if connector_id is not None:
        conditions.append(Transaction.connector_id == connector_id)
    if stop_reason is not None:
        conditions.append(Transaction.stop_reason == stop_reason)
    if stopped_from is not None:
        conditions.append(Transaction.stopped_reported_at >= stopped_from)
    if stopped_to is not None:
        conditions.append(Transaction.stopped_reported_at <= stopped_to)
    if min_consumed_wh is not None:
        conditions.append(
            (Transaction.meter_stop_wh - Transaction.meter_start_wh) >= min_consumed_wh
        )
    if max_consumed_wh is not None:
        conditions.append(
            (Transaction.meter_stop_wh - Transaction.meter_start_wh) <= max_consumed_wh
        )
    return conditions


async def list_transactions(
    session: AsyncSession,
    *,
    after_id: int | None,
    limit: int,
    cp_id: str | None = None,
    id_tag: str | None = None,
    active: bool | None = None,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
    connector_id: int | None = None,
    stop_reason: str | None = None,
    stopped_from: datetime | None = None,
    stopped_to: datetime | None = None,
    min_consumed_wh: int | None = None,
    max_consumed_wh: int | None = None,
    offset: int | None = None,
) -> list[dict[str, Any]]:
    """Global transactions list.

    Pagination modes (mutually exclusive at the caller):

    - **Cursor mode** (`after_id`): keyset-paginate; returns up to
      `limit+1` rows for next-page detection.
    - **Offset mode** (`offset`): `OFFSET N LIMIT M`; returns exactly
      up to `limit`.

    Same filter shape as ``list_transactions_by_cp`` plus an optional
    ``cp_id`` filter (exact match). Unlike the per-cp variant this
    function does not return ``None`` for unknown chargers — an unknown
    or omitted ``cp_id`` simply yields the unfiltered (or empty) result.

    Joins ``charge_points`` so each row's projected dict includes the
    charger's OCPP-visible ``cp_id`` string; the BaaS / operator
    console can render rows without a second lookup.
    """
    stmt = select(Transaction, ChargePoint.cp_id).join(
        ChargePoint, Transaction.charge_point_id == ChargePoint.id
    )
    conditions = _transactions_filter_conditions(
        cp_id=cp_id,
        id_tag=id_tag,
        active=active,
        started_from=started_from,
        started_to=started_to,
        connector_id=connector_id,
        stop_reason=stop_reason,
        stopped_from=stopped_from,
        stopped_to=stopped_to,
        min_consumed_wh=min_consumed_wh,
        max_consumed_wh=max_consumed_wh,
    )
    if conditions:
        stmt = stmt.where(and_(*conditions))
    stmt = stmt.order_by(Transaction.id)
    if offset is not None:
        stmt = stmt.offset(offset).limit(limit)
    else:
        if after_id is not None:
            stmt = stmt.where(Transaction.id > after_id)
        stmt = stmt.limit(limit + 1)

    result = await session.execute(stmt)
    rows: list[dict[str, Any]] = []
    for tx, cp_id_value in result.all():
        row = _transaction_to_dict(tx)
        row["cp_id"] = cp_id_value
        rows.append(row)
    return rows


async def count_transactions(
    session: AsyncSession,
    *,
    cp_id: str | None = None,
    id_tag: str | None = None,
    active: bool | None = None,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
    connector_id: int | None = None,
    stop_reason: str | None = None,
    stopped_from: datetime | None = None,
    stopped_to: datetime | None = None,
    min_consumed_wh: int | None = None,
    max_consumed_wh: int | None = None,
) -> int:
    """`SELECT COUNT(*)` matching `list_transactions`'s filters."""
    stmt = (
        select(func.count())
        .select_from(Transaction)
        .join(ChargePoint, Transaction.charge_point_id == ChargePoint.id)
    )
    conditions = _transactions_filter_conditions(
        cp_id=cp_id,
        id_tag=id_tag,
        active=active,
        started_from=started_from,
        started_to=started_to,
        connector_id=connector_id,
        stop_reason=stop_reason,
        stopped_from=stopped_from,
        stopped_to=stopped_to,
        min_consumed_wh=min_consumed_wh,
        max_consumed_wh=max_consumed_wh,
    )
    if conditions:
        stmt = stmt.where(and_(*conditions))
    result = await session.execute(stmt)
    return int(result.scalar_one() or 0)


async def get_transaction_by_id(
    session: AsyncSession, *, transaction_id: int
) -> dict[str, Any] | None:
    """Single-transaction detail, looked up by `transaction_id` (the
    OCPP-visible id, not the surrogate PK).

    Joins ``charge_points`` so the projected dict carries the
    OCPP-visible ``cp_id`` string — the detail route needs it to scope
    the ClickHouse telemetry lookup, and it's the same shape callers
    already get from ``list_transactions``.

    Returns `None` when no row exists (route handler → 404
    `UNKNOWN_TRANSACTION_ID`)."""
    stmt = (
        select(Transaction, ChargePoint.cp_id)
        .join(ChargePoint, Transaction.charge_point_id == ChargePoint.id)
        .where(Transaction.transaction_id == transaction_id)
    )
    result = await session.execute(stmt)
    row = result.one_or_none()
    if row is None:
        return None
    tx, cp_id_value = row
    out = _transaction_to_dict(tx)
    out["cp_id"] = cp_id_value
    return out


def _reservation_to_dict(r: Reservation) -> dict[str, Any]:
    """Project a `Reservation` ORM row to the REST shape.

    `reservation_id` is the surrogate `id` per ADR-0021 — the gateway
    assigns it via `INSERT ... RETURNING id` and forwards it to the
    charger as the OCPP-visible identifier."""
    return {
        "id": r.id,
        "reservation_id": r.id,
        "connector_id": r.connector_id,
        "id_tag": r.id_tag,
        "parent_id_tag": r.parent_id_tag,
        "expiry_date": r.expiry_date,
        "status": r.status,
        "created_at": r.created_at,
        "updated_at": r.updated_at,
    }


async def list_reservations_by_cp(
    session: AsyncSession,
    *,
    cp_id: str,
    after_id: int | None,
    limit: int,
    status: str | None = None,
    active: bool | None = None,
    id_tag: str | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]] | None:
    """Cursor-paginated reservations for a charger.

    Returns `None` when the charger is unknown (caller maps to
    `UNKNOWN_CP_ID`); empty list = known charger with no matching rows.

    Filters: `status` exact match (e.g. `Active`, `Cancelled`); `active`
    is a query-time computed flag — `True` keeps rows where
    `status='Active'` and `expiry_date > now()` (effective-but-not-
    expired), `False` is the inverse, `None` returns both. `id_tag`
    exact match. `now` overrides the comparison clock for tests.
    """
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        return None

    conditions = [Reservation.charge_point_id == cp_pk]
    if status is not None:
        conditions.append(Reservation.status == status)
    if id_tag is not None:
        conditions.append(Reservation.id_tag == id_tag)
    if active is not None:
        from datetime import UTC

        clock = now if now is not None else datetime.now(UTC)
        if active:
            conditions.append(Reservation.status == "Active")
            conditions.append(Reservation.expiry_date > clock)
        else:
            conditions.append(or_(Reservation.status != "Active", Reservation.expiry_date <= clock))
    if after_id is not None:
        conditions.append(Reservation.id > after_id)

    stmt = select(Reservation).where(and_(*conditions)).order_by(Reservation.id).limit(limit + 1)
    result = await session.execute(stmt)
    return [_reservation_to_dict(r) for r in result.scalars().all()]


def _charging_profile_to_dict(p: ChargingProfile) -> dict[str, Any]:
    """Project a `ChargingProfile` ORM row (with periods eager-loaded)
    to the REST shape. Schedule periods are inlined; their order is
    fixed by the relationship's `order_by=start_period`."""
    return {
        "id": p.id,
        "charging_profile_id": p.charging_profile_id,
        "connector_id": p.connector_id,
        "stack_level": p.stack_level,
        "purpose": p.charging_profile_purpose,
        "kind": p.charging_profile_kind,
        "recurrency_kind": p.recurrency_kind,
        "valid_from": p.valid_from,
        "valid_to": p.valid_to,
        "transaction_id": p.transaction_id,
        "charging_rate_unit": p.charging_rate_unit,
        "min_charging_rate": (
            float(p.min_charging_rate) if p.min_charging_rate is not None else None
        ),
        "schedule_duration": p.schedule_duration,
        "start_schedule": p.start_schedule,
        "status": p.status,
        "schedule_periods": [
            {
                "start_period": sp.start_period,
                "limit": float(sp.limit),
                "number_phases": sp.number_phases,
            }
            for sp in p.schedule_periods
        ],
        "created_at": p.created_at,
        "updated_at": p.updated_at,
    }


async def list_charging_profiles_by_cp(
    session: AsyncSession,
    *,
    cp_id: str,
    after_id: int | None,
    limit: int,
    purpose: str | None = None,
    stack_level: int | None = None,
    connector_id: int | None = None,
) -> list[dict[str, Any]] | None:
    """Cursor-paginated charging profiles for a charger, with their
    schedule periods inlined.

    Returns `None` when the charger is unknown; empty list = known
    charger, no matching profiles.

    Filters: `purpose` (e.g. `TxDefaultProfile`), `stack_level`,
    `connector_id` (0 = whole-charger profile, positive = specific).
    Cleared profiles are returned along with active ones; callers that
    only want live ones should pass through the `status` field.
    """
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        return None

    conditions = [ChargingProfile.charge_point_id == cp_pk]
    if purpose is not None:
        conditions.append(ChargingProfile.charging_profile_purpose == purpose)
    if stack_level is not None:
        conditions.append(ChargingProfile.stack_level == stack_level)
    if connector_id is not None:
        conditions.append(ChargingProfile.connector_id == connector_id)
    if after_id is not None:
        conditions.append(ChargingProfile.id > after_id)

    stmt = (
        select(ChargingProfile)
        .where(and_(*conditions))
        .options(selectinload(ChargingProfile.schedule_periods))
        .order_by(ChargingProfile.id)
        .limit(limit + 1)
    )
    result = await session.execute(stmt)
    return [_charging_profile_to_dict(p) for p in result.scalars().all()]


# ---- E5-6: per-charger Basic Auth credentials -----------------------------


async def get_credential_hash(session: AsyncSession, *, cp_id: str) -> str | None:
    """Return the bcrypt hash for a charger, or `None` if no credential
    row exists.

    None means "not provisioned" — the WS server's Basic Auth gate
    decides whether that's accept (permissive mode) or reject
    (strict mode) based on `Settings.ws_basic_auth_required`.
    """
    stmt = (
        select(ChargePointCredential.password_hash)
        .join(ChargePoint, ChargePoint.id == ChargePointCredential.charge_point_id)
        .where(ChargePoint.cp_id == cp_id)
    )
    result = await session.execute(stmt)
    row = result.scalar_one_or_none()
    return str(row) if row is not None else None


async def upsert_charge_point_credential(
    session: AsyncSession,
    *,
    cp_id: str,
    password_hash: str,
) -> bool:
    """Upsert the bcrypt hash for a charger (TC_073). Returns True when
    the charger row exists and the credential is now in place;
    False when the `cp_id` is unknown (no `charge_points` row to FK
    against — the REST layer surfaces this as 404).

    The caller bcrypts the plaintext at the boundary so the plaintext
    never reaches a SQL statement or a log line.
    """
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        return False
    stmt = (
        pg_insert(ChargePointCredential)
        .values(charge_point_id=cp_pk, password_hash=password_hash)
        .on_conflict_do_update(
            index_elements=[ChargePointCredential.charge_point_id],
            set_={"password_hash": password_hash, "updated_at": func.now()},
        )
    )
    await session.execute(stmt)
    return True


async def delete_charge_point_credential(session: AsyncSession, *, cp_id: str) -> bool:
    """Drop a charger's credential row. Returns True if a row was
    deleted, False if there was none (the REST layer treats both
    as success — idempotent unprovisioning).
    """
    cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
    if cp_pk is None:
        return False
    stmt = delete(ChargePointCredential).where(ChargePointCredential.charge_point_id == cp_pk)
    result = await session.execute(stmt)
    rowcount = getattr(result, "rowcount", 0)
    return bool(rowcount and rowcount > 0)
