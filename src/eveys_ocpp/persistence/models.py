"""SQLAlchemy ORM models.

Conventions:
- Primary keys are surrogate `id` (BigInteger), the natural key (`cp_id`,
  `transaction_id`) is a separate uniquely-indexed column. Surrogate keys
  decouple us from charger-vendor variance.
- Timestamps are stored as `TIMESTAMP WITH TIME ZONE`, always UTC.
- We keep two timestamps for any charger-reported event: the charger's claim
  (`reported_at`) and our server-receive time (`received_at`). The charger's
  clock is untrusted (AGENTS.md OCPP rule 7).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class ChargePoint(Base):
    """One row per charger ever seen. Created on first BootNotification."""

    __tablename__ = "charge_points"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    cp_id: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)

    vendor: Mapped[str | None] = mapped_column(String(128))
    model: Mapped[str | None] = mapped_column(String(128))
    firmware_version: Mapped[str | None] = mapped_column(String(64))
    serial_number: Mapped[str | None] = mapped_column(String(64))

    last_boot_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_status: Mapped[str | None] = mapped_column(String(32))

    # Latest DiagnosticsStatusNotification / FirmwareStatusNotification
    # status string (E2-1F). Latest-wins; the inbound handlers update
    # whichever cell is in scope. Operator dashboards read these for an
    # at-a-glance "is firmware updating right now" without polling the
    # charger.
    last_diagnostics_status: Mapped[str | None] = mapped_column(String(32))
    last_firmware_status: Mapped[str | None] = mapped_column(String(32))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    transactions: Mapped[list[Transaction]] = relationship(
        back_populates="charge_point", cascade="all, delete-orphan"
    )

    local_auth_list: Mapped[LocalAuthList | None] = relationship(
        back_populates="charge_point",
        cascade="all, delete-orphan",
        uselist=False,
    )

    reservations: Mapped[list[Reservation]] = relationship(
        back_populates="charge_point", cascade="all, delete-orphan"
    )

    charging_profiles: Mapped[list[ChargingProfile]] = relationship(
        back_populates="charge_point", cascade="all, delete-orphan"
    )

    credential: Mapped[ChargePointCredential | None] = relationship(
        back_populates="charge_point",
        cascade="all, delete-orphan",
        uselist=False,
    )


class ChargePointCredential(Base):
    """Per-charger Basic Auth password store (E5-6).

    Separate table from `charge_points` so credential lifecycle is
    independent of identity lifecycle: a charger can be seen-but-
    not-yet-provisioned, or rotated without touching its `vendor` /
    `model` / `last_boot_at` columns.

    `password_hash` is a bcrypt hash. The plaintext is never written.
    Operators provision rows via direct SQL or the platform's
    standard secret-distribution path; a REST endpoint follows when
    the operator UI lands.
    """

    __tablename__ = "charge_point_credentials"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    charge_point_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("charge_points.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )

    # bcrypt hash. ~60 bytes; 128 leaves room for future hash schemes
    # encoded with a prefix marker (e.g. `$argon2id$...`) without a
    # column migration.
    password_hash: Mapped[str] = mapped_column(String(128), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    charge_point: Mapped[ChargePoint] = relationship(back_populates="credential")


class Transaction(Base):
    """One row per OCPP transaction (StartTransaction → StopTransaction)."""

    __tablename__ = "transactions"
    __table_args__ = (
        # OCPP 1.6 transaction_id is unique per CSMS (us). Server-assigned.
        UniqueConstraint("transaction_id", name="uq_transactions_transaction_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    transaction_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    charge_point_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("charge_points.id", ondelete="CASCADE"), index=True, nullable=False
    )
    charge_point: Mapped[ChargePoint] = relationship(back_populates="transactions")

    connector_id: Mapped[int] = mapped_column(nullable=False)
    id_tag: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    meter_start_wh: Mapped[int] = mapped_column(BigInteger, nullable=False)
    meter_stop_wh: Mapped[int | None] = mapped_column(BigInteger)

    started_reported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    stopped_reported_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stopped_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stop_reason: Mapped[str | None] = mapped_column(String(32))

    # Idempotency anchor for inbound StopTransaction retries (AGENTS rule 3).
    # Once a stop is recorded for a transaction, replays must be no-ops.
    idempotency_key: Mapped[str | None] = mapped_column(Text, unique=True, index=True)


class LocalAuthList(Base):
    """OCPP 1.6 LocalAuthList — one row per charger that has a list.

    The CSMS pushes the list to the charger via ``SendLocalList``. We
    mirror what the charger accepted so the gateway can answer
    operator queries ("what's on charger X's list?") without polling
    the charger, and so we can re-send the right shape on a
    Differential update without re-fetching from the charger first.

    The charger remains the source of truth for "what list is
    actually active right now"; this row is a mirror that's only
    written when the charger replies ``Accepted``.
    """

    __tablename__ = "local_auth_lists"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    charge_point_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("charge_points.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    charge_point: Mapped[ChargePoint] = relationship(back_populates="local_auth_list")

    # OCPP 1.6 listVersion. Monotonically increasing; the charger uses
    # it to detect "we got a Differential for an older snapshot —
    # reject as VersionMismatch" so the CSMS knows to send a Full.
    list_version: Mapped[int] = mapped_column(nullable=False)

    last_full_replace_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    entries: Mapped[list[LocalAuthListEntry]] = relationship(
        back_populates="local_auth_list", cascade="all, delete-orphan"
    )


class LocalAuthListEntry(Base):
    """One id_tag → IdTagInfo on a charger's local list.

    Two-table layout (rather than JSONB on `local_auth_lists`) so a
    Differential update can do per-tag insert / update / delete
    without read-modify-write of the entire list. Postgres enforces
    the (list, id_tag) uniqueness; SQLAlchemy's ON CONFLICT path
    handles the upsert in the repository layer.
    """

    __tablename__ = "local_auth_list_entries"
    __table_args__ = (
        UniqueConstraint(
            "local_auth_list_id", "id_tag", name="uq_local_auth_list_entries_list_tag"
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    local_auth_list_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("local_auth_lists.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    local_auth_list: Mapped[LocalAuthList] = relationship(back_populates="entries")

    id_tag: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    # AuthorizationStatus enum name from `ocpp.v16.enums.AuthorizationStatus`.
    # Stored as String for the same forward-compat reason ADR-0020 stores
    # proto enums as strings in ClickHouse.
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    parent_id_tag: Mapped[str | None] = mapped_column(String(64))
    expiry_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class Reservation(Base):
    """OCPP 1.6 Reservation — gateway-side mirror of what the charger
    has Accepted (ADR-0021).

    The charger is the source of truth for "is this reservation still
    honoured right now". This row exists for operator queries
    (mobile-BFF / dashboards) without round-tripping the WebSocket.

    Lifecycle:
    - `Pending`: row inserted before the OCPP call, ID assigned via
      `INSERT ... RETURNING id`. If the charger Accepts, flipped to
      `Active`; on any other reply the row is deleted.
    - `Active`: charger accepted; reservation is honoured until
      `expiry_date` or until consumed by a matching StartTransaction.
    - `Cancelled`: operator issued CancelReservation and the charger
      Accepted.
    - Effective expiry is `expiry_date < now()` (computed at query
      time per ADR-0021 — no scheduler).
    """

    __tablename__ = "reservations"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    charge_point_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("charge_points.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    charge_point: Mapped[ChargePoint] = relationship(back_populates="reservations")

    connector_id: Mapped[int] = mapped_column(nullable=False)
    id_tag: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    parent_id_tag: Mapped[str | None] = mapped_column(String(64))

    expiry_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class ChargingProfile(Base):
    """OCPP 1.6 Smart Charging profile — gateway-side mirror of what the
    charger Accepted (ADR-0022).

    The charger is the source of truth for the *composite* schedule —
    the resolver lives there per OCPP § 3.13.4. This row plus its
    `schedule_periods` is the *input* the operator pushed; analytics
    and operator dashboards query it for "show me all profiles on
    charger X" without an OCPP round-trip. To get the resolved
    effective schedule, callers must use `GetCompositeSchedule` (a
    charger round-trip).

    Lifecycle:
    - Inserted on charger Accepted to `SetChargingProfile`. Upsert by
      `(charge_point_id, charging_profile_id)`: replacing a profile
      with the same operator-assigned id replaces this row's schedule
      wholesale.
    - Marked `Cleared` (status flip, not deletion) on charger
      Accepted to `ClearChargingProfile`. Phase 5 may prune
      `Cleared` rows older than N days.
    """

    __tablename__ = "charging_profiles"
    __table_args__ = (
        # Operator-supplied charging_profile_id must be unique within a charger.
        UniqueConstraint(
            "charge_point_id",
            "charging_profile_id",
            name="uq_charging_profiles_cp_profile_id",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    charge_point_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("charge_points.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    charge_point: Mapped[ChargePoint] = relationship(back_populates="charging_profiles")

    # 0 = whole-charger profile (`ChargePointMaxProfile`); positive = specific connector.
    connector_id: Mapped[int] = mapped_column(nullable=False)

    # Wire identifier (the operator picks this; charger uses it as the
    # ID to refer to the profile in subsequent operations).
    charging_profile_id: Mapped[int] = mapped_column(nullable=False)

    stack_level: Mapped[int] = mapped_column(nullable=False)
    charging_profile_purpose: Mapped[str] = mapped_column(String(32), nullable=False)
    charging_profile_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    recurrency_kind: Mapped[str | None] = mapped_column(String(8))

    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    valid_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # OCPP TxProfile binds to a transaction. Other purposes leave it null.
    transaction_id: Mapped[int | None] = mapped_column(BigInteger)

    # Top-level ChargingSchedule fields (the per-period rows live in
    # the child table to keep per-period analytics queries cheap).
    charging_rate_unit: Mapped[str] = mapped_column(String(2), nullable=False)
    min_charging_rate: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    schedule_duration: Mapped[int | None] = mapped_column()
    start_schedule: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    status: Mapped[str] = mapped_column(String(16), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    schedule_periods: Mapped[list[ChargingSchedulePeriod]] = relationship(
        back_populates="charging_profile",
        cascade="all, delete-orphan",
        order_by="ChargingSchedulePeriod.start_period",
    )


class ChargingSchedulePeriod(Base):
    """One period in a ChargingProfile's schedule (ADR-0022).

    `start_period` is offset in seconds from the schedule's anchor
    (charger-local clock per spec; we store the integer verbatim).
    `limit` is in `charging_profile.charging_rate_unit` (W or A).
    """

    __tablename__ = "charging_schedule_periods"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    charging_profile_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("charging_profiles.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    charging_profile: Mapped[ChargingProfile] = relationship(back_populates="schedule_periods")

    start_period: Mapped[int] = mapped_column(nullable=False)
    limit: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    number_phases: Mapped[int | None] = mapped_column()
