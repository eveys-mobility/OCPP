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

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, Text, UniqueConstraint, func
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
