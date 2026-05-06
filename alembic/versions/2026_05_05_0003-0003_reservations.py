"""Reservations table (E2-1C): one row per reservation the charger Accepted.

Revision ID: 0003
Revises: 0002
Create Date: 2026-05-05

Per ADR-0021, the gateway is a mirror of charger-Accepted reservations,
not a lock-holder. This table exists so operator dashboards / mobile
BFF can render reservation state without round-tripping the WebSocket.

`reservation_id` is the surrogate primary key, assigned by the gateway
on `INSERT ... RETURNING id` before the OCPP call. The charger receives
that integer and uses it as the OCPP `reservation_id` field.

Lifecycle: Pending → Active (on charger Accepted) → Cancelled (on
CancelReservation Accepted) | implicitly expired by `expiry_date`.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "reservations",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column(
            "charge_point_id",
            sa.BigInteger(),
            sa.ForeignKey("charge_points.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("connector_id", sa.Integer(), nullable=False),
        sa.Column("id_tag", sa.String(64), nullable=False),
        sa.Column("parent_id_tag", sa.String(64)),
        sa.Column("expiry_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_reservations_charge_point_id", "reservations", ["charge_point_id"])
    op.create_index("ix_reservations_id_tag", "reservations", ["id_tag"])
    # The gateway needs to query "active reservations expiring soon"
    # cheaply for operator dashboards. Composite index on the natural
    # query key.
    op.create_index("ix_reservations_status_expiry", "reservations", ["status", "expiry_date"])


def downgrade() -> None:
    op.drop_index("ix_reservations_status_expiry", table_name="reservations")
    op.drop_index("ix_reservations_id_tag", table_name="reservations")
    op.drop_index("ix_reservations_charge_point_id", table_name="reservations")
    op.drop_table("reservations")
