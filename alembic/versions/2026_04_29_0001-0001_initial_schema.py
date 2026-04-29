"""Initial schema: charge_points, transactions.

Revision ID: 0001
Revises:
Create Date: 2026-04-29

"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "charge_points",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("cp_id", sa.String(64), nullable=False),
        sa.Column("vendor", sa.String(128)),
        sa.Column("model", sa.String(128)),
        sa.Column("firmware_version", sa.String(64)),
        sa.Column("serial_number", sa.String(64)),
        sa.Column("last_boot_at", sa.DateTime(timezone=True)),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True)),
        sa.Column("last_status", sa.String(32)),
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
    op.create_index("ix_charge_points_cp_id", "charge_points", ["cp_id"], unique=True)

    op.create_table(
        "transactions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("transaction_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "charge_point_id",
            sa.BigInteger(),
            sa.ForeignKey("charge_points.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("connector_id", sa.Integer(), nullable=False),
        sa.Column("id_tag", sa.String(64), nullable=False),
        sa.Column("meter_start_wh", sa.BigInteger(), nullable=False),
        sa.Column("meter_stop_wh", sa.BigInteger()),
        sa.Column("started_reported_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "started_received_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("stopped_reported_at", sa.DateTime(timezone=True)),
        sa.Column("stopped_received_at", sa.DateTime(timezone=True)),
        sa.Column("stop_reason", sa.String(32)),
        sa.Column("idempotency_key", sa.Text()),
        sa.UniqueConstraint("transaction_id", name="uq_transactions_transaction_id"),
    )
    op.create_index("ix_transactions_transaction_id", "transactions", ["transaction_id"])
    op.create_index("ix_transactions_charge_point_id", "transactions", ["charge_point_id"])
    op.create_index("ix_transactions_id_tag", "transactions", ["id_tag"])
    op.create_index(
        "ix_transactions_idempotency_key",
        "transactions",
        ["idempotency_key"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_transactions_idempotency_key", table_name="transactions")
    op.drop_index("ix_transactions_id_tag", table_name="transactions")
    op.drop_index("ix_transactions_charge_point_id", table_name="transactions")
    op.drop_index("ix_transactions_transaction_id", table_name="transactions")
    op.drop_table("transactions")
    op.drop_index("ix_charge_points_cp_id", table_name="charge_points")
    op.drop_table("charge_points")
