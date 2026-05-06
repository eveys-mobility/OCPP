"""ChargingProfile + ChargingSchedulePeriod tables (E2-1E, ADR-0022).

Revision ID: 0005
Revises: 0004
Create Date: 2026-05-05

Per ADR-0022 the gateway is a mirror of charger-Accepted profiles; the
*composite schedule* lives in the charger and is fetched via
`GetCompositeSchedule` round-trip when an operator needs it. These
tables store the input profiles for operator listing / analytics.

Two-table layout: profile + per-period rows so per-period analytics
queries don't need JSONB unwrap. Profile updates wholesale-replace
their schedule (delete child rows, insert new ones).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "charging_profiles",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column(
            "charge_point_id",
            sa.BigInteger(),
            sa.ForeignKey("charge_points.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("connector_id", sa.Integer(), nullable=False),
        sa.Column("charging_profile_id", sa.Integer(), nullable=False),
        sa.Column("stack_level", sa.Integer(), nullable=False),
        sa.Column("charging_profile_purpose", sa.String(32), nullable=False),
        sa.Column("charging_profile_kind", sa.String(16), nullable=False),
        sa.Column("recurrency_kind", sa.String(8)),
        sa.Column("valid_from", sa.DateTime(timezone=True)),
        sa.Column("valid_to", sa.DateTime(timezone=True)),
        sa.Column("transaction_id", sa.BigInteger()),
        sa.Column("charging_rate_unit", sa.String(2), nullable=False),
        sa.Column("min_charging_rate", sa.Numeric(10, 2)),
        sa.Column("schedule_duration", sa.Integer()),
        sa.Column("start_schedule", sa.DateTime(timezone=True)),
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
        sa.UniqueConstraint(
            "charge_point_id",
            "charging_profile_id",
            name="uq_charging_profiles_cp_profile_id",
        ),
    )
    op.create_index(
        "ix_charging_profiles_charge_point_id", "charging_profiles", ["charge_point_id"]
    )
    # Operators query "all profiles for purpose X on connector Y" frequently.
    op.create_index(
        "ix_charging_profiles_purpose_connector",
        "charging_profiles",
        ["charge_point_id", "charging_profile_purpose", "connector_id"],
    )

    op.create_table(
        "charging_schedule_periods",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column(
            "charging_profile_id",
            sa.BigInteger(),
            sa.ForeignKey("charging_profiles.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("start_period", sa.Integer(), nullable=False),
        sa.Column("limit", sa.Numeric(10, 2), nullable=False),
        sa.Column("number_phases", sa.Integer()),
    )
    op.create_index(
        "ix_charging_schedule_periods_charging_profile_id",
        "charging_schedule_periods",
        ["charging_profile_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_charging_schedule_periods_charging_profile_id",
        table_name="charging_schedule_periods",
    )
    op.drop_table("charging_schedule_periods")
    op.drop_index("ix_charging_profiles_purpose_connector", table_name="charging_profiles")
    op.drop_index("ix_charging_profiles_charge_point_id", table_name="charging_profiles")
    op.drop_table("charging_profiles")
