"""LocalAuthList tables (E2-1B): local_auth_lists + local_auth_list_entries.

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-05

Adds the gateway-side mirror of OCPP 1.6's LocalAuthList per charger.
The CSMS pushes the list via ``SendLocalList``; we record what the
charger accepted so operator queries don't have to round-trip the
WebSocket.

Two-table layout (one row per (charger, id_tag)) so a Differential
update can do per-tag insert / update / delete without rewriting the
whole list. JSONB would force read-modify-write on every Differential.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "local_auth_lists",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column(
            "charge_point_id",
            sa.BigInteger(),
            sa.ForeignKey("charge_points.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("list_version", sa.Integer(), nullable=False),
        sa.Column("last_full_replace_at", sa.DateTime(timezone=True)),
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
        sa.UniqueConstraint("charge_point_id", name="uq_local_auth_lists_charge_point_id"),
    )

    op.create_table(
        "local_auth_list_entries",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column(
            "local_auth_list_id",
            sa.BigInteger(),
            sa.ForeignKey("local_auth_lists.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("id_tag", sa.String(64), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("parent_id_tag", sa.String(64)),
        sa.Column("expiry_date", sa.DateTime(timezone=True)),
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
            "local_auth_list_id", "id_tag", name="uq_local_auth_list_entries_list_tag"
        ),
    )
    op.create_index(
        "ix_local_auth_list_entries_local_auth_list_id",
        "local_auth_list_entries",
        ["local_auth_list_id"],
    )
    op.create_index("ix_local_auth_list_entries_id_tag", "local_auth_list_entries", ["id_tag"])


def downgrade() -> None:
    op.drop_index("ix_local_auth_list_entries_id_tag", table_name="local_auth_list_entries")
    op.drop_index(
        "ix_local_auth_list_entries_local_auth_list_id",
        table_name="local_auth_list_entries",
    )
    op.drop_table("local_auth_list_entries")
    op.drop_table("local_auth_lists")
