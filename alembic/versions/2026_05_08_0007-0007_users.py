"""users + user_charge_points (issue #84, PR-A).

Revision ID: 0007
Revises: 0006
Create Date: 2026-05-08

Multi-tenant operator user accounts. The superadmin is in env, not
this table — bootstrap-time identity that doesn't need a DB row.
Every other login-user is a row here, managed by superadmin via
the admin endpoints (PR-B).

`user_charge_points` is the visibility grant table. PR-C wires the
read-endpoint filtering off it; PR-A just creates the schema so
PR-B's `POST /api/v1/admin/users/{id}/chargers` has somewhere to
write.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("username", sa.String(length=64), nullable=False),
        # 128 chars for the same reason as `charge_point_credentials`:
        # bcrypt hashes are ~60 bytes, the column has room for a future
        # `$argon2id$...` migration without a width change.
        sa.Column("password_hash", sa.String(length=128), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        # Per-user webhook URL (issue #84). PR-B surfaces it on the
        # CRUD endpoints; a future PR wires the dispatcher to fan
        # out events per-user.
        sa.Column("webhook_url", sa.Text(), nullable=True),
        # Incident-alert contact channels (issue #84). Used by the
        # alert dispatcher when a charger this user owns goes
        # offline / faults. Both nullable: a tenant may opt out of
        # one channel.
        sa.Column("email", sa.String(length=254), nullable=True),
        sa.Column("phone", sa.String(length=32), nullable=True),
        sa.Column("contact_name", sa.String(length=128), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("username", name="uq_users_username"),
    )
    op.create_index("ix_users_username", "users", ["username"])

    op.create_table(
        "user_charge_points",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column(
            "user_id",
            sa.BigInteger(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "charge_point_id",
            sa.BigInteger(),
            sa.ForeignKey("charge_points.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("user_id", "charge_point_id", name="uq_user_charge_points_pair"),
    )
    op.create_index("ix_user_charge_points_user_id", "user_charge_points", ["user_id"])
    op.create_index(
        "ix_user_charge_points_charge_point_id", "user_charge_points", ["charge_point_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_user_charge_points_charge_point_id", table_name="user_charge_points")
    op.drop_index("ix_user_charge_points_user_id", table_name="user_charge_points")
    op.drop_table("user_charge_points")
    op.drop_index("ix_users_username", table_name="users")
    op.drop_table("users")
