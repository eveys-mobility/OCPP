"""charge_point_credentials table (E5-6).

Revision ID: 0006
Revises: 0005
Create Date: 2026-05-08

Per-charger Basic Auth password store. Separate from `charge_points`
so credential lifecycle is independent of identity lifecycle: a
charger can be seen-but-not-yet-provisioned, or rotated without
touching its `vendor` / `model` / `last_boot_at` columns.

`password_hash` is a bcrypt hash. Plaintext is never written.
Operators provision rows via direct SQL or the platform's standard
secret-distribution path; a REST endpoint follows when the operator
UI lands.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "charge_point_credentials",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column(
            "charge_point_id",
            sa.BigInteger(),
            sa.ForeignKey("charge_points.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        # 128 chars leaves room for a future hash-scheme prefix
        # (`$argon2id$...`) without a column migration. bcrypt
        # hashes are ~60 bytes today.
        sa.Column("password_hash", sa.String(length=128), nullable=False),
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
    )


def downgrade() -> None:
    op.drop_table("charge_point_credentials")
