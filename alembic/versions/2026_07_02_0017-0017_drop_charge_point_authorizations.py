"""drop_charge_point_authorizations.

Revision ID: 0017
Revises: 0016
Create Date: 2026-07-02

Retire the Postgres-backed device-authorization allowlist. The pending
queue now lives in Redis (`cp:pending:{cp_id}`, 1 h TTL by default —
see `eveys_ocpp/pending_authorizations.py`) and the authorized fleet
is just "any row in `charge_points`". That leaves this table with no
readers, so we drop it here.

Rollback: `downgrade()` recreates the table + status index verbatim
from migration 0013 so a revert is possible for a full release window.
The grandfather backfill from 0013 is deliberately NOT replayed on
downgrade — a running gateway has been treating every `charge_points`
row as authorized, and the code paths that consumed the allowlist are
gone, so a downgrade lands the schema back in place but leaves the
table empty for the operator to reseed. That matches the more
conservative rollback contract (schema restore, no data assumptions).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0017"
down_revision: str | None = "0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Drop the status index explicitly so the operation is idempotent on
    # Postgres versions where a plain `DROP TABLE` doesn't cascade the
    # named index cleanly under all catalog configurations.
    op.drop_index(
        "ix_charge_point_authorizations_status",
        table_name="charge_point_authorizations",
    )
    op.drop_table("charge_point_authorizations")


def downgrade() -> None:
    # Mirror of migration 0013's `upgrade()`, minus the grandfather
    # INSERT — see this migration's module docstring for why.
    op.create_table(
        "charge_point_authorizations",
        sa.Column(
            "charge_point_id",
            sa.BigInteger(),
            sa.ForeignKey("charge_points.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column(
            "requested_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("decided_by", sa.String(length=128), nullable=True),
        sa.Column("last_attempt_ip", sa.String(length=64), nullable=True),
        sa.Column("last_attempt_user_agent", sa.String(length=255), nullable=True),
        sa.Column(
            "last_attempt_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_charge_point_authorizations_status",
        "charge_point_authorizations",
        ["status"],
    )
