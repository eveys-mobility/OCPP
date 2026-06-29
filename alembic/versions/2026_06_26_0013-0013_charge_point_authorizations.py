"""charge_point_authorizations.

Revision ID: 0013
Revises: 0012
Create Date: 2026-06-26

Device-authorization allowlist. A separate row per charger tracks
whether the operator has admitted that charger to the fleet:

    pending     first-seen (or operator hasn't decided yet)
    approved    allowed to connect and exchange OCPP frames
    rejected    operator said no; future upgrades return 401
    revoked     was approved, then withdrawn; future upgrades return 401

Why a separate table from `charge_points`:
    `charge_points` is the *identity* table (created on first
    BootNotification; carries vendor / model / firmware). Authorization
    is a distinct lifecycle — a charger can be seen-but-not-decided,
    or revoked without losing its boot/transaction history. Same
    separation rationale as `charge_point_credentials` (#0006).

Grandfather policy (decided per the device-authorization design):
    Every existing `charge_points` row is backfilled to `approved` at
    this migration's runtime. Production fleets stay connected through
    the upgrade; the pending+approval flow only kicks in for chargers
    the gateway has never seen.

The decision-author columns (`decided_by`, `decided_at`) record who
flipped the row to its current `approved` / `rejected` / `revoked`
state. They're nullable because pending rows haven't been decided yet
and because the grandfather backfill has no author (a synthetic
"system: grandfather" string in `decided_by` is the convention).

`last_attempt_ip` and `last_attempt_user_agent` are written every time
the WS pre-handshake encounters this charger, regardless of decision
state. Pending rows show the operator who's actually trying to
connect — useful sniff test against a cp_id-spoofing attempt.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "charge_point_authorizations",
        sa.Column(
            "charge_point_id",
            sa.BigInteger(),
            sa.ForeignKey("charge_points.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        # Closed enum kept as String so future statuses don't need an
        # ALTER TYPE (Postgres enums are a hassle to extend safely).
        # Application code validates the value before write.
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column(
            "requested_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        # Free-form operator identifier — bearer-token subject, email,
        # or a sentinel like "system:grandfather" for backfilled rows.
        # Audit trail is one row deep (latest decision); per-decision
        # history would live in a separate `*_events` table we haven't
        # needed yet.
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
    # Index on status so the "list pending" query in the operator
    # console (the dominant read for this table) is O(pending), not
    # O(fleet).
    op.create_index(
        "ix_charge_point_authorizations_status",
        "charge_point_authorizations",
        ["status"],
    )

    # Grandfather every existing charger as approved. `decided_by` =
    # the sentinel `system:grandfather` so an audit query can tell
    # human decisions apart from the migration-time backfill.
    op.execute(
        """
        INSERT INTO charge_point_authorizations (
            charge_point_id, status, requested_at, decided_at, decided_by
        )
        SELECT id, 'approved', created_at, now(), 'system:grandfather'
        FROM charge_points
        """
    )


def downgrade() -> None:
    op.drop_index(
        "ix_charge_point_authorizations_status",
        table_name="charge_point_authorizations",
    )
    op.drop_table("charge_point_authorizations")
