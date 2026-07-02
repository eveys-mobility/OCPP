"""authorized_at + purge auth stubs.

Revision ID: 0018
Revises: 0017
Create Date: 2026-07-02

Follow-up to migration 0017's Redis-backed pending queue.

Under the OLD flow `record_authorization_attempt` inserted a
`charge_points` row on every first-sighting WS upgrade — a bare stub
with only `cp_id` populated. The pending/approved state lived in the
separate `charge_point_authorizations` table, which 0017 dropped. That
left `charge_points` full of historical stubs that the new "row in
`charge_points` = authorized" check treats as authorized. Not safe.

This migration:

1. Adds `charge_points.authorized_at` (nullable timestamp). The new
   authorization gate keys off THIS column, not just row existence.
   Set the moment an operator posts `/authorize`; NULL for legacy
   stubs and any unauthorized row.
2. Grandfathers existing rows that have real activity — anything with
   a successful BootNotification (`last_boot_at IS NOT NULL`) gets
   `authorized_at = last_boot_at`. Best available heuristic: a device
   that actually booted was, in practice, a live fleet member the
   operator wanted online. Without the old auth table there's no
   better signal.
3. Purges pure auth stubs — rows where no Boot ever succeeded AND
   there's no heartbeat, no status, no vendor metadata. Those are
   almost certainly rows created by the old first-sighting insert
   for devices that were never approved.

Rollback: `downgrade()` drops the column. It does NOT re-create the
purged stub rows (their only content was `cp_id` and would be
re-created by the OLD auth flow on the next reconnect anyway, so
there's no signal to preserve).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0018"
down_revision: str | None = "0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # New column — nullable. Default NULL means "not authorized yet".
    op.add_column(
        "charge_points",
        sa.Column("authorized_at", sa.DateTime(timezone=True), nullable=True),
    )

    # Grandfather any row that has real activity. `last_boot_at IS NOT
    # NULL` is the strongest signal that this row was ever a live fleet
    # member. Anchor `authorized_at` to `last_boot_at` so an operator
    # scrolling the fleet list sees when the device joined.
    op.execute(
        """
        UPDATE charge_points
        SET authorized_at = last_boot_at
        WHERE authorized_at IS NULL
          AND last_boot_at IS NOT NULL
        """
    )

    # Purge remaining pure stubs. These are rows where NOTHING has ever
    # been reported by the device — no boot, no heartbeat, no status
    # notification, no vendor / model / firmware metadata. Under the old
    # flow that meant "first-sighting stub, operator never approved,
    # device never actually connected past auth reject". Delete them
    # so the fleet list is clean.
    op.execute(
        """
        DELETE FROM charge_points
        WHERE authorized_at IS NULL
          AND vendor IS NULL
          AND model IS NULL
          AND firmware_version IS NULL
          AND last_boot_at IS NULL
          AND last_heartbeat_at IS NULL
          AND last_status IS NULL
        """
    )


def downgrade() -> None:
    op.drop_column("charge_points", "authorized_at")
