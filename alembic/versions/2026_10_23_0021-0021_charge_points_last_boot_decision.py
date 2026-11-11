"""charge_points: last BootNotification decision.

Revision ID: 0021
Revises: 0020
Create Date: 2026-10-23

Records how the gateway answered a charger's most recent (non-replay)
BootNotification and why:

- ``last_boot_status`` — the OCPP RegistrationStatus sent back
  (``Accepted`` / ``Pending`` / ``Rejected``).
- ``last_boot_reason`` — who decided: ``backend_pending`` when the
  TOGER backend's ``/charge-points/register`` answered Pending (no
  device registered under this cp_id), ``backend_rejected``,
  ``backend_unavailable`` (fallback policy applied), ``backend_error``,
  ``backend_unknown_status``, ``backend_accepted``, or
  ``local_accepted`` when no backend is wired.
- ``last_boot_decided_at`` — when.

Motivation: a charger the backend keeps answering ``Pending`` re-sends
BootNotification every ~60 s and never sends StatusNotification, so the
Console shows 0 connectors and nothing says who is holding it. The
operator-authorization queue (Redis, migration 0017) is a different
gate and is not affected.

All three columns are nullable and start NULL for every existing row —
"not recorded yet". No backfill: a row's ``last_boot_at`` alone can't
tell an Accepted boot from a Pending loop, so guessing would mislabel
exactly the chargers this is meant to expose. Each charger's next
BootNotification fills the columns.

Rollback: ``downgrade()`` drops the three columns. Nothing else depends
on them.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0021"
down_revision: str | None = "0020"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Nullable ADD COLUMN without a default is a catalog-only change in
    # Postgres — no table rewrite, no long lock on a busy fleet table.
    op.add_column(
        "charge_points",
        sa.Column("last_boot_status", sa.String(length=16), nullable=True),
    )
    op.add_column(
        "charge_points",
        sa.Column("last_boot_reason", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "charge_points",
        sa.Column("last_boot_decided_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("charge_points", "last_boot_decided_at")
    op.drop_column("charge_points", "last_boot_reason")
    op.drop_column("charge_points", "last_boot_status")
