"""Drop charge_points.charger_type (introduced in 0014, never wired in).

Revision ID: 0015
Revises: 0014
Create Date: 2026-06-30

0014 added a `charger_type` column so the post-boot ChangeConfiguration
push could pick AC vs DC measurand lists. In practice
BootNotification doesn't carry a reliable AC/DC signal — vendor /
model strings vary too widely — so the column was always going to be
operator-labelled, which made the feature equivalent to "operator
sends a ChangeConfiguration manually". The column adds no value over
that, so 0015 drops it. The post-boot push now sends only
type-agnostic keys (HeartbeatInterval, ConnectionTimeOut, …);
measurand lists are an operator decision via the existing per-CP
ChangeConfiguration command.

The column was never queried by anything outside the post-boot push
itself, so the downgrade-equivalent on existing rows is a no-op.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_column("charge_points", "charger_type")


def downgrade() -> None:
    op.add_column(
        "charge_points",
        sa.Column("charger_type", sa.String(length=8), nullable=True),
    )
