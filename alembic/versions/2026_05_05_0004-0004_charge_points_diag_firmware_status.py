"""Add last_diagnostics_status / last_firmware_status to charge_points (E2-1F).

Revision ID: 0004
Revises: 0003
Create Date: 2026-05-05

Two latest-wins columns updated by the inbound
``DiagnosticsStatusNotification`` and ``FirmwareStatusNotification``
handlers. Both nullable so existing charger rows don't need backfill;
they'll populate on the next status notification from each charger.
Per-state history goes through structured logs only — no Kafka
envelope evolution (ADR-0015 keeps the firehose at five payload
variants for now).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("charge_points", sa.Column("last_diagnostics_status", sa.String(32)))
    op.add_column("charge_points", sa.Column("last_firmware_status", sa.String(32)))


def downgrade() -> None:
    op.drop_column("charge_points", "last_firmware_status")
    op.drop_column("charge_points", "last_diagnostics_status")
