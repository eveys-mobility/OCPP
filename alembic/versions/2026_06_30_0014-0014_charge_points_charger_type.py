"""charge_points.charger_type.

Revision ID: 0014
Revises: 0013
Create Date: 2026-06-30

Records whether a charger is AC or DC. The boot-handler's post-boot
ChangeConfiguration push uses this to pick the right measurand list
(DC sites carry ``SoC`` in the meter-value lists; AC sites carry
``Current.Export`` instead).

Nullable so existing rows aren't forced to guess. The boot handler
treats ``NULL`` as ``ac`` (the more conservative default — DC adds
the SoC measurand which an AC charger will reject; AC measurands
are universal). Operators set the value explicitly via the Console's
OCPP-config page or via the per-charger detail page.

``String(8)`` fits ``'ac'`` / ``'dc'`` with room for future
classifications without forcing another migration.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "charge_points",
        sa.Column("charger_type", sa.String(length=8), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("charge_points", "charger_type")
