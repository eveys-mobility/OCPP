"""charge_points.ocpp_version.

Revision ID: 0012
Revises: 0011
Create Date: 2026-05-12

Records which OCPP subprotocol the charger negotiated on its
WebSocket handshake. Today the gateway only accepts ``ocpp1.6`` so
the column is effectively constant — but exposing it now lets the
Console show "OCPP 1.6" on the charger detail page without guessing,
and gives us the seam to flip to ``ocpp2.0.1`` per-row when the 2.0.1
profile lands.

Set by the BootNotification handler (which runs after the WS
upgrade, so the subprotocol is known). Backfilled to ``ocpp1.6`` on
upgrade for the existing rows — that's the only protocol any of them
have ever spoken.

``String(16)`` fits both 1.6 and 2.0.1 plus headroom for future
suffixes.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "charge_points",
        sa.Column("ocpp_version", sa.String(length=16), nullable=True),
    )
    # Backfill: every row already in the DB was created by a charger
    # that connected on ocpp1.6 (the only subprotocol the gateway has
    # ever accepted). New rows get the value written by the
    # BootNotification handler.
    op.execute("UPDATE charge_points SET ocpp_version = 'ocpp1.6' WHERE ocpp_version IS NULL")


def downgrade() -> None:
    op.drop_column("charge_points", "ocpp_version")
