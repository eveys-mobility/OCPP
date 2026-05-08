"""security_events table (Phase 5 / TC_077, TC_078).

Revision ID: 0007
Revises: 0006
Create Date: 2026-05-08

Per-charger security event log from OCPP 1.6 Security Whitepaper §4
SecurityEventNotification. **Append-only log**, NOT latest-wins —
events are audit-grade and we keep every one. The 18 spec-defined
event types (FirmwareUpdated, InvalidFirmwareSignature,
InvalidSecurityEventCertificate, ...) are stored as the charger-
reported string; the column width allows for future spec additions
or vendor extensions without a migration.

`reported_at` is the charger's claimed timestamp; `received_at` is
our server-receive time. The charger's clock is untrusted (AGENTS
rule 7), so operator alerting / dedup MUST anchor on received_at.

Index on (charge_point_id, received_at DESC) supports the natural
operator query: "what security events did this charger emit, most
recent first?".
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
        "security_events",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column(
            "charge_point_id",
            sa.BigInteger(),
            sa.ForeignKey("charge_points.id", ondelete="CASCADE"),
            nullable=False,
        ),
        # OCPP 1.6 Security Whitepaper §4 enumerates 18 event-type
        # names; longest is `InvalidSecurityEventCertificate` at 31
        # chars. 64 leaves room for vendor extensions and future
        # spec additions.
        sa.Column("event_type", sa.String(length=64), nullable=False),
        # Charger-claimed timestamp (untrusted; AGENTS rule 7).
        sa.Column("reported_at", sa.DateTime(timezone=True), nullable=False),
        # Server-receive time. The trustworthy ordering anchor.
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        # Optional vendor-specific context. The OCPP spec calls this
        # `techInfo`; we keep snake_case. NULL distinct from empty
        # string because the spec field is optional.
        sa.Column("tech_info", sa.Text(), nullable=True),
    )
    # Ops query: "show me the last N events for charger X, newest
    # first". Postgres can walk this index forward to satisfy the
    # ORDER BY received_at DESC LIMIT N pattern.
    op.create_index(
        "ix_security_events_cp_received",
        "security_events",
        ["charge_point_id", sa.text("received_at DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_security_events_cp_received", table_name="security_events")
    op.drop_table("security_events")
