"""Atomic physical Stop event ledger; requires migration before new code rollout.

Revision ID: 0019
Revises: 0018
SET NULL preserves event evidence when operator revoke cascades transactions. Downgrade
deletes pending delivery evidence and requires an explicit drained review.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0019"
down_revision: str | None = "0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "transaction_event_outbox",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "transaction_pk",
            sa.BigInteger(),
            sa.ForeignKey("transactions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("topic", sa.String(255), nullable=False),
        sa.Column("cp_id", sa.String(64), nullable=False),
        sa.Column("body", postgresql.BYTEA(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("last_error_class", sa.String(128)),
        sa.UniqueConstraint("transaction_pk", "topic", name="uq_transaction_event_outbox_tx_topic"),
    )
    op.create_index(
        "ix_transaction_event_outbox_ready",
        "transaction_event_outbox",
        ["next_attempt_at", "created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_transaction_event_outbox_ready", table_name="transaction_event_outbox")
    op.drop_table("transaction_event_outbox")
