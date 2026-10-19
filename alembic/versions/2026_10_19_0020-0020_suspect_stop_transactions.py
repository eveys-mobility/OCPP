"""StopTransaction frames held back because they are stamped before the start.

Revision ID: 0020
Revises: 0019
Append-only evidence. Downgrade drops the evidence rows; transactions are untouched.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0020"
down_revision: str | None = "0019"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # The FK takes a lock on transactions that waits behind an open Start
    # admission, and Start/Stop writes then queue behind this migration.
    # Fail fast instead; the migration is a no-op to re-run.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.create_table(
        "suspect_stop_transactions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "transaction_pk",
            sa.BigInteger(),
            sa.ForeignKey("transactions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("cp_id", sa.String(64), nullable=False),
        sa.Column("transaction_id", sa.BigInteger(), nullable=False),
        sa.Column("id_tag", sa.String(64)),
        sa.Column("meter_stop_wh", sa.BigInteger(), nullable=False),
        sa.Column("stopped_reported_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stop_reason", sa.String(32)),
        sa.Column("failed_checks", sa.String(128), nullable=False),
    )
    op.create_index(
        "ix_suspect_stop_transactions_transaction_pk",
        "suspect_stop_transactions",
        ["transaction_pk"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_suspect_stop_transactions_transaction_pk", table_name="suspect_stop_transactions"
    )
    op.drop_table("suspect_stop_transactions")
