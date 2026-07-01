"""webhook_delivery_backlog.

Revision ID: 0016
Revises: 0015
Create Date: 2026-07-01

Durable buffer for webhook events the in-loop dispatcher couldn't
deliver within its ~12.6 min retry budget (five attempts: 1 s, 5 s,
30 s, 120 s, 600 s). Without this table, a backend outage longer
than 12 min silently drops every event in the outage window; with
it, the events sit here until a background drainer (``webhooks/
backlog_drainer.py``) either delivers them or ages them out at the
configured retention window (default 7 days).

Design choices:

* ``event_id`` is UNIQUE — the X-Eveys-Event-Id header on the
  original delivery attempt is the same value we store here, so a
  double-enqueue (a bug in the dispatcher's failure path) can't
  produce a double-deliver.
* ``body`` is BYTEA rather than JSONB because we already carry the
  pre-encoded JSON body from the dispatcher — re-encoding on drain
  would risk producing a byte stream different from what was signed.
  Re-using the byte-identical body preserves the ``signature`` we
  computed alongside it.
* ``signature`` is stored (not recomputed on drain) for the same
  reason. HMAC over the encoded body must match what the backend
  verifies; keep the signing boundary at enqueue time.
* Partial index on ``(next_attempt_at) WHERE NOT dead`` keeps the
  drainer's hot query — ``WHERE next_attempt_at <= now() AND NOT
  dead`` — fast without indexing rows the drainer will never look
  at again.
* Timestamps are TIMESTAMPTZ (matches the rest of the schema per
  models.py convention).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0016"
down_revision: str | None = "0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "webhook_delivery_backlog",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            primary_key=True,
        ),
        sa.Column(
            "event_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            unique=True,
        ),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("body", postgresql.BYTEA(), nullable=False),
        sa.Column("signature", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "attempts",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "dead",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )
    # Partial index: the drainer's dominant query is
    # ``WHERE next_attempt_at <= now() AND NOT dead ORDER BY
    # next_attempt_at``. Excluding ``dead=true`` rows keeps the
    # index tight — dead rows accumulate until an operator either
    # purges them or resurrects them with ``dead=false,
    # next_attempt_at=now()``.
    op.create_index(
        "ix_webhook_backlog_ready",
        "webhook_delivery_backlog",
        ["next_attempt_at"],
        postgresql_where=sa.text("NOT dead"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_webhook_backlog_ready",
        table_name="webhook_delivery_backlog",
    )
    op.drop_table("webhook_delivery_backlog")
