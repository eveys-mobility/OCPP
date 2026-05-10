"""pending_certificate_signings: add operator-review columns (#189).

Revision ID: 0011
Revises: 0010
Create Date: 2026-05-11

Operator-queue slice of the deferred signing pipeline (#187).
Adds the bookkeeping needed for manual approve / reject:

- `approved_by` — opaque operator identifier (free-form string;
  whatever the auth layer in front of the REST endpoint surfaces).
  Nullable: only populated when a row transitions to `signed`.
- `rejected_at` / `rejected_reason` — populated when a row
  transitions to `rejected`.

The existing `signed_at` / `signed_chain` columns from 0010 are
reused unchanged on the approve path.

Status values now in scope:
- `pending` (server default, set on insert by the inbound handler)
- `signed` (operator approved + CertificateSigned.req dispatched)
- `rejected` (operator rejected; charger re-submits per spec)
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "pending_certificate_signings",
        sa.Column("approved_by", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "pending_certificate_signings",
        sa.Column("rejected_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Reason is free-form (operator note / CA error). 512 covers
    # typical X.509 validation messages with headroom.
    op.add_column(
        "pending_certificate_signings",
        sa.Column("rejected_reason", sa.String(length=512), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("pending_certificate_signings", "rejected_reason")
    op.drop_column("pending_certificate_signings", "rejected_at")
    op.drop_column("pending_certificate_signings", "approved_by")
