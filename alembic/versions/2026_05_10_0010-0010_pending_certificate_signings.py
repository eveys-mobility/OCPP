"""pending_certificate_signings table (#186).

Revision ID: 0010
Revises: 0009
Create Date: 2026-05-10

Inbound side of OCPP 1.6 Security Whitepaper §4.13 SignCertificate.
The charger sends us a CSR; we persist it here for operator review
and emit a `cp.csr_submitted` Kafka event. The actual signing
pipeline (which CA, who approves, how the signed chain is delivered
back via CertificateSigned.req) is deferred to a separate design —
see #187.

Until that follow-up lands, this table accumulates pending rows;
chargers that don't get a CertificateSigned reply re-submit per
spec, so the table also serves as a charger-retry dedup buffer.

`status` is a free-form string — `pending` on insert, future values
(`approved`, `rejected`, `signed`) come from the follow-up. String
keeps the schema forward-compatible without a migration each time
we add a state.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pending_certificate_signings",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column(
            "charge_point_id",
            sa.BigInteger(),
            sa.ForeignKey("charge_points.id", ondelete="CASCADE"),
            nullable=False,
        ),
        # PEM-encoded CSR as supplied by the charger. Sized loosely —
        # typical 1-2 KB; TEXT handles outliers.
        sa.Column("csr", sa.Text(), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        # `pending` on insert; follow-up adds `approved` / `rejected` /
        # `signed`. Kept as a 16-char string for forward-compat.
        sa.Column(
            "status",
            sa.String(length=16),
            server_default="pending",
            nullable=False,
        ),
        sa.Column("signed_at", sa.DateTime(timezone=True), nullable=True),
        # Signed chain (PEM bundle: leaf + intermediates) — populated
        # by the follow-up signing pipeline once the CA returns.
        sa.Column("signed_chain", sa.Text(), nullable=True),
    )
    # Operator UI: "what CSRs is this charger waiting on?". Also the
    # natural index for the follow-up queue worker that scans for
    # `pending` rows by charger.
    op.create_index(
        "ix_pending_certificate_signings_cp_status",
        "pending_certificate_signings",
        ["charge_point_id", "status"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_pending_certificate_signings_cp_status",
        table_name="pending_certificate_signings",
    )
    op.drop_table("pending_certificate_signings")
