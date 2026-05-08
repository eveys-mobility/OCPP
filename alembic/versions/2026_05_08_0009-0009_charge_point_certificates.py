"""charge_point_certificates table (TC_075_1, TC_075_2, TC_076).

Revision ID: 0009
Revises: 0008
Create Date: 2026-05-08

Mirror of root certificates installed on a charger via OCPP 1.6
Security Whitepaper §4.5 InstallCertificate. The charger is the
source of truth (per the same convention as `local_auth_lists`,
E2-1B); this table exists so the operator UI can answer "what
certs does this charger have?" without polling.

`sha256_hash` is the OCPP §5.1 `hash_data` identifier — computed
at install-time and recorded so a future DeleteCertificate-by-hash
can find the row without the operator re-sending the original PEM.
The hash is over the DER-encoded `tbsCertificate.subjectPublicKey`
per the spec; we compute it via `_cert_hash.compute_sha256_hash`
at the gRPC boundary, NOT at the DB layer.

Unique constraint on `(charge_point_id, sha256_hash)` — installing
the same cert twice on the same charger is idempotent (charger
acks Accepted both times; we keep one row).

The full `pem` lives in the row for audit / re-export. Cert sizes
range 1-4 KB; Postgres TEXT handles that without padding waste.
The `certificate_type` is the closed OCPP enum (CentralSystemRoot
or ManufacturerRoot); a charger may have multiple of each.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "charge_point_certificates",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column(
            "charge_point_id",
            sa.BigInteger(),
            sa.ForeignKey("charge_points.id", ondelete="CASCADE"),
            nullable=False,
        ),
        # OCPP 1.6 enum stored as the spec's name string. 32 chars
        # covers `CentralSystemRootCertificate` (28) and
        # `ManufacturerRootCertificate` (27) with headroom.
        sa.Column("certificate_type", sa.String(length=32), nullable=False),
        # SHA-256 hex digest, 64 chars. The OCPP §5.1 `hash_data`
        # identifier; the load-bearing column for DeleteCertificate
        # lookups.
        sa.Column("sha256_hash", sa.String(length=64), nullable=False),
        # Full PEM as supplied by the operator. Sized loosely — typical
        # is 1-4 KB; TEXT handles outliers.
        sa.Column("pem", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        # Re-installing the same cert on the same charger is a no-op
        # mirror-side. The unique constraint also makes the
        # delete-by-hash query fully indexed.
        sa.UniqueConstraint(
            "charge_point_id", "sha256_hash", name="uq_charge_point_certificates_cp_hash"
        ),
    )
    # Operator UI: "what certs of type X does this charger have?".
    op.create_index(
        "ix_charge_point_certificates_cp_type",
        "charge_point_certificates",
        ["charge_point_id", "certificate_type"],
    )


def downgrade() -> None:
    op.drop_index("ix_charge_point_certificates_cp_type", table_name="charge_point_certificates")
    op.drop_table("charge_point_certificates")
