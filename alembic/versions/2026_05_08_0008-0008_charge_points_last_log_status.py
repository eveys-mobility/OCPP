"""charge_points.last_log_status (TC_079).

Revision ID: 0008
Revises: 0007
Create Date: 2026-05-08

Latest-wins column on `charge_points` for the
`LogStatusNotification` upload state (Idle / Uploading / Uploaded /
UploadFailure / BadMessage / NotSupportedOperation /
PermissionDenied per OCPP 1.6 Security Whitepaper §4.6).

Same shape as `last_diagnostics_status` / `last_firmware_status`:
operator dashboards read it to answer "did the log upload finish?"
without re-querying the charger. Per-event audit history is on the
Kafka topic, not here — for security-log audit trails specifically,
SIEM consumers tail `cp.security_event` (PR #109) for the events
themselves; the upload-progress is operator-facing.

`String(32)` matches the convention; longest spec value
(`NotSupportedOperation`) is 22 chars.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "charge_points",
        sa.Column("last_log_status", sa.String(length=32), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("charge_points", "last_log_status")
