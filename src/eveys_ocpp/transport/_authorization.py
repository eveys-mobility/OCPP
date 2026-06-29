"""WS-edge device-authorization check (#0013).

Runs after the Basic Auth gate. Looks up the charger's row in
``charge_point_authorizations`` and decides:

- **approved** → accept the upgrade
- **pending** within the grace window → accept the upgrade and tell
  the caller to schedule a force-disconnect at the deadline. The
  operator console gets a polled refresh that shows the new pending
  row; if they click Approve before the timer fires, the connection
  stays up.
- **pending** past the grace window → reject (401)
- **rejected** / **revoked** → reject (401)
- **first sighting** (no row at all) → create a pending row, treat as
  "pending, fresh deadline" and accept.

Why a `pending` row sticks around even after the grace window expires:
the operator might come back hours later and click Approve — the row
records the request so they can decide retrospectively. The expired
flag lives in `requested_at` vs. now; the row's `status` stays
`pending` until an operator decision lands.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from eveys_ocpp.observability import get_logger
from eveys_ocpp.persistence.models import (
    AUTH_STATUS_APPROVED,
    AUTH_STATUS_PENDING,
    AUTH_STATUS_REJECTED,
    AUTH_STATUS_REVOKED,
)
from eveys_ocpp.persistence.repositories import (
    get_authorization,
    record_authorization_attempt,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


# Closed enum of outcomes — bounded label set on the metric counter.
OUTCOME_APPROVED = "approved"
OUTCOME_PENDING_NEW = "pending_new"
OUTCOME_PENDING_WITHIN_WINDOW = "pending_within_window"
OUTCOME_PENDING_EXPIRED = "pending_expired"
OUTCOME_REJECTED = "rejected"
OUTCOME_REVOKED = "revoked"
# DB lookup or upsert failed — fail closed (reject the upgrade) and
# log loud. Same posture as the Basic Auth gate.
OUTCOME_DB_ERROR = "db_error"


@dataclass(frozen=True, slots=True)
class AuthorizationCheck:
    """Result of `check_and_record_authorization`.

    - `accepted=True, pending_deadline=None` → connect normally.
    - `accepted=True, pending_deadline=<dt>` → connect, but the caller
      schedules a force-close at `pending_deadline`.
    - `accepted=False` → reject the WS upgrade.
    """

    accepted: bool
    outcome: str
    pending_deadline: datetime | None = None


async def check_and_record_authorization(
    *,
    cp_id: str,
    peer_ip: str | None,
    user_agent: str | None,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    now: datetime,
) -> AuthorizationCheck:
    """Decide whether `cp_id` may connect, and update the authorization
    row's `last_attempt_*` columns.

    Every WS upgrade hits this function once (after Basic Auth). It's
    one or two indexed Postgres round-trips per upgrade — well within
    the upgrade's latency budget.

    The function commits its own session so the `last_attempt_*`
    write lands even if the upgrade is rejected (otherwise an operator
    looking at the pending row would have no idea the charger was
    still trying to connect).
    """
    grace = timedelta(seconds=settings.auth_pending_grace_seconds)

    try:
        async with session_factory() as session:
            existing = await get_authorization(session, cp_id=cp_id)
            if existing is None:
                # First-ever sighting. Create the charge_points + pending
                # rows so the operator can decide. Accept the connection
                # with a fresh grace deadline.
                row = await record_authorization_attempt(
                    session,
                    cp_id=cp_id,
                    peer_ip=peer_ip,
                    user_agent=user_agent,
                    now=now,
                )
                await session.commit()
                return AuthorizationCheck(
                    accepted=True,
                    outcome=OUTCOME_PENDING_NEW,
                    pending_deadline=row["requested_at"] + grace,
                )

            status = existing["status"]

            if status == AUTH_STATUS_APPROVED:
                # Still record the attempt so an audit query can prove
                # the charger is alive; commit even if the connection
                # later errors out.
                await record_authorization_attempt(
                    session,
                    cp_id=cp_id,
                    peer_ip=peer_ip,
                    user_agent=user_agent,
                    now=now,
                )
                await session.commit()
                return AuthorizationCheck(
                    accepted=True, outcome=OUTCOME_APPROVED, pending_deadline=None
                )

            if status == AUTH_STATUS_PENDING:
                # Refresh `last_attempt_*` so the operator sees the
                # most recent IP/UA. `record_authorization_attempt`
                # preserves the original `requested_at`, so the grace
                # window doesn't sliding-window itself.
                row = await record_authorization_attempt(
                    session,
                    cp_id=cp_id,
                    peer_ip=peer_ip,
                    user_agent=user_agent,
                    now=now,
                )
                await session.commit()
                deadline = row["requested_at"] + grace
                if now < deadline:
                    return AuthorizationCheck(
                        accepted=True,
                        outcome=OUTCOME_PENDING_WITHIN_WINDOW,
                        pending_deadline=deadline,
                    )
                return AuthorizationCheck(
                    accepted=False,
                    outcome=OUTCOME_PENDING_EXPIRED,
                    pending_deadline=None,
                )

            if status == AUTH_STATUS_REJECTED:
                await record_authorization_attempt(
                    session,
                    cp_id=cp_id,
                    peer_ip=peer_ip,
                    user_agent=user_agent,
                    now=now,
                )
                await session.commit()
                return AuthorizationCheck(
                    accepted=False, outcome=OUTCOME_REJECTED, pending_deadline=None
                )

            if status == AUTH_STATUS_REVOKED:
                await record_authorization_attempt(
                    session,
                    cp_id=cp_id,
                    peer_ip=peer_ip,
                    user_agent=user_agent,
                    now=now,
                )
                await session.commit()
                return AuthorizationCheck(
                    accepted=False, outcome=OUTCOME_REVOKED, pending_deadline=None
                )

            # Unknown status — shouldn't happen, but fail closed.
            log.warning("authorization.unknown_status", cp_id=cp_id, status=status)
            return AuthorizationCheck(
                accepted=False, outcome=OUTCOME_DB_ERROR, pending_deadline=None
            )

    except Exception as exc:
        # Fail closed on DB error — Postgres outage / missing table /
        # contention. Loud structured log so an operator notices.
        log.warning(
            "authorization.db_error",
            cp_id=cp_id,
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return AuthorizationCheck(accepted=False, outcome=OUTCOME_DB_ERROR, pending_deadline=None)
