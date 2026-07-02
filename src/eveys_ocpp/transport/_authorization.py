"""WS-edge device-authorization check.

Runs after the Basic Auth gate. Two branches:

- The `cp_id` is already a row in `charge_points` → the operator has
  authorized it (or it's the grandfathered pre-migration fleet); accept
  the upgrade, connection runs normally, **IP rate limit is bypassed**
  so a legitimate NAT'd fleet reconnecting rapidly never trips the ban.
- The `cp_id` is not in `charge_points` → apply the IP rate limit
  first (unknown devices are the only surface where a scanner or
  abuser can burn our budget). If the IP is allowed, add the device to
  the Redis pending list (`cp:pending:{cp_id}`) with the pending TTL
  and accept the upgrade **flagged as pending** — the handler layer
  reads that flag and returns CALLERROR for every inbound OCPP call
  except BootNotification (which is Accepted but cached in Redis, not
  written to Postgres) until an operator posts
  `/authorizations/{cp_id}/authorize`.

The pending row's TTL is what removes stale entries — no sweeper
needed. On operator authorize, the row is moved from Redis into
`charge_points`; from that point the device follows the authorized
branch on every subsequent upgrade.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import get_logger
from eveys_ocpp.persistence.repositories import get_charge_point_pk

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.pending_authorizations import PendingAuthorizations
    from eveys_ocpp.transport._ip_rate_limiter import IpRateLimiter

log = get_logger(__name__)


# Closed enum of outcomes — bounded label set on the metric counter.
OUTCOME_AUTHORIZED = "authorized"
OUTCOME_PENDING_NEW = "pending_new"
OUTCOME_PENDING_REFRESHED = "pending_refreshed"
OUTCOME_IP_BLOCKED = "ip_blocked"
OUTCOME_DB_ERROR = "db_error"
OUTCOME_REDIS_ERROR = "redis_error"


@dataclass(frozen=True, slots=True)
class AuthorizationCheck:
    """Result of `check_and_record_authorization`.

    - `accepted=True, is_pending=False` → authorized fleet member;
      connection runs normally.
    - `accepted=True, is_pending=True`  → unknown device on the
      operator's pending queue; upgrade accepted so the operator can
      see it in the UI, but handlers gate every DB write on this flag.
    - `accepted=False`                  → reject the WS upgrade (401 or
      429, depending on `outcome`).
    """

    accepted: bool
    is_pending: bool
    outcome: str


async def check_and_record_authorization(
    *,
    cp_id: str,
    peer_ip: str | None,
    user_agent: str | None,
    session_factory: async_sessionmaker[AsyncSession],
    pending_store: PendingAuthorizations,
    ip_rate_limiter: IpRateLimiter | None,
    now: datetime,
) -> AuthorizationCheck:
    """Decide whether `cp_id` may connect.

    One indexed Postgres round-trip on the authorized path; one Redis
    EXISTS + one Redis SET on the pending path; plus one Redis
    INCR/EXISTS on the IP rate-limit hop. All well within the
    upgrade's latency budget.

    Fails closed on Postgres error (the authorized decision needs the
    DB to be truthful). Fails closed on Redis error only for the
    pending write — an IP rate-limit Redis error is fail-open (see
    `IpRateLimiter.check`)."""
    try:
        async with session_factory() as session:
            existing_pk = await get_charge_point_pk(session, cp_id=cp_id)
    except Exception as exc:
        log.warning(
            "authorization.db_error",
            cp_id=cp_id,
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return AuthorizationCheck(accepted=False, is_pending=False, outcome=OUTCOME_DB_ERROR)

    if existing_pk is not None:
        # Authorized fleet member. Bypass IP rate limit per the fleet
        # design — legitimate reconnects, however aggressive, are not
        # abuse.
        return AuthorizationCheck(accepted=True, is_pending=False, outcome=OUTCOME_AUTHORIZED)

    # Unknown cp_id. Apply the IP rate limit before touching Redis so a
    # banned IP can't keep refreshing pending TTLs.
    if ip_rate_limiter is not None:
        decision = await ip_rate_limiter.check(peer_ip)
        metrics_registry.WS_IP_RATE_LIMIT_TOTAL.labels(outcome=decision.outcome).inc()
        if not decision.allowed:
            log.info(
                "ws.ip_rate_limit_denied",
                cp_id=cp_id,
                peer_ip=peer_ip,
                outcome=decision.outcome,
            )
            return AuthorizationCheck(accepted=False, is_pending=False, outcome=OUTCOME_IP_BLOCKED)

    # Add to Redis pending. Upsert returns the row so we can classify
    # `pending_new` vs `pending_refreshed` off the attempt counter,
    # which is what the operator sees in the pending queue.
    try:
        row = await pending_store.upsert(
            cp_id=cp_id,
            peer_ip=peer_ip,
            user_agent=user_agent,
            now=now,
        )
    except Exception as exc:
        log.warning(
            "authorization.redis_error",
            cp_id=cp_id,
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return AuthorizationCheck(accepted=False, is_pending=False, outcome=OUTCOME_REDIS_ERROR)

    outcome = OUTCOME_PENDING_NEW if int(row.get("attempts", 1)) == 1 else OUTCOME_PENDING_REFRESHED
    return AuthorizationCheck(accepted=True, is_pending=True, outcome=outcome)
