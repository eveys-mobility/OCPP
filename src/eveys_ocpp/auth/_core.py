"""Implementation of the auth core. Re-exported from the package
init; routes / middleware / tests import from `eveys_ocpp.auth`."""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Literal

import bcrypt
from sqlalchemy import select

from eveys_ocpp.observability import get_logger
from eveys_ocpp.persistence.models import User

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from sqlalchemy.ext.asyncio import AsyncSession

    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


# Identity shape. `kind` is a closed enum so route logic can branch
# without string-matching: `user` for DB-backed users (PR-B manages
# them); `superadmin` for the env-only bootstrap identity;
# `service` for the existing static `rest_inbound_tokens` callers
# (eveys-backend) that bypass per-user filtering.
IdentityKind = Literal["superadmin", "user", "service"]


@dataclass(frozen=True, slots=True)
class AuthIdentity:
    """Who the request is from.

    The middleware attaches one of these to `request.state.identity`.
    PR-C's charger filtering reads `kind` and `user_id`: superadmin
    and service callers get unfiltered results; users get
    `WHERE id IN (SELECT charge_point_id FROM user_charge_points
                  WHERE user_id = :user_id)`.
    """

    kind: IdentityKind
    username: str
    # Populated only for `kind == "user"`. None for superadmin /
    # service — those have no DB row.
    user_id: int | None = None


# ---- bcrypt helpers --------------------------------------------------------


def hash_password(plaintext: str) -> str:
    """bcrypt-hash a plaintext password. Returns the encoded string
    suitable for storage (`$2b$12$...`). Used by PR-B's user-create
    endpoint, the metadata for the env-superadmin (operator
    pre-computes), and tests.
    """
    return bcrypt.hashpw(plaintext.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plaintext: str, stored_hash: str) -> bool:
    """Constant-time bcrypt compare. Returns False on a malformed
    hash rather than raising — a corrupted DB row shouldn't crash
    the login path."""
    try:
        return bcrypt.checkpw(plaintext.encode("utf-8"), stored_hash.encode("utf-8"))
    except (ValueError, TypeError):
        return False


# ---- Token store (Redis-backed) -------------------------------------------
#
# Opaque tokens, not JWT. Each issuance writes a key
# `auth:token:{token}` whose value is the JSON-encoded
# `AuthIdentity`-equivalent dict, with a TTL set to
# `Settings.auth_token_ttl_seconds`.
#
# Why Redis: the fleet already runs Redis (registry, idempotency,
# rate-limit, authorize cache); adding token state is one more key
# pattern in the same instance. Redis blip on lookup → fail closed
# (login required) — the auth gate is not the place to fail open.


def _token_key(token: str) -> str:
    return f"auth:token:{token}"


def _new_opaque_token() -> str:
    """64 chars of base64url-encoded entropy. ~48 bytes of randomness
    — comfortably above brute-force range for a Redis-keyed lookup."""
    return secrets.token_urlsafe(48)


async def issue_token(
    redis: Redis,
    *,
    identity: AuthIdentity,
    ttl_seconds: int,
) -> tuple[str, datetime]:
    """Mint a new token for `identity`, write to Redis with TTL.
    Returns `(token, expires_at)` so the caller can echo both back
    to the client.
    """
    token = _new_opaque_token()
    payload = {
        "kind": identity.kind,
        "username": identity.username,
        "user_id": identity.user_id,
    }
    expires_at = datetime.now(UTC) + timedelta(seconds=ttl_seconds)
    await redis.set(_token_key(token), json.dumps(payload), ex=ttl_seconds)
    log.info(
        "auth.token_issued",
        kind=identity.kind,
        username=identity.username,
        ttl_seconds=ttl_seconds,
    )
    return token, expires_at


async def lookup_token(redis: Redis, *, token: str) -> AuthIdentity | None:
    """Look up an opaque token. Returns the identity on hit, None on
    miss (token unknown, expired, or revoked). The middleware uses
    this on every request that carries a non-static bearer.

    Defensive on parse failures: a malformed payload (e.g. a stray
    Redis key from a prior schema) returns None rather than raising,
    so a bad row in Redis can't 500 the auth gate.
    """
    try:
        raw = await redis.get(_token_key(token))
    except Exception as exc:
        # Fail closed on Redis errors — the auth gate is the wrong
        # place to fail open. Loud-warn so an alert can fire on
        # sustained Redis trouble.
        log.warning("auth.token_lookup_redis_error", error=str(exc))
        return None
    if raw is None:
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        log.warning("auth.token_malformed", token_prefix=token[:8])
        return None
    if not isinstance(payload, dict) or payload.get("kind") not in (
        "superadmin",
        "user",
        "service",
    ):
        return None
    return AuthIdentity(
        kind=payload["kind"],
        username=str(payload.get("username", "")),
        user_id=payload.get("user_id"),
    )


async def revoke_token(redis: Redis, *, token: str) -> bool:
    """Delete a token. Returns True if it was present, False if not.
    Used by the logout endpoint and by PR-B's "kill all sessions"
    when expiring a user."""
    try:
        deleted = await redis.delete(_token_key(token))
    except Exception as exc:
        log.warning("auth.token_revoke_redis_error", error=str(exc))
        return False
    return bool(int(deleted))


# ---- Login flow -----------------------------------------------------------


async def authenticate_login(
    *,
    username: str,
    password: str,
    session: AsyncSession,
    settings: Settings,
) -> AuthIdentity | None:
    """Resolve a username + password to an AuthIdentity, or None on
    bad creds / expired user.

    Two paths:

    1. **Superadmin**: matches `settings.superadmin_username`. The
       hash comes from `settings.superadmin_password_hash` (env-
       sourced bcrypt hash). No DB row.
    2. **Regular user**: looks up `users.username`. Rejects if
       `expires_at` is set and in the past. Hash comes from
       `users.password_hash`.

    On any failure (unknown username, wrong password, expired user,
    superadmin enabled but hash empty, etc.) returns None. Caller
    surfaces that as a 401; the *reason* is in the structured log
    line, not the wire response — login responses are deliberately
    indistinguishable across failure modes (timing-sensitive
    enumeration mitigation).
    """
    # Superadmin first — env-only path, no DB hit.
    if username and username == settings.superadmin_username:
        stored = settings.superadmin_password_hash.get_secret_value()
        if not stored:
            log.warning(
                "auth.superadmin_login_attempt_with_empty_hash",
                username=username,
            )
            return None
        if not verify_password(password, stored):
            log.info("auth.login_failed", username=username, reason="bad_password")
            return None
        return AuthIdentity(kind="superadmin", username=username, user_id=None)

    # Regular user path.
    stmt = select(User).where(User.username == username)
    result = await session.execute(stmt)
    user = result.scalar_one_or_none()
    if user is None:
        log.info("auth.login_failed", username=username, reason="unknown_user")
        return None

    if user.expires_at is not None and user.expires_at <= datetime.now(UTC):
        log.info("auth.login_failed", username=username, reason="expired")
        return None

    if not verify_password(password, user.password_hash):
        log.info("auth.login_failed", username=username, reason="bad_password")
        return None

    return AuthIdentity(kind="user", username=username, user_id=int(user.id))
