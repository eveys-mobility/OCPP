"""WS-edge Basic Auth check (E5-6).

Per OCPP 1.6 Security Whitepaper Profile 1, chargers can authenticate
to the CSMS via HTTP Basic Auth on the WebSocket upgrade. Each
charger has its own password; CSMS stores `cp_id → password_hash`
and verifies on every connect.

This module is the gateway-side check: parse the Authorization
header, look up the charger's hash via the repository, bcrypt-verify.
The lookup is one Postgres round-trip per WS upgrade — well under
the upgrade's own latency budget. Bad creds get a 401 and never
reach the OCPP handler stack.

**Where this fits with mTLS (E5-5).** mTLS authenticates the
*Envoy ↔ gateway* leg — that nothing else inside the cluster can
impersonate Envoy. Basic Auth authenticates the *charger ↔ Envoy*
leg — that the entity claiming `cp_id=CP_ABC_001` actually knows
that charger's password. They're complementary, not redundant.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import TYPE_CHECKING

import bcrypt

from eveys_ocpp.observability import get_logger
from eveys_ocpp.persistence.repositories import get_credential_hash

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.settings import Settings

log = get_logger(__name__)

# Closed enum of outcomes — bounded label set on the Prometheus
# counter. Keep this list and the metric's known label values in
# sync.
OUTCOME_OK = "ok"
OUTCOME_NO_HEADER = "no_header"
OUTCOME_MALFORMED = "malformed"
OUTCOME_USERNAME_MISMATCH = "username_mismatch"
OUTCOME_NO_CREDENTIAL = "no_credential"
OUTCOME_BAD_PASSWORD = "bad_password"


@dataclass(frozen=True, slots=True)
class AuthResult:
    """Pair returned by `verify_basic_auth`. The metric counter
    increments on `outcome`; the `accepted` flag drives the WS
    upgrade's accept-or-reject decision."""

    accepted: bool
    outcome: str


def _parse_basic_header(header: str | None) -> tuple[str, str] | None:
    """Decode `Authorization: Basic ...`. Returns `(username, password)`
    or `None` if the header is missing or malformed.

    Tolerant on the header — broken / non-Basic / non-base64 inputs
    all collapse to `None` so the caller's branch is "got creds vs
    didn't" rather than dealing with three separate parse failures.
    """
    if not header or not header.lower().startswith("basic "):
        return None
    payload = header[6:].strip()
    try:
        decoded = base64.b64decode(payload, validate=True).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return None
    if ":" not in decoded:
        return None
    username, _, password = decoded.partition(":")
    return username, password


async def verify_basic_auth(
    *,
    cp_id: str,
    auth_header: str | None,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> AuthResult:
    """Verify the inbound WS upgrade's Basic Auth credentials.

    Returns an `AuthResult`:

    - `accepted=True` when the password matches the stored hash, or
      when `ws_basic_auth_required=False` and no credential is
      provisioned (permissive default — lets a fleet migrate
      gradually).
    - `accepted=False` for every failure mode (no header, bad
      header shape, username mismatch, missing credential under
      strict mode, or wrong password).

    The OCPP Basic Auth username is conventionally the cp_id; we
    enforce that match so a charger holding `CP_A`'s password can't
    connect as `CP_B`.

    bcrypt's verify is constant-time on matching-length inputs.
    For the username comparison we use Python's `==` which is
    *not* constant-time — that's fine here because the username
    is in the URL path (the gateway's WS routing already exposes
    it), so timing the check leaks nothing the attacker doesn't
    already see.
    """
    parsed = _parse_basic_header(auth_header)
    if parsed is None:
        outcome = OUTCOME_NO_HEADER if not auth_header else OUTCOME_MALFORMED
        return AuthResult(accepted=False, outcome=outcome)

    username, password = parsed
    if username != cp_id:
        return AuthResult(accepted=False, outcome=OUTCOME_USERNAME_MISMATCH)

    async with session_factory() as session:
        stored_hash = await get_credential_hash(session, cp_id=cp_id)

    if stored_hash is None:
        # No credential row. Permissive default lets unprovisioned
        # chargers connect during a fleet migration; strict mode
        # rejects them. Production sets strict via Helm.
        if settings.ws_basic_auth_required:
            return AuthResult(accepted=False, outcome=OUTCOME_NO_CREDENTIAL)
        return AuthResult(accepted=True, outcome=OUTCOME_OK)

    try:
        ok = bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8"))
    except ValueError:
        # Malformed hash in the DB — treat as a non-match. Logs at
        # warning so an operator notices a corrupted credential row.
        log.warning("basic_auth.malformed_stored_hash", cp_id=cp_id)
        return AuthResult(accepted=False, outcome=OUTCOME_BAD_PASSWORD)

    if not ok:
        return AuthResult(accepted=False, outcome=OUTCOME_BAD_PASSWORD)

    return AuthResult(accepted=True, outcome=OUTCOME_OK)


def hash_password(plaintext: str) -> str:
    """Convenience helper for tests + future operator tooling.

    Production credential rows can be inserted via plain SQL with a
    bcrypt hash precomputed by any standard tool; this helper
    exists so tests don't shell out to `htpasswd` and so the
    forthcoming credential-rotation REST endpoint has one place to
    call.
    """
    return bcrypt.hashpw(plaintext.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
