"""User authentication core (issue #84 PR-A).

The single import surface for the user-system auth primitives:

- `AuthIdentity` — who the request is from (superadmin, regular
  user, service-token caller). Attached to `request.state.identity`
  by the middleware so downstream routes can read it without a
  second DB lookup.
- `verify_password` / `hash_password` — bcrypt helpers. Same shape
  as `transport/_basic_auth.hash_password` (E5-6) for the WS edge,
  but kept as a sibling rather than imported because the two
  concerns are different (charger creds vs human creds) and
  diverging in the future would mean the wrong reach-around.
- `issue_token` / `lookup_token` / `revoke_token` — opaque tokens
  in Redis with a TTL. Per ADR-discussion, opaque + Redis was
  chosen over JWT because revocation is trivial (`DEL`) and the
  fleet doesn't need stateless verification.
- `authenticate_login` — the username+password → AuthIdentity flow
  used by the login endpoint. Handles the env-superadmin and the
  DB-user paths in one place.

Why a sibling package rather than an `api/_auth.py` extension:
the user-system is bigger than the existing static-bearer-token
middleware and pairs with PR-B (admin endpoints) and PR-C
(charger filtering). A dedicated package keeps the surface
discoverable and gives PR-B / PR-C an obvious place to land.
"""

from __future__ import annotations

from eveys_ocpp.auth._core import (
    AuthIdentity,
    authenticate_login,
    hash_password,
    issue_token,
    lookup_token,
    revoke_token,
    verify_password,
)

__all__ = [
    "AuthIdentity",
    "authenticate_login",
    "hash_password",
    "issue_token",
    "lookup_token",
    "revoke_token",
    "verify_password",
]
