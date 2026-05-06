"""Cursor-based pagination helpers (ADR-0026 D5).

Cursors are opaque to the client: a base64-encoded JSON object that
the server emits and the client passes back unchanged on the next
page. The contract documents this as opaque, so the inner shape may
evolve.

Postgres-backed endpoints encode the last-seen surrogate PK:

    {"id": 12345}

ClickHouse-backed endpoints (E3-7 commit 4) will encode a
(timestamp, event_id) tuple to keep the keyset stable across rows
sharing a timestamp.

A malformed or non-decodable cursor is a client error → caller raises
`ApiError(status=400, code=BAD_REQUEST)`.
"""

from __future__ import annotations

import base64
import json
from typing import Any

from eveys_ocpp.api._errors import ERR_BAD_REQUEST, ApiError


def encode_cursor(payload: dict[str, Any]) -> str:
    """Serialize a cursor payload to a URL-safe opaque string.

    `sort_keys=True` keeps cursors deterministic for tests; clients
    must not rely on the byte shape."""
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_cursor(cursor: str | None) -> dict[str, Any] | None:
    """Deserialize a cursor; `None` for "no cursor (first page)".

    Raises `ApiError` on any decode/parse failure — that's a client
    bug, never a server bug, since the only caller of `decode_cursor`
    receives a value the server emitted on a prior page."""
    if cursor is None or cursor == "":
        return None
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii"))
        payload = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"malformed cursor: {exc.__class__.__name__}",
        ) from exc
    if not isinstance(payload, dict):
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="malformed cursor: not a JSON object",
        )
    return payload


def clamp_limit(value: int | None, *, default: int, maximum: int) -> int:
    """Clamp the user-supplied `limit` query parameter.

    `None` → `default`. Values below 1 are an error (FastAPI's `Query
    ge=1` already rejects them, but defense in depth). Values above
    `maximum` are silently clamped — the contract caps `limit` at 500
    by default, so the client should never request more, but if they
    do we honour the cap rather than 400ing."""
    if value is None:
        return default
    if value < 1:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="limit must be >= 1",
        )
    return min(value, maximum)
