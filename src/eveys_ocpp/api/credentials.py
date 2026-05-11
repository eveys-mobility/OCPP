"""`/api/v1/charge-points/{cp_id}/credentials` routes (#196, TC_073).

Operator surface for managing per-charger Basic Auth credentials —
the OCPP 1.6 Security Whitepaper Profile-1 password used on the WS
upgrade. Until this endpoint shipped, the only way to provision or
rotate a charger's password was direct SQL against
`charge_point_credentials`. That worked for pilot fleets but it puts
plaintext in shell history and gives no audit trail.

This module:

- accepts a plaintext password at the REST boundary,
- bcrypts it server-side so the plaintext never crosses the persistence layer,
- upserts into `charge_point_credentials`,
- emits a `cp.credential_rotated` Kafka envelope (audit-grade; never
  carries the password itself).

The DELETE path is idempotent — operators can call it twice safely;
the second call still returns 200 with `status=unprovisioned`. Choice
is deliberate: rotation workflows often do "delete then provision"
without wanting to error when the delete is a no-op.

Out of scope: bulk provisioning (one charger at a time), RBAC inside
the gateway, and gateway-side password generation. Operators supply
the plaintext.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Request

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.api._errors import (
    ERR_BAD_REQUEST,
    ERR_UNKNOWN_CP_ID,
    ApiError,
)
from eveys_ocpp.observability import get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import (
    delete_charge_point_credential,
    upsert_charge_point_credential,
)
from eveys_ocpp.transport._basic_auth import hash_password

router = APIRouter(tags=["credentials"])

log = get_logger(__name__)

_BASE = "/charge-points/{cp_id}/credentials"

# Bcrypt's own input limit is 72 bytes — anything beyond that is
# silently truncated. Refuse longer inputs at the boundary so the
# operator notices instead of getting a silent 72-byte password.
_MIN_PASSWORD_LEN = 12
_MAX_PASSWORD_LEN = 72


async def _body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception as exc:
        raise ApiError(
            status_code=400, error_code=ERR_BAD_REQUEST, message=f"invalid json: {exc}"
        ) from exc
    if not isinstance(body, dict):
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="body must be a JSON object",
        )
    return body


async def _emit_rotated(request: Request, *, cp_id: str, action: str, actor: str) -> None:
    """Best-effort `cp.credential_rotated` emit. Audit consumers
    tolerate drops because the source-of-truth row is in Postgres;
    a broker hiccup must not break the operator's call."""
    producer = getattr(request.app.state, "event_producer", None)
    settings = request.app.state.settings
    if producer is None:
        return
    envelope = events_pb2.EventEnvelope(
        event_id=str(uuid.uuid4()),
        occurred_at=datetime.now(UTC).isoformat(),
        cp_id=cp_id,
        schema_version="v1",
        cp_credential_rotated=events_pb2.CpCredentialRotated(
            action=action,
            actor=actor,
        ),
    )
    try:
        await producer.publish(
            topic=settings.kafka_topic_cp_credential_rotated,
            key=cp_id,
            value=envelope.SerializeToString(),
        )
    except Exception as exc:
        log.warning("credential_rotated.publish_failed", cp_id=cp_id, error=str(exc))


@router.put(_BASE)
async def set_credential_route(request: Request, cp_id: str) -> dict[str, Any]:
    """Upsert the charger's Basic Auth credential. Idempotent — calling
    this twice with the same password is a no-op at the bcrypt-verify
    level (different hashes both verify against the same plaintext)."""
    body = await _body(request)
    password = body.get("password")
    if not isinstance(password, str):
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="password is required",
        )
    if len(password) < _MIN_PASSWORD_LEN:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"password must be at least {_MIN_PASSWORD_LEN} characters",
        )
    if len(password.encode("utf-8")) > _MAX_PASSWORD_LEN:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=(
                f"password must be at most {_MAX_PASSWORD_LEN} bytes "
                "(bcrypt's own limit; longer values would be silently truncated)"
            ),
        )

    actor = body.get("actor")
    if actor is not None and not isinstance(actor, str):
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="actor must be a string when provided",
        )

    password_hash = hash_password(password)

    async with session_scope(request.app.state.session_factory) as session:
        ok = await upsert_charge_point_credential(session, cp_id=cp_id, password_hash=password_hash)

    if not ok:
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=f"unknown cp_id: {cp_id}",
        )

    log.info("credential.set", cp_id=cp_id, actor=actor or "")
    await _emit_rotated(request, cp_id=cp_id, action="set", actor=actor or "")

    return {
        "cp_id": cp_id,
        "status": "provisioned",
        "request_id": request.state.request_id,
    }


@router.delete(_BASE)
async def delete_credential_route(request: Request, cp_id: str) -> dict[str, Any]:
    """Drop the charger's credential. Idempotent: calling this on a
    charger with no credential row returns 200 with the same shape.

    The operator may supply `?actor=` as a query param for the audit
    event; the DELETE body is by convention empty for proxies that
    strip it."""
    actor = request.query_params.get("actor", "")

    async with session_scope(request.app.state.session_factory) as session:
        # First confirm the charger exists at all — otherwise an
        # unknown cp_id silently looks the same as "no credential row".
        from eveys_ocpp.persistence.repositories import get_charge_point_pk

        cp_pk = await get_charge_point_pk(session, cp_id=cp_id)
        if cp_pk is None:
            raise ApiError(
                status_code=404,
                error_code=ERR_UNKNOWN_CP_ID,
                message=f"unknown cp_id: {cp_id}",
            )
        deleted = await delete_charge_point_credential(session, cp_id=cp_id)

    log.info("credential.deleted", cp_id=cp_id, actor=actor, had_row=deleted)
    # Emit the event only when there actually was a row to remove;
    # spurious events on a no-op delete would pollute the audit log.
    if deleted:
        await _emit_rotated(request, cp_id=cp_id, action="removed", actor=actor)

    return {
        "cp_id": cp_id,
        "status": "unprovisioned",
        "request_id": request.state.request_id,
    }
