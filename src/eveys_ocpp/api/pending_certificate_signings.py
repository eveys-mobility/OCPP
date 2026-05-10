"""`/api/v1/charge-points/{cp_id}/pending-certificate-signings/*` routes (#189).

Operator-queue slice of the deferred signing pipeline (#187). The
charger sent a CSR (OCPP 1.6 Security Whitepaper §4.13 SignCertificate);
the inbound handler persisted it into `pending_certificate_signings`
with status `pending`. These routes let an operator:

- list the pending rows (filterable by status),
- read one row (with the CSR text),
- approve a row by attaching a signed PEM chain — the gateway then
  dispatches `CertificateSigned.req` to the charger and returns the
  charger's reply,
- reject a row with a reason — no charger interaction (per spec, the
  charger re-submits if it cares).

Today the operator signs offline against whatever CA they want to use
(or hand-signs in pilot). When an automated CA service lands as a
follow-up under #187, it will call `/approve` with the same payload
shape — the dispatch path doesn't change.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Query, Request
from ocpp.v16 import call as ocpp_call

from eveys_ocpp.api._commands import dispatch_ocpp_call
from eveys_ocpp.api._errors import (
    ERR_BAD_REQUEST,
    ERR_UNKNOWN_CP_ID,
    ApiError,
)
from eveys_ocpp.api._pagination import clamp_limit, decode_cursor, encode_cursor
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import (
    get_pending_certificate_signing,
    list_pending_certificate_signings_by_cp,
    mark_pending_certificate_signing_rejected,
    mark_pending_certificate_signing_signed,
)

router = APIRouter(tags=["pending-certificate-signings"])

_BASE = "/charge-points/{cp_id}/pending-certificate-signings"

_ALLOWED_STATUSES: frozenset[str] = frozenset({"pending", "signed", "rejected"})


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _to_response(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "cp_id": row["cp_id"],
        "csr": row["csr"],
        "received_at": _isoformat(row["received_at"]),
        "status": row["status"],
        "signed_at": _isoformat(row["signed_at"]),
        "approved_by": row["approved_by"],
        "rejected_at": _isoformat(row["rejected_at"]),
        "rejected_reason": row["rejected_reason"],
    }


async def _body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception as exc:
        raise ApiError(
            status_code=400, error_code=ERR_BAD_REQUEST, message=f"invalid json: {exc}"
        ) from exc
    if not isinstance(body, dict):
        raise ApiError(
            status_code=400, error_code=ERR_BAD_REQUEST, message="body must be a JSON object"
        )
    return body


@router.get(_BASE)
async def list_route(
    request: Request,
    cp_id: str,
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=10_000),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    if status is not None and status not in _ALLOWED_STATUSES:
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message=f"status must be one of {sorted(_ALLOWED_STATUSES)}",
        )

    settings = request.app.state.settings
    page_size = clamp_limit(
        limit,
        default=settings.rest_default_page_size,
        maximum=settings.rest_max_page_size,
    )

    cursor_payload = decode_cursor(cursor)
    after_id: int | None = None
    if cursor_payload is not None:
        raw_id = cursor_payload.get("id")
        if not isinstance(raw_id, int):
            raise ApiError(
                status_code=400,
                error_code=ERR_BAD_REQUEST,
                message="malformed cursor: missing 'id'",
            )
        after_id = raw_id

    async with session_scope(request.app.state.session_factory) as session:
        rows = await list_pending_certificate_signings_by_cp(
            session,
            cp_id=cp_id,
            after_id=after_id,
            limit=page_size,
            status=status,
        )

    if rows is None:
        raise ApiError(
            status_code=404, error_code=ERR_UNKNOWN_CP_ID, message=f"unknown cp_id: {cp_id}"
        )

    has_more = len(rows) > page_size
    page = rows[:page_size]
    next_cursor: str | None = None
    if has_more and page:
        next_cursor = encode_cursor({"id": page[-1]["id"]})

    return {
        "pending_certificate_signings": [_to_response(r) for r in page],
        "next_cursor": next_cursor,
        "request_id": request.state.request_id,
    }


@router.get(_BASE + "/{pending_id}")
async def get_route(request: Request, cp_id: str, pending_id: int) -> dict[str, Any]:
    async with session_scope(request.app.state.session_factory) as session:
        row = await get_pending_certificate_signing(session, cp_id=cp_id, pending_id=pending_id)
    if row is None:
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=f"unknown pending csr {pending_id} for cp_id {cp_id}",
        )
    return {**_to_response(row), "request_id": request.state.request_id}


@router.post(_BASE + "/{pending_id}/approve")
async def approve_route(request: Request, cp_id: str, pending_id: int) -> dict[str, Any]:
    """Operator-supplied signed PEM chain → mark row signed → dispatch
    `CertificateSigned.req` to the charger → return the charger's
    reply status. The DB write happens BEFORE the charger dispatch:

    - If the dispatch fails (charger offline, etc.) the operator can
      retry by re-fetching the row's `signed_chain` and using the
      existing `commands/certificate-signed` endpoint, OR by waiting
      for the charger to re-submit (per spec).
    - If the dispatch succeeds but the charger replies `Rejected`,
      the row stays `signed` (the operator's decision) and the
      charger's verdict surfaces in the response — operators reading
      the row later can tell from the response that the charger
      didn't accept the chain.
    """
    body = await _body(request)
    chain = body.get("signed_chain")
    if not isinstance(chain, str) or not chain.strip():
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="signed_chain is required",
        )
    approved_by = body.get("approved_by")
    if approved_by is not None and not isinstance(approved_by, str):
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="approved_by must be a string when provided",
        )

    async with session_scope(request.app.state.session_factory) as session:
        updated = await mark_pending_certificate_signing_signed(
            session,
            cp_id=cp_id,
            pending_id=pending_id,
            signed_chain=chain,
            approved_by=approved_by,
        )

    if not updated:
        # Either the charger / row doesn't exist, or the row isn't
        # `pending` anymore. Both are 404 from the operator's view —
        # the action they thought was available isn't.
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=(f"pending csr {pending_id} for cp_id {cp_id} not found or no longer pending"),
        )

    ocpp_response = await dispatch_ocpp_call(
        request,
        rpc="CertificateSigned",
        cp_id=cp_id,
        ocpp_request=ocpp_call.CertificateSigned(certificate_chain=chain),
    )
    return {
        "id": pending_id,
        "cp_id": cp_id,
        "status": "signed",
        "charger_status": str(getattr(ocpp_response, "status", "")),
        "request_id": request.state.request_id,
    }


@router.post(_BASE + "/{pending_id}/reject")
async def reject_route(request: Request, cp_id: str, pending_id: int) -> dict[str, Any]:
    body = await _body(request)
    reason = body.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        raise ApiError(
            status_code=400,
            error_code=ERR_BAD_REQUEST,
            message="reason is required",
        )

    async with session_scope(request.app.state.session_factory) as session:
        updated = await mark_pending_certificate_signing_rejected(
            session, cp_id=cp_id, pending_id=pending_id, reason=reason
        )

    if not updated:
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=(f"pending csr {pending_id} for cp_id {cp_id} not found or no longer pending"),
        )

    return {
        "id": pending_id,
        "cp_id": cp_id,
        "status": "rejected",
        "rejected_reason": reason,
        "request_id": request.state.request_id,
    }
