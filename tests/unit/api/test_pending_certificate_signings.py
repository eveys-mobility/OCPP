"""Unit tests for the operator-queue REST surface (#189).

Covers:
- list (filter by status, pagination)
- get (404 when missing)
- approve (DB write → CertificateSigned dispatch → response)
- reject (DB write only)
- input validation (missing chain / reason)
- approve idempotency surfaced as 404 when row already moved
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from eveys_ocpp.api import pending_certificate_signings as routes


def _row(
    *,
    id: int = 1,
    cp_id: str = "CP_001",
    csr: str = "-----BEGIN CERTIFICATE REQUEST-----\nfoo\n-----END CERTIFICATE REQUEST-----",
    status: str = "pending",
    signed_at: datetime | None = None,
    approved_by: str | None = None,
    rejected_at: datetime | None = None,
    rejected_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "id": id,
        "cp_id": cp_id,
        "csr": csr,
        "received_at": datetime(2026, 5, 11, 10, 0, tzinfo=UTC),
        "status": status,
        "signed_at": signed_at,
        "approved_by": approved_by,
        "rejected_at": rejected_at,
        "rejected_reason": rejected_reason,
    }


def _set_dispatch(fake_command_service: MagicMock, status: str = "Accepted") -> None:
    fake_command_service._dispatch_ocpp_call.return_value = SimpleNamespace(status=status)
    fake_command_service._dispatch_ocpp_call.side_effect = None


# ---- list ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_returns_rows(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        routes,
        "list_pending_certificate_signings_by_cp",
        AsyncMock(return_value=[_row(id=1), _row(id=2, status="signed")]),
    )
    response = await client.get("/api/v1/charge-points/CP_001/pending-certificate-signings")
    assert response.status_code == 200
    body = response.json()
    assert len(body["pending_certificate_signings"]) == 2
    assert body["pending_certificate_signings"][0]["status"] == "pending"
    assert body["pending_certificate_signings"][1]["status"] == "signed"
    assert body["next_cursor"] is None


@pytest.mark.asyncio
async def test_list_unknown_charger_404(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        routes, "list_pending_certificate_signings_by_cp", AsyncMock(return_value=None)
    )
    response = await client.get("/api/v1/charge-points/UNKNOWN/pending-certificate-signings")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_status_filter_validated(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Status outside the allowed set is rejected at the boundary —
    avoids leaking arbitrary strings into the SQL filter even though
    ORM parameterisation would handle it safely."""
    monkeypatch.setattr(
        routes, "list_pending_certificate_signings_by_cp", AsyncMock(return_value=[])
    )
    response = await client.get(
        "/api/v1/charge-points/CP_001/pending-certificate-signings?status=bogus"
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_list_pagination_emits_cursor(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Repository returns limit+1 rows when more exist; the route
    truncates to limit and emits a next_cursor."""
    rows = [_row(id=i) for i in range(1, 4)]
    monkeypatch.setattr(
        routes, "list_pending_certificate_signings_by_cp", AsyncMock(return_value=rows)
    )
    response = await client.get("/api/v1/charge-points/CP_001/pending-certificate-signings?limit=2")
    body = response.json()
    assert len(body["pending_certificate_signings"]) == 2
    assert body["next_cursor"] is not None


# ---- get -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_returns_row(client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        routes, "get_pending_certificate_signing", AsyncMock(return_value=_row(id=42))
    )
    response = await client.get("/api/v1/charge-points/CP_001/pending-certificate-signings/42")
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == 42
    assert body["status"] == "pending"
    assert body["csr"].startswith("-----BEGIN")


@pytest.mark.asyncio
async def test_get_missing_404(client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(routes, "get_pending_certificate_signing", AsyncMock(return_value=None))
    response = await client.get("/api/v1/charge-points/CP_001/pending-certificate-signings/99")
    assert response.status_code == 404


# ---- approve ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_approve_writes_then_dispatches(
    client: httpx.AsyncClient,
    fake_command_service: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: DB transition succeeds → CertificateSigned dispatched
    → charger reply surfaces in `charger_status`."""
    mark = AsyncMock(return_value=True)
    monkeypatch.setattr(routes, "mark_pending_certificate_signing_signed", mark)
    _set_dispatch(fake_command_service, status="Accepted")

    response = await client.post(
        "/api/v1/charge-points/CP_001/pending-certificate-signings/7/approve",
        json={"signed_chain": "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == 7
    assert body["status"] == "signed"
    assert body["charger_status"] == "Accepted"
    mark.assert_awaited_once()
    fake_command_service._dispatch_ocpp_call.assert_awaited_once()


@pytest.mark.asyncio
async def test_approve_charger_rejects_chain_still_marked_signed(
    client: httpx.AsyncClient,
    fake_command_service: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the charger replies Rejected, the row stays `signed` (the
    operator made the decision); the charger's verdict surfaces in
    the response so the operator can see the disagreement."""
    monkeypatch.setattr(
        routes, "mark_pending_certificate_signing_signed", AsyncMock(return_value=True)
    )
    _set_dispatch(fake_command_service, status="Rejected")

    response = await client.post(
        "/api/v1/charge-points/CP_001/pending-certificate-signings/7/approve",
        json={"signed_chain": "chain"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "signed"
    assert body["charger_status"] == "Rejected"


@pytest.mark.asyncio
async def test_approve_404_when_row_not_pending(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Row already approved/rejected, or missing → 404. No dispatch
    attempted."""
    monkeypatch.setattr(
        routes, "mark_pending_certificate_signing_signed", AsyncMock(return_value=False)
    )
    response = await client.post(
        "/api/v1/charge-points/CP_001/pending-certificate-signings/7/approve",
        json={"signed_chain": "chain"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_approve_requires_signed_chain(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/pending-certificate-signings/7/approve",
        json={},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_approve_rejects_empty_chain(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/pending-certificate-signings/7/approve",
        json={"signed_chain": "   "},
    )
    assert response.status_code == 400


# ---- reject ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_reject_writes_no_dispatch(
    client: httpx.AsyncClient,
    fake_command_service: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        routes, "mark_pending_certificate_signing_rejected", AsyncMock(return_value=True)
    )
    response = await client.post(
        "/api/v1/charge-points/CP_001/pending-certificate-signings/7/reject",
        json={"reason": "CN does not match charger serial"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "rejected"
    assert body["rejected_reason"] == "CN does not match charger serial"
    fake_command_service._dispatch_ocpp_call.assert_not_awaited()


@pytest.mark.asyncio
async def test_reject_404_when_row_not_pending(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        routes, "mark_pending_certificate_signing_rejected", AsyncMock(return_value=False)
    )
    response = await client.post(
        "/api/v1/charge-points/CP_001/pending-certificate-signings/7/reject",
        json={"reason": "stale"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_reject_requires_reason(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/pending-certificate-signings/7/reject",
        json={},
    )
    assert response.status_code == 400
