"""Unit tests for the operator credential rotation surface (#196, TC_073).

Covers PUT (upsert), DELETE (unprovision), validation, idempotency,
and the audit-event emission.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from eveys_ocpp.api import credentials as routes

_VALID_PASSWORD = "a-long-enough-password-123"


# ---- PUT ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_put_provisions_charger(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    upsert = AsyncMock(return_value=True)
    monkeypatch.setattr(routes, "upsert_charge_point_credential", upsert)

    response = await client.put(
        "/api/v1/charge-points/CP_001/credentials",
        json={"password": _VALID_PASSWORD, "actor": "ops@example.com"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body == {"cp_id": "CP_001", "status": "provisioned", "request_id": body["request_id"]}
    upsert.assert_awaited_once()
    # password_hash arg looks like a bcrypt output; plaintext never leaked
    kwargs = upsert.await_args.kwargs
    assert kwargs["cp_id"] == "CP_001"
    assert kwargs["password_hash"].startswith("$2")
    assert _VALID_PASSWORD not in kwargs["password_hash"]


@pytest.mark.asyncio
async def test_put_unknown_cp_id_404(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(routes, "upsert_charge_point_credential", AsyncMock(return_value=False))
    response = await client.put(
        "/api/v1/charge-points/NOPE/credentials",
        json={"password": _VALID_PASSWORD},
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_put_rejects_short_password(client: httpx.AsyncClient) -> None:
    response = await client.put(
        "/api/v1/charge-points/CP_001/credentials",
        json={"password": "tooshort"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_put_rejects_oversized_password(client: httpx.AsyncClient) -> None:
    """bcrypt silently truncates beyond 72 bytes; reject at the boundary."""
    response = await client.put(
        "/api/v1/charge-points/CP_001/credentials",
        json={"password": "a" * 73},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_put_requires_password(client: httpx.AsyncClient) -> None:
    response = await client.put(
        "/api/v1/charge-points/CP_001/credentials",
        json={"actor": "x"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_put_rejects_non_string_password(client: httpx.AsyncClient) -> None:
    response = await client.put(
        "/api/v1/charge-points/CP_001/credentials",
        json={"password": 12345678901234},  # int, not str
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_put_rejects_non_string_actor(client: httpx.AsyncClient) -> None:
    response = await client.put(
        "/api/v1/charge-points/CP_001/credentials",
        json={"password": _VALID_PASSWORD, "actor": 123},
    )
    assert response.status_code == 400


# ---- DELETE ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_unprovisions(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(routes, "delete_charge_point_credential", AsyncMock(return_value=True))
    monkeypatch.setattr(
        "eveys_ocpp.persistence.repositories.get_charge_point_pk",
        AsyncMock(return_value=42),
    )

    response = await client.delete("/api/v1/charge-points/CP_001/credentials")
    assert response.status_code == 200
    body = response.json()
    assert body["cp_id"] == "CP_001"
    assert body["status"] == "unprovisioned"


@pytest.mark.asyncio
async def test_delete_idempotent_when_no_row(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Charger exists but has no credential row → still 200, status unprovisioned."""
    monkeypatch.setattr(routes, "delete_charge_point_credential", AsyncMock(return_value=False))
    monkeypatch.setattr(
        "eveys_ocpp.persistence.repositories.get_charge_point_pk",
        AsyncMock(return_value=42),
    )

    response = await client.delete("/api/v1/charge-points/CP_001/credentials")
    assert response.status_code == 200
    assert response.json()["status"] == "unprovisioned"


@pytest.mark.asyncio
async def test_delete_unknown_cp_id_404(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "eveys_ocpp.persistence.repositories.get_charge_point_pk",
        AsyncMock(return_value=None),
    )

    response = await client.delete("/api/v1/charge-points/NOPE/credentials")
    assert response.status_code == 404


# ---- Audit event ----------------------------------------------------------


@pytest.mark.asyncio
async def test_put_emits_credential_rotated_event(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eveys_ocpp._generated.events.v1 import events_pb2

    monkeypatch.setattr(routes, "upsert_charge_point_credential", AsyncMock(return_value=True))
    producer = MagicMock()
    producer.publish = AsyncMock()
    # Attach via the running app state — same hook the routes use.
    client._transport.app.state.event_producer = producer  # type: ignore[attr-defined]

    response = await client.put(
        "/api/v1/charge-points/CP_001/credentials",
        json={"password": _VALID_PASSWORD, "actor": "ops@example.com"},
    )
    assert response.status_code == 200

    producer.publish.assert_awaited_once()
    kwargs = producer.publish.await_args.kwargs
    assert kwargs["key"] == "CP_001"

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(kwargs["value"])
    assert envelope.cp_id == "CP_001"
    assert envelope.WhichOneof("payload") == "cp_credential_rotated"
    assert envelope.cp_credential_rotated.action == "set"
    assert envelope.cp_credential_rotated.actor == "ops@example.com"


@pytest.mark.asyncio
async def test_delete_emits_only_when_row_existed(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No spurious audit on a no-op delete."""
    monkeypatch.setattr(
        "eveys_ocpp.persistence.repositories.get_charge_point_pk",
        AsyncMock(return_value=42),
    )
    monkeypatch.setattr(routes, "delete_charge_point_credential", AsyncMock(return_value=False))
    producer = MagicMock()
    producer.publish = AsyncMock()
    client._transport.app.state.event_producer = producer  # type: ignore[attr-defined]

    response = await client.delete("/api/v1/charge-points/CP_001/credentials")
    assert response.status_code == 200
    producer.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_failure_does_not_crash(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(routes, "upsert_charge_point_credential", AsyncMock(return_value=True))
    producer = MagicMock()
    producer.publish = AsyncMock(side_effect=RuntimeError("kafka down"))
    client._transport.app.state.event_producer = producer  # type: ignore[attr-defined]

    response = await client.put(
        "/api/v1/charge-points/CP_001/credentials",
        json={"password": _VALID_PASSWORD},
    )
    # Still 200: the DB write succeeded, the audit event is best-effort.
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_no_producer_skips_emit(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(routes, "upsert_charge_point_credential", AsyncMock(return_value=True))
    # Default fixture leaves event_producer unset; no need to set it.
    response = await client.put(
        "/api/v1/charge-points/CP_001/credentials",
        json={"password": _VALID_PASSWORD},
    )
    assert response.status_code == 200
