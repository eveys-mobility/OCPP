"""Unit tests for the BootNotification handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result

from eveys_ocpp.handlers.v16 import boot_notification


@pytest.mark.asyncio
async def test_returns_accepted_with_configured_interval(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    result = await boot_notification.handle(
        fake_cp,
        charge_point_vendor="ACME",
        charge_point_model="X1",
        firmware_version="1.0.0",
        charge_point_serial_number="SN001",
    )

    assert isinstance(result, call_result.BootNotification)
    assert result.status == "Accepted"
    assert result.interval == fake_cp.settings.heartbeat_interval_seconds
    upsert.assert_awaited_once()


@pytest.mark.asyncio
async def test_persists_charger_metadata(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    await boot_notification.handle(
        fake_cp,
        charge_point_vendor="ACME",
        charge_point_model="X1",
        firmware_version="1.0.0",
        charge_point_serial_number="SN001",
    )

    call = upsert.await_args
    assert call is not None
    kwargs = call.kwargs
    assert kwargs["cp_id"] == "TEST_CP_001"
    assert kwargs["vendor"] == "ACME"
    assert kwargs["model"] == "X1"
    assert kwargs["firmware_version"] == "1.0.0"
    assert kwargs["serial_number"] == "SN001"


@pytest.mark.asyncio
async def test_handles_missing_optional_fields(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", AsyncMock())

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.status == "Accepted"


# ---- E2-11 idempotency -----------------------------------------------------


@pytest.mark.asyncio
async def test_replay_skips_db_write(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Cache hit → return Accepted without touching the DB.

    The charger sees the same response as the original; the platform
    never sees a duplicate `cp.boot` event because there's no second
    handler invocation downstream.
    """
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(return_value=True)  # replay
    fake_cp.idempotency = fake_idem

    result = await boot_notification.handle(
        fake_cp,
        message_id="MSG-RETRY-1",
        charge_point_vendor="ACME",
    )

    assert result.status == "Accepted"
    upsert.assert_not_awaited()
    fake_idem.check_and_record.assert_awaited_once_with(
        cp_id="TEST_CP_001", message_id="MSG-RETRY-1"
    )


@pytest.mark.asyncio
async def test_first_sighting_runs_handler(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(return_value=False)  # not a replay
    fake_cp.idempotency = fake_idem

    result = await boot_notification.handle(
        fake_cp,
        message_id="MSG-FIRST",
        charge_point_vendor="ACME",
    )

    assert result.status == "Accepted"
    upsert.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_message_id_falls_through(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing message_id → can't dedup; run the handler normally.

    Defensive — the OCPP library always supplies a message_id, but the
    handler should still work if a test or future caller doesn't.
    """
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    fake_idem = AsyncMock()
    fake_cp.idempotency = fake_idem

    result = await boot_notification.handle(fake_cp, charge_point_vendor="ACME")

    assert result.status == "Accepted"
    upsert.assert_awaited_once()
    fake_idem.check_and_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_idempotency_cache_falls_through(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cache attribute None (unit-test setup, Redis-less local stack) →
    handler still runs."""
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)
    fake_cp.idempotency = None

    result = await boot_notification.handle(fake_cp, message_id="MSG-X", charge_point_vendor="ACME")

    assert result.status == "Accepted"
    upsert.assert_awaited_once()


@pytest.mark.asyncio
async def test_cache_outage_falls_through(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """If the cache raises (Redis down), don't wedge the handler.

    Better a rare double-write than a stuck charger when the cache
    misbehaves. Documented in ADR-0017.
    """
    upsert = AsyncMock()
    monkeypatch.setattr(boot_notification, "upsert_charge_point_boot", upsert)

    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(side_effect=RuntimeError("redis down"))
    fake_cp.idempotency = fake_idem

    result = await boot_notification.handle(fake_cp, message_id="MSG-Y", charge_point_vendor="ACME")

    assert result.status == "Accepted"
    upsert.assert_awaited_once()
