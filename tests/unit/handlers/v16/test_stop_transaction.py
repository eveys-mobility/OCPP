"""Unit tests for the StopTransaction handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16.enums import AuthorizationStatus

from eveys_ocpp.handlers.v16 import stop_transaction


@pytest.mark.asyncio
async def test_applies_stop(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    stop = AsyncMock(return_value=True)
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)

    result = await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
        reason="Local",
        id_tag="VALID_RFID_001",
    )

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    stop.assert_awaited_once()
    kwargs = stop.await_args.kwargs
    assert kwargs["transaction_id"] == 999
    assert kwargs["meter_stop_wh"] == 12345
    # Idempotency key built from (cp_id, transaction_id, meter_stop)
    assert kwargs["idempotency_key"] == "TEST_CP_001:999:12345"


@pytest.mark.asyncio
async def test_replay_is_noop(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Same triple twice → second call sees `applied=False` from the repo."""
    stop = AsyncMock(return_value=False)
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)

    result = await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
        reason="Local",
    )

    # Still returns Accepted to the charger — the charger doesn't know or
    # care that we treated it as a replay; it just needs an OK reply.
    assert result.id_tag_info.status == AuthorizationStatus.accepted


@pytest.mark.asyncio
async def test_reject_invalid_id_tag(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(stop_transaction, "stop_transaction", AsyncMock(return_value=True))

    result = await stop_transaction.handle(
        fake_cp,
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
        reason="Local",
        id_tag="INVALID_TAG",
    )

    assert result.id_tag_info.status == AuthorizationStatus.invalid


# ---- E2-11 idempotency -----------------------------------------------------


@pytest.mark.asyncio
async def test_replay_skips_db_write(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Cache hit → return Accepted without invoking the DB-layer stop."""
    stop = AsyncMock(return_value=True)
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)

    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(return_value=True)  # replay
    fake_cp.idempotency = fake_idem

    result = await stop_transaction.handle(
        fake_cp,
        message_id="MSG-RETRY-1",
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
    )

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    stop.assert_not_awaited()
    fake_idem.check_and_record.assert_awaited_once_with(
        cp_id="TEST_CP_001", message_id="MSG-RETRY-1"
    )


@pytest.mark.asyncio
async def test_first_sighting_runs_handler(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    stop = AsyncMock(return_value=True)
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)

    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(return_value=False)  # not a replay
    fake_cp.idempotency = fake_idem

    result = await stop_transaction.handle(
        fake_cp,
        message_id="MSG-FIRST",
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
    )

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_cache_outage_falls_through_to_db_dedup(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Redis outage → fall through to the DB-layer dedup. Defense in depth."""
    stop = AsyncMock(return_value=True)
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)

    fake_idem = AsyncMock()
    fake_idem.check_and_record = AsyncMock(side_effect=RuntimeError("redis down"))
    fake_cp.idempotency = fake_idem

    result = await stop_transaction.handle(
        fake_cp,
        message_id="MSG-Y",
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
    )

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_idempotency_cache_falls_through(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """idempotency=None → DB-layer dedup is the only line of defense."""
    stop = AsyncMock(return_value=True)
    monkeypatch.setattr(stop_transaction, "stop_transaction", stop)
    fake_cp.idempotency = None

    result = await stop_transaction.handle(
        fake_cp,
        message_id="MSG-Z",
        transaction_id=999,
        meter_stop=12345,
        timestamp="2026-04-29T01:00:00+00:00",
    )

    assert result.id_tag_info.status == AuthorizationStatus.accepted
    stop.assert_awaited_once()
