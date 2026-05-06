"""Unit tests for the DataTransfer (charger-initiated) handler."""

from __future__ import annotations

from typing import Any

import pytest
from ocpp.v16 import call_result
from ocpp.v16.enums import DataTransferStatus

from eveys_ocpp.handlers.v16 import data_transfer


@pytest.mark.asyncio
async def test_returns_unknown_vendor_id_by_default(fake_cp: Any) -> None:
    """No vendor handlers are wired; we honour the spec by returning
    ``UnknownVendorId`` so the charger learns to stop sending."""
    result = await data_transfer.handle(
        fake_cp, vendor_id="acme.fastcharge", message_id="status", data="{}"
    )

    assert isinstance(result, call_result.DataTransfer)
    assert result.status == DataTransferStatus.unknown_vendor_id


@pytest.mark.asyncio
async def test_handles_payload_without_optional_fields(fake_cp: Any) -> None:
    """``message_id`` and ``data`` are optional in the OCPP schema; the
    handler must accept their absence."""
    result = await data_transfer.handle(fake_cp, vendor_id="acme.fastcharge")

    assert result.status == DataTransferStatus.unknown_vendor_id


@pytest.mark.asyncio
async def test_does_not_raise_on_empty_data(fake_cp: Any) -> None:
    """An empty data string is valid per spec — log line should report
    `data_len=0` and the response should still be ``UnknownVendorId``."""
    result = await data_transfer.handle(
        fake_cp, vendor_id="acme.fastcharge", message_id="status", data=""
    )

    assert result.status == DataTransferStatus.unknown_vendor_id
