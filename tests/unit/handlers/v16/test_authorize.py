"""Unit tests for the Authorize handler."""

from __future__ import annotations

from typing import Any

import pytest
from ocpp.v16.enums import AuthorizationStatus

from eveys_ocpp.handlers.v16 import authorize


@pytest.mark.asyncio
async def test_accepts_normal_tag(fake_cp: Any) -> None:
    result = await authorize.handle(fake_cp, id_tag="VALID_RFID_001")
    assert result.id_tag_info.status == AuthorizationStatus.accepted


@pytest.mark.asyncio
async def test_rejects_invalid_prefixed_tag(fake_cp: Any) -> None:
    result = await authorize.handle(fake_cp, id_tag="INVALID_TAG_X")
    assert result.id_tag_info.status == AuthorizationStatus.invalid


@pytest.mark.asyncio
async def test_invalid_match_is_case_insensitive(fake_cp: Any) -> None:
    result = await authorize.handle(fake_cp, id_tag="invalid_tag_y")
    assert result.id_tag_info.status == AuthorizationStatus.invalid
