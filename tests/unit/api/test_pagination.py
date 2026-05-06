"""Tests for the cursor encode/decode + clamp helpers."""

from __future__ import annotations

import pytest

from eveys_ocpp.api._errors import ApiError
from eveys_ocpp.api._pagination import clamp_limit, decode_cursor, encode_cursor


def test_round_trip_preserves_payload() -> None:
    cursor = encode_cursor({"id": 12345})
    assert decode_cursor(cursor) == {"id": 12345}


def test_none_or_empty_cursor_decodes_to_none() -> None:
    assert decode_cursor(None) is None
    assert decode_cursor("") is None


def test_malformed_base64_raises_400() -> None:
    with pytest.raises(ApiError) as exc_info:
        decode_cursor("!!!not-base64!!!")
    assert exc_info.value.status_code == 400
    assert exc_info.value.error_code == "BAD_REQUEST"


def test_non_object_payload_raises_400() -> None:
    # base64 of `"hello"` (a JSON string, not an object).
    bad = encode_cursor({"id": 1}).replace("eyJpZCI6", "Imhlbm")  # arbitrary mangling
    # If our mangling didn't break it, hand-craft a plain string payload:
    import base64

    plain = base64.urlsafe_b64encode(b'"hello"').decode("ascii")

    with pytest.raises(ApiError):
        decode_cursor(plain)
    # also check the mangled one — either it decodes to non-dict OR
    # raises during base64 decode; both are acceptable outcomes.
    with pytest.raises(ApiError):
        decode_cursor(bad)


def test_clamp_limit_uses_default_when_none() -> None:
    assert clamp_limit(None, default=100, maximum=500) == 100


def test_clamp_limit_caps_at_maximum() -> None:
    assert clamp_limit(9999, default=100, maximum=500) == 500


def test_clamp_limit_passes_through_in_range() -> None:
    assert clamp_limit(42, default=100, maximum=500) == 42


def test_clamp_limit_rejects_zero_or_negative() -> None:
    with pytest.raises(ApiError) as exc_info:
        clamp_limit(0, default=100, maximum=500)
    assert exc_info.value.status_code == 400


def test_encode_is_deterministic() -> None:
    """Same payload → same cursor (sort_keys=True). Snapshot-able."""
    a = encode_cursor({"id": 1, "ts": "2026-01-01T00:00:00+00:00"})
    b = encode_cursor({"ts": "2026-01-01T00:00:00+00:00", "id": 1})
    assert a == b
