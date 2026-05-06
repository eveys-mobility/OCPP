"""Tests for the webhook HMAC signer."""

from __future__ import annotations

from eveys_ocpp.webhooks.signer import compute_signature, verify_signature


def test_signature_format() -> None:
    sig = compute_signature(b"hello", "secret")
    assert sig.startswith("sha256=")
    # 64 hex chars after the prefix
    assert len(sig) == len("sha256=") + 64


def test_signature_is_deterministic() -> None:
    body = b'{"event_id":"evt-1"}'
    a = compute_signature(body, "shared-secret")
    b = compute_signature(body, "shared-secret")
    assert a == b


def test_signature_varies_with_body() -> None:
    a = compute_signature(b"a", "secret")
    b = compute_signature(b"b", "secret")
    assert a != b


def test_signature_varies_with_secret() -> None:
    a = compute_signature(b"body", "one")
    b = compute_signature(b"body", "two")
    assert a != b


def test_verify_round_trip() -> None:
    body = b'{"event_id":"evt-42"}'
    secret = "test-secret"
    assert verify_signature(body, compute_signature(body, secret), secret) is True


def test_verify_rejects_wrong_secret() -> None:
    body = b'{"event_id":"evt-42"}'
    sig = compute_signature(body, "right")
    assert verify_signature(body, sig, "wrong") is False


def test_verify_rejects_tampered_body() -> None:
    sig = compute_signature(b"original", "secret")
    assert verify_signature(b"tampered", sig, "secret") is False


def test_verify_rejects_missing_prefix() -> None:
    # Real header looks like "sha256=<hex>"; a digest without prefix
    # is invalid even if the bytes match.
    body = b"body"
    sig = compute_signature(body, "secret")
    bare = sig.removeprefix("sha256=")
    assert verify_signature(body, bare, "secret") is False
