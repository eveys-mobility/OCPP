"""Unit tests for the certificate hashing helpers.

The §5.1 hash_data Dict shape is the load-bearing contract: a typo
in any of the four field names would silently cause every
DeleteCertificate to fail (the charger looks up by exact dict key).
These tests pin the contract.
"""

from __future__ import annotations

import datetime as dt
import hashlib

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from eveys_ocpp.handlers.v16 import _cert_hash


def _make_cert(
    *,
    common_name: str = "test-root",
    serial: int = 0xDEADBEEF,
) -> tuple[str, x509.Certificate]:
    """Build a self-signed cert. Deterministic except for the RSA
    key — that's per-test entropy. Caller pins by serial / CN if it
    needs to know what's inside."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(serial)
        .not_valid_before(dt.datetime(2026, 1, 1))
        .not_valid_after(dt.datetime(2027, 1, 1))
        .sign(key, hashes.SHA256())
    )
    pem = cert.public_bytes(serialization.Encoding.PEM).decode()
    return pem, cert


def test_parse_pem_round_trips() -> None:
    pem, original = _make_cert()
    parsed = _cert_hash.parse_pem(pem)
    assert parsed.serial_number == original.serial_number
    assert parsed.subject == original.subject


def test_parse_pem_raises_on_garbage() -> None:
    with pytest.raises(ValueError, match="invalid PEM"):
        _cert_hash.parse_pem("this is not a pem")


def test_parse_pem_raises_on_empty_string() -> None:
    with pytest.raises(ValueError, match="invalid PEM"):
        _cert_hash.parse_pem("")


def test_compute_cert_sha256_matches_manual_hash() -> None:
    """The `cert_sha256` user handle is plain SHA-256 of the DER —
    operators can recompute it themselves with `openssl x509 -in
    cert.pem -outform DER | sha256sum`. Pin that contract."""
    pem, cert = _make_cert(serial=0xABC)
    expected = hashlib.sha256(cert.public_bytes(serialization.Encoding.DER)).hexdigest()
    assert _cert_hash.compute_cert_sha256(_cert_hash.parse_pem(pem)) == expected


def test_compute_cert_sha256_differs_per_cert() -> None:
    pem_a, _ = _make_cert(common_name="a", serial=1)
    pem_b, _ = _make_cert(common_name="b", serial=2)
    assert _cert_hash.compute_cert_sha256(
        _cert_hash.parse_pem(pem_a)
    ) != _cert_hash.compute_cert_sha256(_cert_hash.parse_pem(pem_b))


# ---- §5.1 hash_data Dict shape — the load-bearing contract ---------------


def test_hash_data_has_exactly_four_keys() -> None:
    """The §5.1 Dict shape is closed; an extra key would be ignored
    by some chargers and reject by others. Pin the keys exactly."""
    pem, _ = _make_cert()
    hd = _cert_hash.build_hash_data(_cert_hash.parse_pem(pem))
    assert set(hd.keys()) == {
        "hashAlgorithm",
        "issuerNameHash",
        "issuerKeyHash",
        "serialNumber",
    }


def test_hash_data_algorithm_is_sha256() -> None:
    pem, _ = _make_cert()
    hd = _cert_hash.build_hash_data(_cert_hash.parse_pem(pem))
    assert hd["hashAlgorithm"] == "SHA256"


def test_hash_data_serial_is_lowercase_hex_no_prefix() -> None:
    """Charger expects bare hex, no `0x` prefix. The OCPP 1.6 schema
    enforces this — a regression here would fail every delete."""
    pem, _ = _make_cert(serial=0xABCDEF)
    hd = _cert_hash.build_hash_data(_cert_hash.parse_pem(pem))
    assert hd["serialNumber"] == "abcdef"
    # No `0x` prefix and no uppercase letters.
    assert not hd["serialNumber"].startswith("0x")
    assert hd["serialNumber"] == hd["serialNumber"].lower()


def test_hash_data_issuer_name_hash_is_64_hex_chars() -> None:
    """SHA-256 hex is always 64 chars. A 32-char value would be raw
    bytes-as-string; a 128-char value would be sha512."""
    pem, _ = _make_cert()
    hd = _cert_hash.build_hash_data(_cert_hash.parse_pem(pem))
    assert len(hd["issuerNameHash"]) == 64
    int(hd["issuerNameHash"], 16)  # must be valid hex


def test_hash_data_issuer_key_hash_is_64_hex_chars() -> None:
    pem, _ = _make_cert()
    hd = _cert_hash.build_hash_data(_cert_hash.parse_pem(pem))
    assert len(hd["issuerKeyHash"]) == 64
    int(hd["issuerKeyHash"], 16)


def test_hash_data_two_different_certs_have_different_key_hashes() -> None:
    """Two RSA keys with the same CN must produce different
    issuerKeyHash. Without this, two certs from the same operator
    would collide and DeleteCertificate would target the wrong one."""
    pem_a, _ = _make_cert(common_name="root", serial=1)
    pem_b, _ = _make_cert(common_name="root", serial=2)
    hd_a = _cert_hash.build_hash_data(_cert_hash.parse_pem(pem_a))
    hd_b = _cert_hash.build_hash_data(_cert_hash.parse_pem(pem_b))
    # Same issuer name (both CN=root) — issuerNameHash collides.
    # That's expected and exactly why §5.1 requires the 4-tuple, not
    # just the issuer DN. The serial differentiates them.
    assert hd_a["issuerKeyHash"] != hd_b["issuerKeyHash"]
    assert hd_a["serialNumber"] != hd_b["serialNumber"]
