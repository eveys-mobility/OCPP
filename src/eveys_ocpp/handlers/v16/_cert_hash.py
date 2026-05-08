"""Certificate hashing helpers (TC_074, TC_075_1, TC_075_2, TC_076).

OCPP 1.6 Security Whitepaper §5.1 defines `hash_data` as a 4-tuple:

    {
      "hashAlgorithm": "SHA256",   // also SHA384 / SHA512 by spec
      "issuerNameHash":  hex,       // SHA-256 of issuer DN (DER)
      "issuerKeyHash":   hex,       // SHA-256 of issuer publicKeyInfo (DER)
      "serialNumber":    hex,       // cert serial as hex
    }

The charger uses this Dict to identify which installed cert to
delete via `DeleteCertificate`. The operator only sees a single
SHA-256 of the **whole cert DER** as the user-facing handle (it's
what we store on `charge_point_certificates.sha256_hash`); we
reconstruct the spec's 4-tuple at delete time from the stored PEM.

We use `cryptography` (the de facto X.509 library) for parsing.
The gateway never validates signatures or chains — that's the
charger's job — so we only ever extract identifying fields.

Note on the OCPP `hashAlgorithm` field: the spec allows SHA-256,
SHA-384, SHA-512. We always emit SHA-256 today; the charger spec
mandates SHA-256 support, and operators don't typically need 384
or 512 for OCPP cert management. Forward-compat is a single enum
flip if a customer hits a charger that demands one of the others.
"""

from __future__ import annotations

import hashlib

from cryptography import x509
from cryptography.hazmat.primitives import serialization

OCPP_HASH_ALGORITHM = "SHA256"


def parse_pem(pem: str) -> x509.Certificate:
    """Parse a PEM-encoded X.509 certificate. Raises `ValueError` on
    a malformed input — callers translate that to the appropriate
    boundary error (gRPC `INVALID_ARGUMENT` / REST 400)."""
    try:
        return x509.load_pem_x509_certificate(pem.encode("utf-8"))
    except (ValueError, TypeError) as exc:
        raise ValueError(f"invalid PEM: {exc}") from exc


def compute_cert_sha256(cert: x509.Certificate) -> str:
    """SHA-256 of the cert's DER encoding, hex-lowercase. This is the
    user-facing identifier we store on `charge_point_certificates`
    so a future DeleteCertificate can find the row without the
    operator hauling the original PEM around."""
    der = cert.public_bytes(serialization.Encoding.DER)
    return hashlib.sha256(der).hexdigest()


def build_hash_data(cert: x509.Certificate) -> dict[str, str]:
    """Return the OCPP 1.6 §5.1 `hash_data` Dict the charger expects
    on `DeleteCertificate.req`. All hex values are lowercase.

    `issuerNameHash` is SHA-256 of the issuer's DistinguishedName in
    DER form. `issuerKeyHash` is SHA-256 of the cert's own
    SubjectPublicKeyInfo — per OCPP §5.1 this is the issuer's key
    used to verify THIS cert, but for self-signed root certs (the
    only kind installed via InstallCertificate) the issuer key IS
    the cert's own subject key, so we hash the cert's
    SubjectPublicKeyInfo directly. For non-self-signed certs (a
    SignCertificate flow chain), the operator would supply the
    parent's hash_data instead — out of scope today.
    """
    issuer_der = cert.issuer.public_bytes()
    issuer_name_hash = hashlib.sha256(issuer_der).hexdigest()

    pub_key_der = cert.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    issuer_key_hash = hashlib.sha256(pub_key_der).hexdigest()

    # OCPP serialNumber is the hex of the integer; lowercase, no `0x`
    # prefix. Negative serials don't occur in practice (X.509
    # mandates positive); guard anyway.
    serial = cert.serial_number
    if serial < 0:
        raise ValueError(f"negative cert serial: {serial}")
    serial_hex = format(serial, "x")

    return {
        "hashAlgorithm": OCPP_HASH_ALGORITHM,
        "issuerNameHash": issuer_name_hash,
        "issuerKeyHash": issuer_key_hash,
        "serialNumber": serial_hex,
    }
