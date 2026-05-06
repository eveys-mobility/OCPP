"""HMAC-SHA-256 signing for outbound webhook deliveries (E3-9).

Per `docs/integration/03-webhooks.md` § Authentication, every delivery
carries:

    X-Eveys-Signature: sha256=<lowercase hex>

over the raw request body. The receiver verifies with the same shared
secret. Signature is computed over the bytes that go on the wire
exactly — no normalisation. This module is the single place that
shape is defined.

`verify_signature` is the inverse helper, exposed mostly for tests
(real receivers re-implement it in their own language). It uses
`hmac.compare_digest` to dodge timing-comparison attacks.
"""

from __future__ import annotations

import hashlib
import hmac

_PREFIX = "sha256="


def compute_signature(body: bytes, secret: str) -> str:
    """Return the value of the `X-Eveys-Signature` header for a body.

    `body` is the raw bytes sent on the wire. `secret` is the shared
    HMAC secret (matches the backend's stored secret). The output
    string is `sha256=<lowercase hex>` ready to put in the header.
    """
    digest = hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()
    return f"{_PREFIX}{digest}"


def verify_signature(body: bytes, signature_header: str, secret: str) -> bool:
    """Constant-time check of an inbound signature.

    Returns False on prefix mismatch (no exception — receivers should
    treat any malformed header as a 401, same as a wrong digest)."""
    if not signature_header.startswith(_PREFIX):
        return False
    expected = compute_signature(body, secret)
    return hmac.compare_digest(expected, signature_header)
