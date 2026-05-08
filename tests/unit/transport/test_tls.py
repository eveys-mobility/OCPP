"""Unit tests for the WS server's TLS context helper (E5-5)."""

from __future__ import annotations

import shutil
import ssl
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

from eveys_ocpp.settings import Settings
from eveys_ocpp.transport._tls import TlsConfigError, build_server_ssl_context


def test_returns_none_when_mtls_disabled() -> None:
    """The default (`ws_mtls_enabled=False`) is what compose, dev,
    and e2e all run with — plain WS, no TLS context."""
    ctx = build_server_ssl_context(Settings())
    assert ctx is None


def test_raises_when_enabled_without_paths() -> None:
    with pytest.raises(TlsConfigError) as exc:
        build_server_ssl_context(Settings(ws_mtls_enabled=True))
    msg = str(exc.value)
    # Each missing path is named so the operator knows what to fix.
    assert "cert" in msg
    assert "key" in msg
    assert "ca" in msg


def test_raises_when_enabled_with_partial_paths() -> None:
    with pytest.raises(TlsConfigError) as exc:
        build_server_ssl_context(
            Settings(
                ws_mtls_enabled=True,
                ws_mtls_cert_path="/some/cert.pem",
                ws_mtls_key_path="/some/key.pem",
                # ca missing
            )
        )
    assert "ca" in str(exc.value)
    # cert + key were provided so they shouldn't be flagged.
    assert "cert," not in str(exc.value)
    assert "key," not in str(exc.value)


# --- Real-cert loading test --------------------------------------------------
#
# A unit test that exercises ssl.SSLContext.load_cert_chain has to
# produce a real PEM. We shell out to openssl rather than introducing a
# `cryptography` dev dep — `openssl` is already a hard requirement of
# `scripts/gen-dev-certs.sh` and we don't want a divergent toolchain.


@pytest.fixture
def cert_set(tmp_path: Path) -> Iterator[dict[str, Path]]:
    """Mint a self-signed cert + key + CA bundle in a tmp dir.

    For these tests the CA *is* the server cert (self-signed), which
    is fine — `load_verify_locations` only needs a PEM that
    represents a trust anchor. The point is to exercise the loading
    path, not validate any chain."""
    if shutil.which("openssl") is None:
        pytest.skip("openssl not on PATH")

    cert = tmp_path / "server.crt"
    key = tmp_path / "server.key"
    ca = tmp_path / "ca.crt"

    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-nodes",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=test",
        ],
        check=True,
        capture_output=True,
    )
    # Self-trust: same cert is the CA bundle for these tests.
    shutil.copyfile(cert, ca)
    yield {"cert": cert, "key": key, "ca": ca}


def test_builds_context_with_cert_required_when_paths_valid(
    cert_set: dict[str, Path],
) -> None:
    settings = Settings(
        ws_mtls_enabled=True,
        ws_mtls_cert_path=str(cert_set["cert"]),
        ws_mtls_key_path=str(cert_set["key"]),
        ws_mtls_ca_path=str(cert_set["ca"]),
    )

    ctx = build_server_ssl_context(settings)

    assert ctx is not None
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    # Server-side mTLS doesn't validate the peer's hostname — the
    # peer is Envoy, identified by cert, not by hostname.
    assert ctx.check_hostname is False


def test_raises_on_unreadable_cert_path(tmp_path: Path) -> None:
    """A wrong path during boot should fail loud, not silently
    fall through to a half-initialised context."""
    settings = Settings(
        ws_mtls_enabled=True,
        ws_mtls_cert_path=str(tmp_path / "nope.crt"),
        ws_mtls_key_path=str(tmp_path / "nope.key"),
        ws_mtls_ca_path=str(tmp_path / "nope.ca"),
    )

    # SSLContext.load_cert_chain raises FileNotFoundError on a
    # missing file. We let it propagate — a config error at boot
    # should crash the gateway, not start it half-broken.
    with pytest.raises((FileNotFoundError, ssl.SSLError)):
        build_server_ssl_context(settings)
