"""TLS context helpers for the gateway WS server (E5-5, ADR-0011).

Builds the `ssl.SSLContext` the WS server hands to `websockets.serve()`
when mutual-TLS is enabled. Pairs with E5-1 + ADR-0007 — Envoy
terminates the charger-facing TLS, then opens an authenticated
upstream connection to the gateway. mTLS on this Envoy-↔-gateway leg
is the in-cluster authentication boundary that makes "anything can
reach :9000" not an authorisation grant.

Why a dedicated helper rather than building the context inline in
`serve_forever`: the boot-time fail-fast on missing files is much
cleaner here, and the context construction is the obvious unit-test
boundary — without isolating it we'd be testing `websockets.serve`
indirectly.
"""

from __future__ import annotations

import ssl
from typing import TYPE_CHECKING

from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


class TlsConfigError(RuntimeError):
    """Raised at boot when `ws_mtls_enabled` is True but the cert /
    key / CA paths aren't fully configured. Surfaces as a clean
    process exit rather than a half-initialised SSLContext that
    would silently accept connections without verifying clients.
    """


def build_server_ssl_context(settings: Settings) -> ssl.SSLContext | None:
    """Return the `SSLContext` for the WS server, or `None` to run
    plain WS.

    `None` is returned exactly when `ws_mtls_enabled` is False —
    which is the dev / compose / e2e default. Production sets the
    flag and the three paths via the Helm chart.

    The returned context:

    - Loads the gateway's own server cert + key (`load_cert_chain`).
    - Loads the CA bundle (`load_verify_locations`) used to verify
      the peer's client cert.
    - Sets `verify_mode = CERT_REQUIRED` so a peer connecting
      without a cert is rejected outright.
    - Enables hostname checking off (`check_hostname = False`)
      because the *client* side of mTLS verifies the server's name;
      we're the server, and we authenticate the client by cert
      identity (the client cert's CN / SAN is the operator's
      audit signal, not a hostname).
    """
    if not settings.ws_mtls_enabled:
        return None

    cert = settings.ws_mtls_cert_path
    key = settings.ws_mtls_key_path
    ca = settings.ws_mtls_ca_path
    missing = [name for name, value in (("cert", cert), ("key", key), ("ca", ca)) if not value]
    if missing:
        raise TlsConfigError(
            f"ws_mtls_enabled=True but missing path(s): {', '.join(missing)}. "
            "Set ws_mtls_cert_path, ws_mtls_key_path, ws_mtls_ca_path."
        )

    ctx = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
    ctx.load_cert_chain(certfile=cert, keyfile=key)
    ctx.load_verify_locations(cafile=ca)
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = False
    log.info(
        "ws_tls.context_built",
        cert_path=cert,
        ca_path=ca,
        verify_mode="CERT_REQUIRED",
    )
    return ctx
