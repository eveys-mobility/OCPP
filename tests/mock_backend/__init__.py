"""Mock Eveys backend (E3-10).

Implements the contract in `docs/integration/01-backend-rest-contract.md`
with simulated responses, so the gateway team can develop / test
against a stable surface without waiting for the real backend.

This package is **dev only**. It's never imported by the gateway
runtime (the production wheel built from `pyproject.toml` only
packages `src/eveys_ocpp`). Two ways to use it:

1. **In-process** (test fixtures): instantiate the FastAPI ``app``
   and drive it with ``httpx.AsyncClient`` — see
   ``tests/unit/mock_backend/test_endpoints.py``.

2. **As a standalone process** (compose / local dev):
   ``python -m tests.mock_backend`` — boots uvicorn on the
   configured host:port (defaults to ``0.0.0.0:9200``).

Behaviour controls (env vars; all optional):

- ``MOCK_BACKEND_TOKEN`` — required Bearer token. Default ``dev-token``.
- ``MOCK_BACKEND_BLOCKED_ID_TAGS`` — comma-separated id_tags to refuse
  Authorize for. Returns ``Blocked`` instead of ``Accepted``.
- ``MOCK_BACKEND_FAIL_AUTHORIZE`` — when ``1``, always returns 503
  (for testing the gateway's circuit breaker / fallback policy).
- ``MOCK_BACKEND_HEARTBEAT_INTERVAL_SECONDS`` — value to return on
  ``/charge-points/register``. Default ``60``.

The mock is intentionally simple: no DB, no persistence beyond an
in-memory idempotency cache. Keys live for the process lifetime.
"""

from __future__ import annotations

from .app import app, build_app

__all__ = ["app", "build_app"]
