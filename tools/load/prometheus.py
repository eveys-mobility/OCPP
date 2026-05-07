"""Thin Prometheus HTTP API client — instant + range queries only.

Doesn't pull `prometheus-api-client` because the only thing we need
is `GET /api/v1/query` and `GET /api/v1/query_range`, both of which
are 20 lines of httpx. Fewer deps = fewer surprises in CI.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True, slots=True)
class PrometheusClient:
    """Sync (blocking) client. The load test runs after the
    simulator finishes, not concurrently with traffic, so blocking
    HTTP is fine here — and it keeps the report generator usable
    from a script without an event loop."""

    base_url: str  # e.g. `http://localhost:9090`
    timeout_seconds: float = 10.0

    def instant(self, query: str) -> Any:
        """Run a Prometheus instant query, return the raw `result`
        list from the API envelope. Empty list when the query
        matches no series."""
        response = self._get("/api/v1/query", params={"query": query})
        data = response.get("data") or {}
        return data.get("result") or []

    def range(
        self,
        query: str,
        *,
        start: float,
        end: float,
        step_seconds: float,
    ) -> Any:
        """Range query. `start`/`end` are unix epoch floats; `step_seconds`
        is the resolution. Returns the raw `result` list (one entry per
        matched series, each carrying a `values` list of `[ts, value]`)."""
        response = self._get(
            "/api/v1/query_range",
            params={
                "query": query,
                "start": str(start),
                "end": str(end),
                "step": str(step_seconds),
            },
        )
        data = response.get("data") or {}
        return data.get("result") or []

    def _get(self, path: str, *, params: dict[str, str]) -> dict[str, Any]:
        with httpx.Client(base_url=self.base_url, timeout=self.timeout_seconds) as client:
            response = client.get(path, params=params)
            response.raise_for_status()
            payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected Prometheus response: {payload!r}")
        if payload.get("status") != "success":
            raise RuntimeError(f"Prometheus error: {payload.get('error') or payload}")
        return payload
