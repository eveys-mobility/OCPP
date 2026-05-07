"""Prometheus client — instant + range queries against canned httpx
responses. We don't pull a real Prometheus into the unit suite.

Each test patches `httpx.Client` to return a real `httpx.Client`
backed by a `MockTransport` so the request/response flow stays
authentic; only the wire is short-circuited.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest
from tools.load.prometheus import PrometheusClient


def _patch_client(
    monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]
) -> None:
    """Replace `httpx.Client(...)` with one that routes through the
    given handler. Strips out `transport=` from kwargs first because
    the production code doesn't pass one and we want to inject ours."""
    transport = httpx.MockTransport(handler)
    real_client = httpx.Client

    def _factory(**kwargs: object) -> httpx.Client:
        kwargs.pop("transport", None)
        return real_client(transport=transport, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(httpx, "Client", _factory)


def test_instant_returns_result_list_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canned = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [{"metric": {}, "value": [1234567890.0, "1.5"]}],
        },
    }

    def _handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/query"
        assert request.url.params["query"] == "up"
        return httpx.Response(200, json=canned)

    _patch_client(monkeypatch, _handler)
    client = PrometheusClient(base_url="http://example.invalid")
    result = client.instant("up")
    assert result == canned["data"]["result"]


def test_instant_raises_on_prometheus_error_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prometheus returns 200 with `{"status": "error"}` on bad
    queries — we surface that as a RuntimeError so callers don't
    silently get an empty result."""
    canned = {"status": "error", "errorType": "bad_data", "error": "parse error"}

    _patch_client(monkeypatch, lambda _r: httpx.Response(200, json=canned))
    client = PrometheusClient(base_url="http://example.invalid")
    with pytest.raises(RuntimeError, match="Prometheus error"):
        client.instant("not[a[query")


def test_range_passes_start_end_step_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_params: dict[str, str] = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        seen_params.update(dict(request.url.params))
        return httpx.Response(200, json={"status": "success", "data": {"result": []}})

    _patch_client(monkeypatch, _handler)
    client = PrometheusClient(base_url="http://example.invalid")
    client.range("up", start=100.0, end=200.0, step_seconds=10.0)
    assert seen_params["query"] == "up"
    assert seen_params["start"] == "100.0"
    assert seen_params["end"] == "200.0"
    assert seen_params["step"] == "10.0"
