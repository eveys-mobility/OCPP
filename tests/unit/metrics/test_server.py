"""End-to-end shape test for the metrics scrape server.

Boots a `MetricsServer` on an ephemeral host port, scrapes `/metrics`
with the stdlib `urllib`, and asserts every `eveys_ocpp_*` metric the
registry defines is present in the response. This is the "stable
schema" contract from `metrics/registry.py`'s docstring — it must
hold from the first scrape onward.

The test deliberately bypasses the autouse `_disable_metrics_server`
fixture by constructing a `MetricsServer` directly (the fixture only
flips the env var that the boot path reads).
"""

from __future__ import annotations

import socket
import urllib.request
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from prometheus_client import Counter, Gauge, Histogram

from eveys_ocpp.metrics import MetricsServer
from eveys_ocpp.metrics import registry as m


def _ephemeral_port() -> int:
    """Pick a port the kernel hands us right now, then close so the
    MetricsServer can bind it. Race-y in theory; safe enough in practice
    for a single-test boot/shutdown cycle."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest_asyncio.fixture
async def running_metrics_server() -> AsyncIterator[tuple[MetricsServer, int]]:
    port = _ephemeral_port()
    server = MetricsServer(host="127.0.0.1", port=port)
    await server.start()
    try:
        yield server, port
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_scrape_returns_every_inventory_metric(
    running_metrics_server: tuple[MetricsServer, int],
) -> None:
    """The first scrape returns every `eveys_ocpp_*` metric — the
    stable-schema property."""
    _server, port = running_metrics_server

    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as resp:
        body = resp.read().decode()

    # Build the set of expected wire names from the registry module —
    # mirrors the same iteration logic the inventory test uses.
    expected: set[str] = set()
    for name in dir(m):
        if name.startswith("_") or "BUCKETS" in name:
            continue
        attr = getattr(m, name)
        if isinstance(attr, Counter | Gauge | Histogram):
            expected.add(attr._name)  # type: ignore[attr-defined]

    missing = [name for name in expected if name not in body and f"{name}_bucket" not in body]
    assert not missing, f"first scrape is missing metric(s): {missing}"


@pytest.mark.asyncio
async def test_scrape_starts_with_help_text(
    running_metrics_server: tuple[MetricsServer, int],
) -> None:
    """Sanity check on the wire format — the scrape begins with HELP
    lines (Prometheus exposition format)."""
    _server, port = running_metrics_server
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as resp:
        body = resp.read().decode()
    assert body.startswith("# HELP "), body[:120]


@pytest.mark.asyncio
async def test_double_start_is_idempotent() -> None:
    """`start()` after a previous successful start is a no-op — the
    port stays bound to the original listener and we don't crash on
    `address already in use`."""
    port = _ephemeral_port()
    server = MetricsServer(host="127.0.0.1", port=port)
    await server.start()
    try:
        # Second start must not raise.
        await server.start()
        # And the listener still answers.
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as resp:
            assert resp.status == 200
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_double_stop_is_idempotent() -> None:
    """`stop()` after a previous stop is a no-op."""
    server = MetricsServer(host="127.0.0.1", port=_ephemeral_port())
    await server.start()
    await server.stop()
    await server.stop()  # must not raise
