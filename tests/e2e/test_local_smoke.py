"""End-to-end smoke test against `make compose-up` data plane.

Skipped when Postgres / Redis / Kafka / ClickHouse aren't reachable —
this lets `make tests` stay green on machines without the stack running.
Run explicitly with `make smoke` once the stack is up.
"""

from __future__ import annotations

import socket
from collections.abc import Iterator
from contextlib import closing

import pytest


def _can_connect(host: str, port: int, timeout: float = 0.5) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.settimeout(timeout)
        try:
            s.connect((host, port))
            return True
        except OSError:
            return False


pytestmark = pytest.mark.skipif(
    not all(
        [
            _can_connect("localhost", 5432),  # Postgres
            _can_connect("localhost", 6379),  # Redis
            _can_connect("localhost", 9092),  # Kafka
            _can_connect("localhost", 8123),  # ClickHouse HTTP
        ]
    ),
    reason="local stack not reachable — run `make compose-up` first",
)


@pytest.fixture
def compose_endpoints() -> Iterator[dict[str, str]]:
    yield {
        "postgres": "localhost:5432",
        "redis": "localhost:6379",
        "kafka": "localhost:9092",
        "clickhouse": "localhost:8123",
    }


def test_each_endpoint_is_reachable(compose_endpoints: dict[str, str]) -> None:
    for name, addr in compose_endpoints.items():
        host, port = addr.split(":")
        assert _can_connect(host, int(port)), f"{name} not reachable at {addr}"


def test_clickhouse_responds_ok() -> None:
    """ClickHouse `/ping` must return `Ok.\\n`."""
    import urllib.request

    with urllib.request.urlopen("http://localhost:8123/ping", timeout=2) as resp:
        body = resp.read().decode()
    assert body.strip() == "Ok."
