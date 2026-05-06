"""Standalone-process smoke test for the mock backend (E3-10).

Boots `python -m tests.mock_backend` as a real subprocess on a free
local port, hits every endpoint over real HTTP, and verifies the
canonical envelope shape. Catches issues the in-process ASGI tests
in `tests/unit/mock_backend/` miss:

- uvicorn import + startup
- ``__main__`` CLI flag handling
- Real socket binding (so a port-conflict bug is caught here, not
  in production)
- Header pass-through across the actual HTTP stack

This is what the CI's ``tests:mock-backend`` job runs. Lives in
``tests/smoke/`` so the unit-test job (`pytest tests/unit/`) doesn't
spawn subprocesses — those are slower and need real socket access.

The test is also useful for the backend developer as a
contract-validation reference: every endpoint here is something
*their* implementation must also satisfy.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import closing

import httpx
import pytest

_TOKEN = "smoke-token"
_AUTH_HEADER = {"Authorization": f"Bearer {_TOKEN}"}


def _free_port() -> int:
    """Bind to an OS-assigned port and immediately release it."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_for_port(port: int, timeout_seconds: float = 10.0) -> None:
    """Poll until the mock backend is accepting connections."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.settimeout(0.2)
            try:
                s.connect(("127.0.0.1", port))
                return
            except OSError:
                time.sleep(0.1)
    raise RuntimeError(f"mock backend never opened port {port} within {timeout_seconds}s")


@pytest.fixture(scope="module")
def mock_backend_url() -> Iterator[str]:
    """Boot `python -m tests.mock_backend` on a free port for the
    module's duration; tear it down at the end."""
    port = _free_port()
    env = {
        **os.environ,
        "MOCK_BACKEND_HOST": "127.0.0.1",
        "MOCK_BACKEND_PORT": str(port),
        "MOCK_BACKEND_TOKEN": _TOKEN,
        "MOCK_BACKEND_BLOCKED_ID_TAGS": "RFID_BLOCKED_SMOKE",
        "MOCK_BACKEND_LOG_LEVEL": "warning",  # cut noise from the test log
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "tests.mock_backend"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_port(port)
        yield f"http://127.0.0.1:{port}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


# ---- the contract surface --------------------------------------------------


def test_health_returns_envelope(mock_backend_url: str) -> None:
    response = httpx.get(f"{mock_backend_url}/api/eveys/health", timeout=5)
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["status"] == "ok"
    assert "version" in body["data"]


def test_health_passes_through_request_id_header(mock_backend_url: str) -> None:
    """X-Request-ID round-trips both ways across the real HTTP stack."""
    rid = "smoke-request-id-42"
    response = httpx.get(
        f"{mock_backend_url}/api/eveys/health",
        headers={"X-Request-ID": rid},
        timeout=5,
    )
    assert response.headers["x-request-id"] == rid
    assert response.json()["data"]["request_id"] == rid


def test_authorize_rejects_missing_bearer(mock_backend_url: str) -> None:
    response = httpx.post(
        f"{mock_backend_url}/api/eveys/authorize",
        json={"id_tag": "RFID_X", "cp_id": "CP_001"},
        timeout=5,
    )
    assert response.status_code == 401
    body = response.json()
    assert body["success"] is False
    assert body["error_code"] == "UNAUTHORIZED"


def test_authorize_accepts_with_valid_token(mock_backend_url: str) -> None:
    response = httpx.post(
        f"{mock_backend_url}/api/eveys/authorize",
        headers=_AUTH_HEADER,
        json={"id_tag": "RFID_HAPPY", "cp_id": "CP_001"},
        timeout=5,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["id_tag_info"]["status"] == "Accepted"


def test_authorize_blocks_configured_id_tag(mock_backend_url: str) -> None:
    """The fixture set ``MOCK_BACKEND_BLOCKED_ID_TAGS=RFID_BLOCKED_SMOKE``,
    so this id_tag must come back ``Blocked``."""
    response = httpx.post(
        f"{mock_backend_url}/api/eveys/authorize",
        headers=_AUTH_HEADER,
        json={"id_tag": "RFID_BLOCKED_SMOKE", "cp_id": "CP_001"},
        timeout=5,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["id_tag_info"]["status"] == "Blocked"


def test_sessions_open_and_close(mock_backend_url: str) -> None:
    """End-to-end open + close, simulating a real charging session."""
    open_response = httpx.post(
        f"{mock_backend_url}/api/eveys/sessions/open",
        headers=_AUTH_HEADER,
        json={
            "transaction_id": 9001,
            "cp_id": "CP_SMOKE",
            "connector_id": 1,
            "id_tag": "RFID_HAPPY",
            "meter_start_wh": 0,
            "started_reported_at": "2026-05-05T14:00:00+00:00",
        },
        timeout=5,
    )
    assert open_response.status_code == 200
    assert open_response.json()["data"]["transaction_id"] == 9001

    close_response = httpx.post(
        f"{mock_backend_url}/api/eveys/sessions/close",
        headers=_AUTH_HEADER,
        json={
            "transaction_id": 9001,
            "cp_id": "CP_SMOKE",
            "id_tag": "RFID_HAPPY",
            "meter_stop_wh": 12345,
            "stopped_reported_at": "2026-05-05T14:30:00+00:00",
            "stop_reason": "Local",
        },
        timeout=5,
    )
    assert close_response.status_code == 200
    assert close_response.json()["data"]["transaction_id"] == 9001


def test_charge_point_register(mock_backend_url: str) -> None:
    response = httpx.post(
        f"{mock_backend_url}/api/eveys/charge-points/register",
        headers=_AUTH_HEADER,
        json={
            "cp_id": "CP_SMOKE_NEW",
            "vendor": "ACME",
            "model": "X1",
            "boot_at": "2026-05-05T14:00:00+00:00",
        },
        timeout=5,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["data"]["registration_status"] == "Accepted"
    assert isinstance(body["data"]["heartbeat_interval_seconds"], int)


def test_idempotency_replay_returns_same_response(mock_backend_url: str) -> None:
    """Replay the exact same body with the same Idempotency-Key —
    second response is byte-equal to the first."""
    headers = {**_AUTH_HEADER, "Idempotency-Key": "smoke-replay-1"}
    body = {
        "transaction_id": 9999,
        "cp_id": "CP_SMOKE",
        "connector_id": 1,
        "id_tag": "RFID_HAPPY",
        "meter_start_wh": 0,
        "started_reported_at": "2026-05-05T14:00:00+00:00",
    }
    first = httpx.post(
        f"{mock_backend_url}/api/eveys/sessions/open",
        headers=headers,
        json=body,
        timeout=5,
    )
    second = httpx.post(
        f"{mock_backend_url}/api/eveys/sessions/open",
        headers=headers,
        json=body,
        timeout=5,
    )
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()


def test_idempotency_conflict_when_body_differs(mock_backend_url: str) -> None:
    """Same key + different body → 409 Conflict per the contract."""
    headers = {**_AUTH_HEADER, "Idempotency-Key": "smoke-conflict-1"}
    first = httpx.post(
        f"{mock_backend_url}/api/eveys/authorize",
        headers=headers,
        json={"id_tag": "RFID_A", "cp_id": "CP_001"},
        timeout=5,
    )
    second = httpx.post(
        f"{mock_backend_url}/api/eveys/authorize",
        headers=headers,
        json={"id_tag": "RFID_B", "cp_id": "CP_001"},
        timeout=5,
    )
    assert first.status_code == 200
    assert second.status_code == 409
    assert second.json()["error_code"] == "IDEMPOTENCY_CONFLICT"
