"""Pure smoke-harness isolation checks; no Docker, listeners or subprocesses."""

from __future__ import annotations

import ast
import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml
from ocpp.messages import Call, validate_payload

from eveys_ocpp.settings import Settings
from tests.compose_smoke import conftest as smoke
from tests.mock_backend.client import in_process_backend_client


def test_smoke_compose_uses_test_backend_override_without_loading_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".env").write_text("EVEYS_OCPP_BACKEND_BASE_URL=https://invalid.example\n")
    monkeypatch.setattr(smoke, "_REPO_ROOT", tmp_path)
    run = Mock(return_value=subprocess.CompletedProcess([], 0, "", ""))
    monkeypatch.setattr(smoke.subprocess, "run", run)

    smoke._compose("config")

    command = run.call_args.args[0]
    assert "--env-file" in command
    assert command[command.index("--env-file") + 1] == "/dev/null"
    assert any(str(path).endswith("docker-compose.mock-backend.yml") for path in command)
    assert str(tmp_path / ".env") not in command


def test_smoke_compose_drops_inherited_gateway_and_mock_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EVEYS_OCPP_BACKEND_BASE_URL", "https://invalid.example")
    monkeypatch.setenv("EVEYS_OCPP_WEBHOOK_BASE_URL", "https://invalid.example/hooks")
    monkeypatch.setenv("EVEYS_OCPP_WEBHOOK_URL_TX_STOPPED", "https://invalid.example/stop")
    monkeypatch.setenv("EVEYS_OCPP_REST_INBOUND_TOKENS", "inherited-token")
    monkeypatch.setenv("MOCK_BACKEND_FAIL_AUTHORIZE", "1")
    run = Mock(return_value=subprocess.CompletedProcess([], 0, "", ""))
    monkeypatch.setattr(smoke.subprocess, "run", run)

    smoke._compose("config")

    child_env = run.call_args.kwargs["env"]
    assert not any(key.startswith(("EVEYS_OCPP_", "MOCK_BACKEND_")) for key in child_env)


def test_smoke_refuses_existing_containers_before_destructive_test_setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(smoke, "_container_state", lambda _name: {"status": "running"})
    with pytest.raises(RuntimeError, match="disposable Docker environment"):
        smoke._require_disposable_stack()


def test_smoke_overlay_wires_existing_mock_contract_with_strict_auth() -> None:
    services = yaml.safe_load(smoke._MOCK_COMPOSE_FILE.read_text())["services"]
    backend = services["mock-backend"]
    gateway = services["ocpp"]
    assert backend["command"][:2] == ["-m", "tests.mock_backend"]
    assert "ports" not in backend
    assert gateway["environment"]["EVEYS_OCPP_BACKEND_BASE_URL"] == "http://mock-backend:9200/api/eveys"
    assert (
        gateway["environment"]["EVEYS_OCPP_BACKEND_TOKEN"]
        == backend["environment"]["MOCK_BACKEND_TOKEN"]
    )
    assert gateway["environment"]["EVEYS_OCPP_BACKEND_AUTHORIZE_FALLBACK"] == "reject"
    assert gateway["environment"]["EVEYS_OCPP_WEBHOOK_BASE_URL"] == ""
    assert gateway["depends_on"]["mock-backend"]["condition"] == "service_healthy"


@pytest.mark.asyncio
async def test_smoke_blocked_tag_is_valid_ocpp_authorize_wire_data() -> None:
    services = yaml.safe_load(smoke._MOCK_COMPOSE_FILE.read_text())["services"]
    tag = services["mock-backend"]["environment"]["MOCK_BACKEND_BLOCKED_ID_TAGS"]
    tree = ast.parse((smoke._REPO_ROOT / "tests/compose_smoke/test_compose_stack.py").read_text())
    tags = {
        keyword.value.value for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and node.func.attr == "Authorize"
        for keyword in node.keywords
        if keyword.arg == "id_tag" and isinstance(keyword.value, ast.Constant)
    }
    assert tag in tags, "the mock's blocked tag must match the actual simulator request"
    for value in tags:
        await validate_payload(Call("isolated-tag", "Authorize", {"idTag": value}), "1.6")


def test_native_fixture_keeps_bounded_logs_before_removing_test_containers(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = []
    monkeypatch.setattr(smoke, "_REPO_ROOT", tmp_path)
    monkeypatch.setattr(smoke, "_gate", lambda: None)
    monkeypatch.setattr(smoke, "_require_disposable_stack", lambda: None)
    monkeypatch.setattr(smoke.time, "sleep", lambda _seconds: None)
    state = {"running": "true", "restart_count": "0"}
    monkeypatch.setattr(smoke, "_wait_for_running", lambda *_args: state)
    monkeypatch.setattr(smoke, "_container_state", lambda _name: state)
    monkeypatch.setattr(smoke, "_wait_for_tcp", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(smoke.subprocess, "run", lambda *_args, **_kwargs:
                        subprocess.CompletedProcess([], 0, "", ""))
    def compose(*args):
        events.append(args[0])
        return subprocess.CompletedProcess([], 0, "", "")
    def logs(name, tail=200):
        events.append("logs")
        assert tail == 200
        return f"synthetic fixture log for {name}"
    monkeypatch.setattr(smoke, "_compose", compose)
    monkeypatch.setattr(smoke, "_container_logs", logs)
    fixture = smoke._compose_stack.__wrapped__()
    next(fixture)
    with pytest.raises(StopIteration):
        next(fixture)
    assert events[-1] == "down"
    assert "logs" in events[:-1]
    assert (tmp_path / ".compose-smoke-logs/fixture-eveys-ocpp.log").read_text().startswith(
        "synthetic fixture log"
    )


@pytest.mark.asyncio
async def test_native_fixture_client_uses_real_mock_contract_without_outbound_network() -> None:
    client = in_process_backend_client(Settings(
        _env_file=None, backend_base_url="https://invalid.example", backend_token="inert-only",
    ))
    try:
        allowed = await client.authorize(cp_id="SMOKE_CP", id_tag="VALID_RFID_001")
        blocked = await client.authorize(cp_id="SMOKE_CP", id_tag="SMOKE_BLOCKED")
        assert allowed.id_tag_info.status == "Accepted"
        assert blocked.id_tag_info.status == "Blocked"
        opened = await client.open_session(
            transaction_id=7001, cp_id="SMOKE_CP", connector_id=1, id_tag="VALID_RFID_001",
            meter_start_wh=0, started_reported_at="2026-10-14T00:00:00Z",
            idempotency_key="isolated-open-7001",
        )
        assert opened.id_tag_info.status == "Accepted"
        assert opened.transaction_id == 7001
        closed = await client.close_session(
            transaction_id=7001, cp_id="SMOKE_CP", id_tag="VALID_RFID_001",
            meter_stop_wh=12345, stopped_reported_at="2026-10-14T00:01:00Z",
            idempotency_key="isolated-close-7001",
        )
        assert closed.id_tag_info.status == "Accepted"
        assert closed.transaction_id == 7001
    finally:
        await client.aclose()
