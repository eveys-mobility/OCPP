"""Pure smoke-harness isolation checks; no Docker, listeners or subprocesses."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

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
