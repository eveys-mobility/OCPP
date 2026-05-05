"""Conftest for the Tier-3 compose-smoke layer (ADR-0024).

Owns the lifecycle of the **production-shaped** docker compose stack
defined at ``deploy/compose/docker-compose.yml``: the very same compose
file that ships, built from the very same Dockerfile that ships. No
mocking, no GitLab `services:` shortcut, no in-process fixtures.

The fixtures here exist for one reason: to fail loud when a deploy-time
wiring change (env var, listener, healthcheck, entrypoint) breaks the
stack on startup. See ADR-0024 §"Tier 3" for the rationale.

Single-stack scope: the compose project comes up once per pytest
session and is torn down at the end. Tests within a session share the
stack — they cannot mutate it in destructive ways. If you need a clean
DB row count, query for what you wrote, do not assume an empty table.

Tests in this layer skip when Docker is not installed or `COMPOSE_SMOKE`
is unset, EXCEPT in CI where ``COMPOSE_SMOKE_REQUIRE=1`` flips a skip
into a hard failure — a green-but-skipped pipeline is the original
class of bug this layer was added to prevent.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path

import pytest

# Resolve compose file relative to the repo root, not CWD — pytest may
# invoke us from anywhere.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE_FILE = _REPO_ROOT / "deploy" / "compose" / "docker-compose.yml"

# How long after a container is "Up" we wait before re-checking
# `restart_count`. A container that exits a few seconds in is a startup
# bug, not a healthy steady state. 15 s catches the slow-crash class
# without bloating the CI budget.
_STABILISATION_SECONDS = 15

# Per-container deadlines for first reaching "Up". Kafka takes the
# longest because of KRaft controller bootstrap.
_BOOT_TIMEOUT_SECONDS = 90

# Containers we expect to see running. Order roughly matches startup
# dependency. The names are pinned by `container_name:` in the compose
# file — they do NOT change with COMPOSE_PROJECT_NAME.
_EXPECTED_CONTAINERS = (
    "eveys-ocpp-postgres",
    "eveys-ocpp-redis",
    "eveys-ocpp-kafka",
    "eveys-ocpp-clickhouse",
    "eveys-ocpp-clickhouse-ingestor",
    "eveys-ocpp",
)

# Host ports the compose file publishes. Used by tests that drive the
# real WS / gRPC / DB / ClickHouse from outside the docker network.
HOST_WS_PORT = 19000
HOST_PG_PORT = 5432
HOST_CH_HTTP_PORT = 8123

# Where the published ports actually answer. On a laptop this is
# `localhost`. Under GitLab Docker-in-Docker the test process and the
# Docker daemon live in different network namespaces — the daemon
# publishes ports on the dind sidecar, which the job container reaches
# via the service alias (`docker` by default). CI sets this to
# `docker`; defaults stay laptop-friendly.
PUBLISHED_HOST = os.environ.get("COMPOSE_SMOKE_PUBLISHED_HOST", "localhost")

# Host-side DSN for alembic to run against the compose Postgres. Inside
# the docker network the gateway uses `postgres:5432`; from the test
# process we go via the published port.
_PG_DSN = f"postgresql+asyncpg://eveys:eveys@{PUBLISHED_HOST}:{HOST_PG_PORT}/eveys_ocpp"


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _gate() -> None:
    """Skip rules. CI sets COMPOSE_SMOKE_REQUIRE=1 to refuse skips."""
    require = os.environ.get("COMPOSE_SMOKE_REQUIRE") == "1"
    enabled = os.environ.get("COMPOSE_SMOKE") == "1" or require

    if not _docker_available():
        msg = "docker CLI not found"
        if require:
            raise RuntimeError(f"COMPOSE_SMOKE_REQUIRE=1 but {msg}")
        pytest.skip(msg, allow_module_level=True)

    if not enabled:
        # Local-laptop default: the smoke tier needs an opt-in because
        # `make tests` should never spend 90 s building a Docker image
        # the developer might not even have built. `make compose-smoke`
        # sets COMPOSE_SMOKE=1.
        pytest.skip(
            "compose-smoke skipped: set COMPOSE_SMOKE=1 (or run `make compose-smoke`)",
            allow_module_level=True,
        )


def _compose(*args: str) -> subprocess.CompletedProcess[str]:
    """Run `docker compose -f <file> <args>` from the repo root."""
    return subprocess.run(
        ["docker", "compose", "-f", str(_COMPOSE_FILE), *args],
        cwd=str(_REPO_ROOT),
        check=False,
        capture_output=True,
        text=True,
    )


def _container_state(name: str) -> dict[str, str]:
    """Return ``State`` + ``RestartCount`` from `docker inspect`. Empty
    dict if the container does not exist."""
    proc = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            "{{.State.Status}}|{{.State.ExitCode}}|{{.RestartCount}}|{{.State.Running}}",
            name,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return {}
    status, exit_code, restart_count, running = proc.stdout.strip().split("|")
    return {
        "status": status,
        "exit_code": exit_code,
        "restart_count": restart_count,
        "running": running,
    }


def _wait_for_running(name: str, timeout_s: int) -> dict[str, str]:
    """Block until a container reports ``Running == true`` or the
    timeout elapses. Returns the final state. Raises on timeout."""
    deadline = time.monotonic() + timeout_s
    last: dict[str, str] = {}
    while time.monotonic() < deadline:
        last = _container_state(name)
        if last.get("running") == "true":
            return last
        if last.get("status") in {"exited", "dead"}:
            # Fast-fail: a container that already exited won't recover.
            return last
        time.sleep(1)
    return last


def _wait_for_tcp(host: str, port: int, timeout_s: int = 30) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.settimeout(1)
            try:
                s.connect((host, port))
                return True
            except OSError:
                time.sleep(0.5)
    return False


def _container_logs(name: str, tail: int = 200) -> str:
    proc = subprocess.run(
        ["docker", "logs", "--tail", str(tail), name],
        check=False,
        capture_output=True,
        text=True,
    )
    return (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")


@pytest.fixture(scope="session", autouse=True)
def _compose_stack() -> Iterator[None]:
    """Bring the full stack up once per pytest session.

    Tear-down on success AND failure — a leftover stack is what makes
    the next session's "first run" mysteriously broken.
    """
    _gate()

    # Clean slate. Downing here is cheap if no project exists; it
    # prevents stale containers from a prior aborted run masking an
    # actual startup bug.
    _compose("down", "--volumes", "--remove-orphans")

    up = _compose("up", "-d", "--build")
    if up.returncode != 0:
        pytest.fail(f"docker compose up failed:\n  stdout: {up.stdout}\n  stderr: {up.stderr}")

    # Phase 1: every container is at least Running.
    failures: list[str] = []
    for name in _EXPECTED_CONTAINERS:
        state = _wait_for_running(name, _BOOT_TIMEOUT_SECONDS)
        if state.get("running") != "true":
            failures.append(
                f"{name}: status={state.get('status', '?')} "
                f"exit={state.get('exit_code', '?')} "
                f"restart_count={state.get('restart_count', '?')}\n"
                f"--- last 200 log lines ---\n{_container_logs(name)}"
            )
    if failures:
        _compose("down", "--volumes", "--remove-orphans")
        pytest.fail("containers failed to reach Running state:\n\n" + "\n\n".join(failures))

    # Phase 2: stabilisation window. A container that exits 10 s into
    # life is a startup bug; this catches it. We sleep ONCE for the
    # window, then re-check every container in one pass — total cost is
    # `_STABILISATION_SECONDS`, not N * window.
    time.sleep(_STABILISATION_SECONDS)
    failures = []
    for name in _EXPECTED_CONTAINERS:
        state = _container_state(name)
        if state.get("running") != "true":
            failures.append(
                f"{name} exited during stabilisation: "
                f"status={state.get('status')} exit={state.get('exit_code')}\n"
                f"--- last 200 log lines ---\n{_container_logs(name)}"
            )
            continue
        # `restart_count > 0` means docker had to bring it back at
        # least once — i.e., the process crashed and got restarted.
        # That is NOT a healthy steady state, even if the container
        # is currently "Up".
        rc = int(state.get("restart_count") or "0")
        if rc > 0:
            failures.append(
                f"{name} has restart_count={rc} after stabilisation — "
                f"the container is crash-looping, not healthy.\n"
                f"--- last 200 log lines ---\n{_container_logs(name)}"
            )
    if failures:
        _compose("down", "--volumes", "--remove-orphans")
        pytest.fail("containers churned after startup:\n\n" + "\n\n".join(failures))

    # Phase 3: published host ports answer. Catches the case where the
    # container is "Up" but the inner process never bound its socket.
    if not _wait_for_tcp(PUBLISHED_HOST, HOST_WS_PORT, timeout_s=15):
        _compose("down", "--volumes", "--remove-orphans")
        pytest.fail(
            f"WS port {HOST_WS_PORT} not reachable after gateway 'Up'.\n"
            f"--- gateway logs ---\n{_container_logs('eveys-ocpp')}"
        )

    # Phase 4: apply schema. The conftest started by wiping volumes so
    # we get a clean run; that means Postgres + ClickHouse are empty
    # right now and the gateway will fail any DB-touching call until
    # Alembic + the ClickHouse migrator finish. We do this here so the
    # tier owns its full setup — no orchestration leak into the
    # Makefile target.
    alembic = subprocess.run(
        [".venv/bin/alembic", "upgrade", "head"],
        cwd=str(_REPO_ROOT),
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "EVEYS_OCPP_DB_URL": _PG_DSN},
    )
    if alembic.returncode != 0:
        _compose("down", "--volumes", "--remove-orphans")
        pytest.fail(
            f"alembic upgrade head failed:\n  stdout: {alembic.stdout}\n  stderr: {alembic.stderr}"
        )

    chmig = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "eveys_ocpp.clickhouse.migrate",
            "--host",
            PUBLISHED_HOST,
            "--port",
            str(HOST_CH_HTTP_PORT),
            "--db",
            "eveys_ocpp",
        ],
        cwd=str(_REPO_ROOT),
        check=False,
        capture_output=True,
        text=True,
    )
    if chmig.returncode != 0:
        _compose("down", "--volumes", "--remove-orphans")
        pytest.fail(
            f"clickhouse migrator failed:\n  stdout: {chmig.stdout}\n  stderr: {chmig.stderr}"
        )

    yield

    # Always tear down — leaving a stack up between sessions makes the
    # next "fresh" run mysteriously broken when a config change lands.
    _compose("down", "--volumes", "--remove-orphans")
