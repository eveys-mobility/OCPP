"""Docker-compose service control helpers for DR drill scenarios.

The reconnect-storm scenario (E4-7) only kills *charger WebSockets*,
which the simulator can do on its own. The DR drill scenarios (E5-10)
need to kill **infrastructure**: Postgres, Redis, individual gateway
pods. That's a `docker compose stop / start` away — but no DR script
should reimplement the subprocess plumbing.

This module is the single place that knows where the compose file
lives and how to drive its services. Kept off the hot path by
importing it only from the DR scenarios; the rest of the load rig
doesn't need it.

Compose-only today. When a k3d / staging target arrives, add a
sibling helper `_kube_helpers.py` keyed on the same scenario inputs;
the scenario module's `run` function picks the right driver per
target.
"""

from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

# Compose file path resolved from this module's location, not CWD —
# the scenario can be invoked from anywhere.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE_FILE = _REPO_ROOT / "deploy" / "compose" / "docker-compose.yml"


class DockerNotAvailableError(RuntimeError):
    """Raised when `docker` isn't on PATH or the compose file is missing.

    The DR scenarios surface this as a clear `passed=False` criterion
    rather than crashing — running on a host without Docker is a
    legitimate operator state (e.g. a k3d-only environment), and the
    failure mode should be informative, not a stack trace.
    """


def _require_docker() -> None:
    if shutil.which("docker") is None:
        raise DockerNotAvailableError(
            "`docker` not on PATH. DR drill scenarios require docker compose."
        )
    if not _COMPOSE_FILE.exists():
        raise DockerNotAvailableError(f"compose file missing: {_COMPOSE_FILE}")


def stop(service: str, *, timeout_seconds: int = 10) -> None:
    """`docker compose stop <service>`. Returns when the container has
    exited; `timeout_seconds` is how long Docker waits for a graceful
    SIGTERM before sending SIGKILL.

    Used by the DR scenarios to simulate a hard infrastructure failure.
    The container stays down until `start()` is called.
    """
    _require_docker()
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(_COMPOSE_FILE),
            "stop",
            "-t",
            str(timeout_seconds),
            service,
        ],
        cwd=str(_REPO_ROOT),
        check=True,
        capture_output=True,
        text=True,
    )


def start(service: str) -> None:
    """`docker compose start <service>`. Returns when the start command
    has been accepted; the container may still be initializing — pair
    with `wait_healthy()` if the test depends on the service being
    responsive."""
    _require_docker()
    subprocess.run(
        ["docker", "compose", "-f", str(_COMPOSE_FILE), "start", service],
        cwd=str(_REPO_ROOT),
        check=True,
        capture_output=True,
        text=True,
    )


def wait_healthy(service: str, *, deadline_seconds: float = 60.0) -> bool:
    """Poll `docker inspect` until the service reports healthy or the
    deadline expires. Returns True on success.

    Compose's healthcheck is what we believe — a service that's "Up"
    but not yet healthy is not ready for the next phase of the drill
    (e.g. a Postgres that's accepting connections but hasn't applied
    its WAL yet would silently corrupt the durability check).
    """
    _require_docker()
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        proc = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(_COMPOSE_FILE),
                "ps",
                "--format",
                "json",
                service,
            ],
            cwd=str(_REPO_ROOT),
            check=False,
            capture_output=True,
            text=True,
        )
        # Compose emits one JSON object per service. Healthy state lives
        # under `Health` for services with a `healthcheck:`; for those
        # without (e.g. busybox helpers) we accept "running".
        out = proc.stdout.strip()
        # `ps --format json` may emit one JSON per line or a single
        # JSON; tolerate both. Substring match is robust against both
        # shapes and avoids dragging json into this helper. Empty `out`
        # falls through to the sleep — `in ""` is false for both arms.
        if '"Health":"healthy"' in out or ('"State":"running"' in out and '"Health"' not in out):
            return True
        time.sleep(1.0)
    return False
