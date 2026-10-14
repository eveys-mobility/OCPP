"""Exercise the composite action's actual scripts with disposable Python venvs.

No uv, dependency installation, generated code, application or network is run.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import yaml

_ACTION = Path(__file__).parents[2] / ".github/actions/setup-python-venv/action.yml"


def _steps() -> list[dict]:
    return yaml.safe_load(_ACTION.read_text())["runs"]["steps"]


def _validate(tmp_path: Path) -> str:
    step = next((step for step in _steps() if step.get("id") == "validate-venv"), None)
    assert step is not None, "a cache hit needs interpreter validation before codegen"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "python").symlink_to(sys.executable)
    output = tmp_path / "github-output"
    subprocess.run(
        ["/bin/bash", "-eo", "pipefail", "-c", step["run"]],
        cwd=tmp_path, check=True, capture_output=True, text=True,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin", "GITHUB_OUTPUT": str(output)},
    )
    outputs = dict(line.split("=", 1) for line in output.read_text().splitlines())
    return outputs["valid"]


def test_broken_cached_python_requires_rebuild_even_on_cache_hit(tmp_path: Path) -> None:
    venv_bin = tmp_path / ".venv/bin"
    venv_bin.mkdir(parents=True)
    (venv_bin / "python").symlink_to(tmp_path / "removed-runner-python")
    assert _validate(tmp_path) == "false"
    build = next(step for step in _steps() if step.get("name", "").startswith("Build venv"))
    assert "steps.validate-venv.outputs.valid != 'true'" in build["if"]


def test_current_cached_venv_is_reusable_without_dependency_install(tmp_path: Path) -> None:
    subprocess.run(
        [sys.executable, "-I", "-S", "-m", "venv", "--without-pip", str(tmp_path / ".venv")],
        check=True, capture_output=True, text=True, env={"PATH": "/usr/bin:/bin"},
    )
    assert _validate(tmp_path) == "true"


def test_wrong_cached_interpreter_identity_requires_rebuild(tmp_path: Path) -> None:
    venv_bin = tmp_path / ".venv/bin"
    venv_bin.mkdir(parents=True)
    python = venv_bin / "python"
    python.write_text('#!/bin/sh\nprintf \'["3.13.14", "/removed/python", "wrong-abi"]\\n\'\n')
    python.chmod(0o700)
    assert _validate(tmp_path) == "false"


def test_venv_cache_bucket_changes_with_resolved_python_identity() -> None:
    key = next(step for step in _steps() if step.get("id") == "cache-venv")["with"]["key"]
    def render(identity: str) -> str:
        return re.sub(
            r"\$\{\{\s*(.*?)\s*\}\}",
            lambda match: (
                identity if match[1] == "steps.python-identity.outputs.cache-key" else "fixed"
            ),
            key,
        )
    assert render("resolved-3.13.14-path-abi") != render("resolved-3.13.16-path-abi")
    assert "runner.arch" in key


def test_python_identity_step_reports_executable_and_abi_cache_fingerprint(tmp_path: Path) -> None:
    step = next((step for step in _steps() if step.get("id") == "python-identity"), None)
    assert step is not None
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "python").symlink_to(sys.executable)
    output = tmp_path / "github-output"
    subprocess.run(
        ["/bin/bash", "-eo", "pipefail", "-c", step["run"]], cwd=tmp_path,
        check=True, capture_output=True, text=True,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin", "GITHUB_OUTPUT": str(output)},
    )
    outputs = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert outputs["executable"] == os.path.realpath(sys.executable)
    assert outputs["version"].startswith("3.13.")
    assert re.fullmatch(r"[0-9a-f]{64}", outputs["cache-key"])
