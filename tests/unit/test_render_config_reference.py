"""Snapshot tests for the configuration-reference generator (ADR-0025).

Three properties matter and are individually asserted:

1. **Snapshot**: `render_config_reference(Settings)` produces output
   byte-identical to `docs/11-configuration-reference.md` and
   `.env.example` on disk. Failing this is how a developer learns
   "you changed Settings, run `make config-export`" — it is the local
   equivalent of the E0-14 CI staleness gate.

2. **Determinism**: two calls produce the same output. Catches any
   accidental dependency on dict iteration order, time, env, or
   filesystem state.

3. **Unknown-category guard**: a synthetic `Settings` subclass that
   tags a field with a category not in the closed enum makes the
   generator raise — the closed-enum invariant is enforced at render
   time, not just at metadata-test time.
"""

from __future__ import annotations

# Adjacent path so tests run regardless of where pytest is invoked from.
import sys
from pathlib import Path

import pytest
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from eveys_ocpp.settings import Settings

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT / "scripts"))

from render_config_reference import (  # noqa: E402
    DOC_PATH,
    ENV_EXAMPLE_PATH,
    render_config_reference,
)


def test_generator_output_matches_committed_doc() -> None:
    markdown, _ = render_config_reference(Settings)
    on_disk = DOC_PATH.read_text()
    assert markdown == on_disk, (
        "docs/11-configuration-reference.md is out of date with Settings. "
        "Run `make config-export` to regenerate."
    )


def test_generator_output_matches_committed_env_example() -> None:
    _, env_example = render_config_reference(Settings)
    on_disk = ENV_EXAMPLE_PATH.read_text()
    assert env_example == on_disk, (
        ".env.example is out of date with Settings. Run `make config-export` to regenerate."
    )


def test_generator_is_deterministic() -> None:
    a = render_config_reference(Settings)
    b = render_config_reference(Settings)
    assert a == b


def test_unknown_category_is_rejected() -> None:
    class BadSettings(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="X_")

        weird: str = Field(
            default="x",
            description="placeholder",
            json_schema_extra={
                "category": "definitely_not_a_real_category",
                "impact": "n/a",
                "secret": False,
                "stability": "tunable",
            },
        )

    with pytest.raises(ValueError, match="unknown category"):
        render_config_reference(BadSettings)
