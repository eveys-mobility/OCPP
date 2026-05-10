"""Helm chart ↔ Settings consistency (Phase 6 pre-staging gate).

The chart at ``deploy/helm/eveys-ocpp/`` references ``EVEYS_OCPP_*``
env vars across templates, values.yaml, Chart.yaml, and inline
comments. A typo or a reference to a removed Settings field renders
fine through ``helm lint`` / ``helm template`` — the chart still
templates valid YAML, the pod still boots, and the gateway silently
falls back to the Settings default. Operator opens Grafana, panel
graphs flat-line, root cause is a typo in the chart that survived
review and CI both.

This test parses every file under ``deploy/helm/``, extracts every
``EVEYS_OCPP_*`` token, strips the prefix + lowercases, and asserts
each is a real field on ``Settings``.

Out of scope:

- Opposite-direction check (every Settings field is referenced in
  the chart). Most fields have sensible defaults the operator
  doesn't need to override; not a contract violation.
- Type / value validation (the chart sets all envs as strings; the
  Settings constructor validates at gateway boot).
- compose-side env wiring (``deploy/compose/docker-compose.yml``);
  similar shape, separate scan if it matters later.
"""

from __future__ import annotations

import re
from pathlib import Path

from eveys_ocpp.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[2]
HELM_DIR = REPO_ROOT / "deploy" / "helm"

# `EVEYS_OCPP_<UPPER_SNAKE>` — anchored on a final letter so a glob-stem
# in a comment (`EVEYS_OCPP_WS_MTLS_*` becomes the trailing `_` once
# the splat is stripped) doesn't match. That's intentional: globs
# document a family, they aren't a single field reference.
_ENV_TOKEN = re.compile(r"\bEVEYS_OCPP_[A-Z][A-Z0-9_]*[A-Z0-9]\b")


def _all_env_tokens_in_chart() -> dict[str, set[str]]:
    """Return ``{token: {file_path, ...}}`` for every ``EVEYS_OCPP_*``
    string anywhere under the chart directory. Tracks origin paths so
    a failure can name the offending file."""
    out: dict[str, set[str]] = {}
    for path in HELM_DIR.rglob("*"):
        if not path.is_file():
            continue
        # Skip binary blobs (charts can ship icons or `.tgz` deps); the
        # decode-or-skip catches anything that isn't UTF-8 text.
        try:
            content = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for match in _ENV_TOKEN.finditer(content):
            token = match.group(0)
            out.setdefault(token, set()).add(str(path.relative_to(REPO_ROOT)))
    return out


def _settings_field_names_as_envs() -> set[str]:
    """Every valid ``EVEYS_OCPP_<FIELD>`` for the live Settings model."""
    prefix = Settings.model_config.get("env_prefix", "")
    return {f"{prefix}{name.upper()}" for name in Settings.model_fields}


def test_chart_dir_exists_and_has_files() -> None:
    """If this fails, someone moved or deleted the chart and every
    assertion below would vacuously pass."""
    assert HELM_DIR.is_dir(), HELM_DIR
    files = [p for p in HELM_DIR.rglob("*") if p.is_file()]
    assert len(files) >= 5, f"only {len(files)} files under {HELM_DIR}"


def test_some_env_tokens_were_actually_extracted() -> None:
    """Sanity guard against a regex that matches nothing — without
    this, the consistency test below trivially passes when a refactor
    silently breaks the extractor."""
    tokens = _all_env_tokens_in_chart()
    assert tokens, "no EVEYS_OCPP_* tokens found anywhere under deploy/helm/"
    # We expect at least the load-bearing ones the chart must set.
    expected_minimum = {
        "EVEYS_OCPP_DB_URL",
        "EVEYS_OCPP_BACKEND_BASE_URL",
        "EVEYS_OCPP_LOG_LEVEL",
    }
    missing = expected_minimum - set(tokens)
    assert not missing, f"chart no longer sets these load-bearing envs: {sorted(missing)}"


def test_every_env_token_in_chart_maps_to_a_real_settings_field() -> None:
    """Every ``EVEYS_OCPP_*`` the chart references must be a real
    field on ``Settings``. A typo or a reference to a removed field
    silently degrades the deployed gateway to the Settings default;
    this test fails the PR before staging finds out for us."""
    referenced = _all_env_tokens_in_chart()
    valid = _settings_field_names_as_envs()

    unknown_with_origins: dict[str, set[str]] = {
        token: paths for token, paths in referenced.items() if token not in valid
    }

    if unknown_with_origins:
        details = "\n".join(
            f"  {token}\n    used in: {sorted(paths)}"
            for token, paths in sorted(unknown_with_origins.items())
        )
        raise AssertionError(
            "Helm chart references EVEYS_OCPP_* env vars that don't exist as "
            "Settings fields. Either the field was renamed/removed, or the "
            "chart has a typo — in either case the deployed gateway will "
            "silently fall back to the Settings default for that knob.\n"
            f"{details}\n\n"
            "Source of truth for valid field names: src/eveys_ocpp/settings.py"
        )
