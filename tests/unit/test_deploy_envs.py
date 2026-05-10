"""Deploy artefact ↔ Settings consistency (Phase 6 pre-staging gate).

Both deploy targets — the Helm chart at ``deploy/helm/eveys-ocpp/``
and the compose stack at ``deploy/compose/`` — reference
``EVEYS_OCPP_*`` env vars across templates, values, manifests, and
inline comments. A typo (``EVEYS_OCPP_WS_MTLS_CERT_PAHT``) or a
reference to a removed Settings field renders fine through
``helm lint`` / ``helm template`` and ``docker compose config`` —
the artefact still emits valid YAML, the pod / container still
boots, and the gateway silently falls back to the Settings default.

Operator opens Grafana, panel graphs flat-line, root cause is a
typo that survived review and CI both. Compose-smoke catches the
class eventually (``tests/compose_smoke/`` exists for exactly this),
but only after spinning the full stack — too slow as a PR signal.

This test parameterizes the same scan over both deploy targets:
extract every ``EVEYS_OCPP_*`` token, strip prefix + lowercase, and
assert each is a real field on ``Settings``.

Out of scope:

- Opposite-direction check (every Settings field must be referenced).
  Most fields have sensible defaults; not a contract violation.
- Type / value validation (envs are strings; Settings validates at
  gateway boot).
- ``tools/`` and ``tests/load/`` env wiring (different audience,
  less load-bearing). Add to ``DEPLOY_ROOTS`` if drift bites.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from eveys_ocpp.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[2]

# Each deploy target's root + a human label used in failure messages
# and in the parametrize id. Adding a new target = one new tuple.
DEPLOY_ROOTS: list[tuple[str, Path]] = [
    ("helm", REPO_ROOT / "deploy" / "helm"),
    ("compose", REPO_ROOT / "deploy" / "compose"),
]

# `EVEYS_OCPP_<UPPER_SNAKE>` — anchored on a final letter so a glob
# stem in a comment (``EVEYS_OCPP_WS_MTLS_*`` becomes the trailing
# ``_`` once the splat is stripped) doesn't match. That's
# intentional: globs document a family, they aren't a single field
# reference.
_ENV_TOKEN = re.compile(r"\bEVEYS_OCPP_[A-Z][A-Z0-9_]*[A-Z0-9]\b")


def _all_env_tokens_in(root: Path) -> dict[str, set[str]]:
    """Return ``{token: {file_path, ...}}`` for every ``EVEYS_OCPP_*``
    string anywhere under ``root``. Tracks origin paths so a failure
    can name the offending file."""
    out: dict[str, set[str]] = {}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        # Skip binary blobs (charts can ship icons or `.tgz` deps);
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


@pytest.mark.parametrize(("label", "root"), DEPLOY_ROOTS, ids=[label for label, _ in DEPLOY_ROOTS])
def test_deploy_root_exists_and_has_files(label: str, root: Path) -> None:
    """If this fails, someone moved or deleted a deploy target and
    every assertion below would vacuously pass."""
    assert root.is_dir(), f"{label}: {root} not a directory"
    files = [p for p in root.rglob("*") if p.is_file()]
    assert len(files) >= 1, f"{label}: no files under {root}"


@pytest.mark.parametrize(("label", "root"), DEPLOY_ROOTS, ids=[label for label, _ in DEPLOY_ROOTS])
def test_some_env_tokens_were_actually_extracted(label: str, root: Path) -> None:
    """Sanity guard against a regex that matches nothing — without
    this, the consistency test below trivially passes when a refactor
    silently breaks the extractor.

    Both deploy targets must reference at least the load-bearing
    envs the gateway can't run without."""
    tokens = _all_env_tokens_in(root)
    assert tokens, f"{label}: no EVEYS_OCPP_* tokens found anywhere under {root}"
    expected_minimum = {
        "EVEYS_OCPP_DB_URL",
        "EVEYS_OCPP_BACKEND_BASE_URL",
    }
    missing = expected_minimum - set(tokens)
    assert not missing, f"{label} no longer references these load-bearing envs: {sorted(missing)}"


@pytest.mark.parametrize(("label", "root"), DEPLOY_ROOTS, ids=[label for label, _ in DEPLOY_ROOTS])
def test_every_env_token_maps_to_a_real_settings_field(label: str, root: Path) -> None:
    """Every ``EVEYS_OCPP_*`` referenced by the deploy artefact must
    be a real field on ``Settings``. A typo or a reference to a
    removed field silently degrades the deployed gateway to the
    Settings default; this test fails the PR before staging finds out."""
    referenced = _all_env_tokens_in(root)
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
            f"{label}: references EVEYS_OCPP_* env vars that don't exist as "
            "Settings fields. Either the field was renamed/removed, or the "
            f"{label} artefact has a typo — in either case the deployed gateway "
            "will silently fall back to the Settings default for that knob.\n"
            f"{details}\n\n"
            "Source of truth for valid field names: src/eveys_ocpp/settings.py"
        )
