"""Rollback runbook ↔ reality validity (Phase 6 pre-staging gate).

`docs/18-rollback-runbook.md` is the load-bearing operator doc for the
Phase-6 deliverable "disconnect a charger from `ocpp-gw` in <2 min."
Today's only gate is review-time eyeballs — a schema rename, a
renamed endpoint, a removed metric label, or a renamed Settings key
all silently break the runbook for an operator under incident
pressure.

Same shape as ``test_grafana_dashboards.py`` (#154) and
``test_deploy_envs.py`` (#160/#162): walk the artefact, extract
identifiers, verify each against the source of truth in this repo.
Failure messages name the specific block + identifier so a PR
reviewer can fix.

What this test covers, and where it intentionally stops:

- **SQL blocks**: tables referenced after ``FROM``/``UPDATE``/
  ``INSERT INTO``/``JOIN`` must match SQLAlchemy ``__tablename__``
  declarations.
- **PromQL blocks**: ``eveys_ocpp_*`` metric names must be exported
  by ``metrics/registry.py`` (with prom-client suffix expansion);
  string-typed label values referenced in ``{outcome="..."}``-style
  matchers must appear in the metric's documented closed enum.
- **`curl` API paths**: the ``/api/v1/...`` route in any ``curl``
  command must resolve in ``docs/api/openapi.json`` (after
  normalising placeholders to template params).
- **``EVEYS_OCPP_*`` tokens**: must map to real Settings fields.
  (Same check as ``test_deploy_envs``, applied to the runbook.)
- **File-path links**: ``[`_basic_auth.py`](../src/...)``-style
  links must resolve relative to the doc.

Out of scope (deliberately, after considering it):

- **Dry-running the blocks.** The runbook has placeholders
  (``<CP_TARGET>``, ``<new-bcrypt-hash>``, ``<gateway>``) — running
  them as-is fails unhelpfully. The right test is identifier
  existence, not execution.
- The ``kubectl`` / firewall / Envoy blocks (Steps 3-4): cluster-
  side, not gateway-side.
- Bash logic / pipeline correctness inside the blocks.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import prometheus_client

from eveys_ocpp.metrics import registry
from eveys_ocpp.persistence.models import Base
from eveys_ocpp.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNBOOK_PATH = REPO_ROOT / "docs" / "18-rollback-runbook.md"
OPENAPI_PATH = REPO_ROOT / "docs" / "api" / "openapi.json"

# Match a fenced code block with optional language tag. Capture both.
_FENCE = re.compile(
    r"```(?P<lang>[a-zA-Z0-9_-]*)\n(?P<body>.*?)```",
    re.DOTALL,
)

# `FROM table`, `UPDATE table`, `INSERT INTO table`, `JOIN table`.
_SQL_TABLE_REF = re.compile(
    r"\b(?:FROM|UPDATE|JOIN|INSERT\s+INTO)\s+([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE,
)

# `eveys_ocpp_*` metric names anywhere in a PromQL block. The
# project-prefix filter is enough to avoid PromQL function names.
_METRIC_NAME = re.compile(r"\beveys_ocpp_[a-zA-Z0-9_]+\b")

# `{outcome="bad_password"}` — extract label-value pairs so we can
# cross-check against documented closed enums in metric descriptions.
_LABEL_MATCHER = re.compile(r'(\w+)\s*=\s*"([^"]+)"')

# `EVEYS_OCPP_*` env vars — same anchor rule as `test_deploy_envs`
# (must end in a letter so glob-stems don't false-match).
_ENV_TOKEN = re.compile(r"\bEVEYS_OCPP_[A-Z][A-Z0-9_]*[A-Z0-9]\b")

# `curl … https://<gateway>/api/v1/...` — capture the path. Anchored
# on `/api/v1/` so we ignore unrelated curls.
_API_PATH = re.compile(r"/api/v1/[A-Za-z0-9_/{}<>\-]*")

# Markdown link to a relative path: `(../src/foo.py)` or `(./bar.md)`.
_REL_LINK = re.compile(r"\]\((?P<path>(?:\.\.?|/)[^)\s#]+)")


def _runbook_text() -> str:
    return RUNBOOK_PATH.read_text(encoding="utf-8")


def _code_blocks() -> list[tuple[str, str, int]]:
    """Yield ``(lang, body, line_number)`` for every fenced code block."""
    text = _runbook_text()
    out: list[tuple[str, str, int]] = []
    for m in _FENCE.finditer(text):
        line = text[: m.start()].count("\n") + 1
        out.append((m.group("lang") or "", m.group("body"), line))
    return out


# ---- SQL: tables referenced must exist in the SQLAlchemy models ------------


def _model_tablenames() -> set[str]:
    """All ``__tablename__`` declared by SQLAlchemy models."""
    out: set[str] = set()
    pending: list[type] = list(Base.__subclasses__())
    while pending:
        cls = pending.pop()
        if hasattr(cls, "__tablename__"):
            out.add(cls.__tablename__)
        pending.extend(cls.__subclasses__())
    return out


def test_runbook_sql_blocks_reference_real_tables() -> None:
    valid_tables = _model_tablenames()
    assert valid_tables, "no SQLAlchemy tables found — bootstrap broken"

    unknown: dict[int, set[str]] = {}
    for lang, body, line in _code_blocks():
        if lang.lower() != "sql":
            continue
        for m in _SQL_TABLE_REF.finditer(body):
            table = m.group(1)
            if table not in valid_tables:
                unknown.setdefault(line, set()).add(table)

    if unknown:
        details = "\n".join(
            f"  block at line {ln}: {sorted(tables)}" for ln, tables in sorted(unknown.items())
        )
        raise AssertionError(
            "Rollback runbook references SQL tables that don't exist as "
            "SQLAlchemy `__tablename__` declarations. Either the table "
            "was renamed/removed, or the runbook has a typo — in either "
            "case an operator following the runbook under incident "
            "pressure will see a SQL error.\n"
            f"{details}\n\n"
            "Source of truth for valid table names: src/eveys_ocpp/persistence/models.py"
        )


# ---- PromQL: metric names + closed-enum label values ----------------------


def _exported_metric_names() -> set[str]:
    """Mirror of ``test_grafana_dashboards._exported_metric_names``."""
    names: set[str] = set()
    for attr in dir(registry):
        obj = getattr(registry, attr)
        if not isinstance(
            obj,
            (
                prometheus_client.Counter,
                prometheus_client.Gauge,
                prometheus_client.Histogram,
                prometheus_client.Summary,
                prometheus_client.Info,
                prometheus_client.Enum,
            ),
        ):
            continue
        base = obj._name
        if isinstance(obj, prometheus_client.Counter):
            names.update({base, f"{base}_total"})
        elif isinstance(obj, prometheus_client.Histogram):
            names.update({base, f"{base}_bucket", f"{base}_count", f"{base}_sum"})
        elif isinstance(obj, prometheus_client.Summary):
            names.update({base, f"{base}_count", f"{base}_sum"})
        else:
            names.add(base)
    return names


def _metric_object_for(metric_name: str) -> object | None:
    """Return the prom-client metric instance whose ``_name`` matches
    ``metric_name`` (with the prom-client suffix stripped)."""
    base = metric_name
    for suffix in ("_total", "_bucket", "_count", "_sum"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    for attr in dir(registry):
        obj = getattr(registry, attr)
        if hasattr(obj, "_name") and obj._name == base:
            return obj
    return None


def test_runbook_promql_blocks_reference_real_metrics() -> None:
    valid_names = _exported_metric_names()
    unknown_metrics: dict[int, set[str]] = {}
    for lang, body, line in _code_blocks():
        if lang.lower() != "promql":
            continue
        for m in _METRIC_NAME.finditer(body):
            if m.group(0) not in valid_names:
                unknown_metrics.setdefault(line, set()).add(m.group(0))

    if unknown_metrics:
        details = "\n".join(
            f"  block at line {ln}: {sorted(names)}"
            for ln, names in sorted(unknown_metrics.items())
        )
        raise AssertionError(
            "Rollback runbook references metrics that aren't exported by "
            "the gateway. Either the metric was renamed/removed, or the "
            "runbook has a typo — operators under incident pressure will "
            "see an empty graph in Grafana.\n"
            f"{details}\n\n"
            "Source of truth: src/eveys_ocpp/metrics/registry.py"
        )


def test_runbook_promql_label_values_appear_in_metric_description() -> None:
    """``{outcome="bad_password"}`` is only useful if ``bad_password``
    is one of the values that metric actually emits.

    Each metric's description spells out its closed enum (per project
    convention — see e.g. ``WS_BASIC_AUTH_TOTAL``: *"`outcome` is a
    closed enum: ok, no_header, malformed, ..."*). The cheapest check
    that catches a renamed enum value is searching the description
    text for the value the runbook references."""
    bad: list[tuple[int, str, str, str]] = []  # (line, metric, label, value)
    for lang, body, line in _code_blocks():
        if lang.lower() != "promql":
            continue
        for metric_match in _METRIC_NAME.finditer(body):
            metric_name = metric_match.group(0)
            metric = _metric_object_for(metric_name)
            if metric is None:
                continue  # caught by the previous test
            doc = (getattr(metric, "_documentation", "") or "").lower()
            # Find the matcher that follows this metric reference, if any.
            tail = body[metric_match.end() :]
            brace_open = tail.find("{")
            brace_close = tail.find("}")
            if brace_open == -1 or brace_close == -1 or brace_close < brace_open:
                continue
            matcher = tail[brace_open + 1 : brace_close]
            for lab_m in _LABEL_MATCHER.finditer(matcher):
                label, value = lab_m.group(1), lab_m.group(2)
                if value.lower() not in doc:
                    bad.append((line, metric_name, label, value))

    if bad:
        details = "\n".join(
            f'  block at line {ln}: {metric}{{{label}="{value}"}} — '
            f"value not mentioned in the metric's description"
            for ln, metric, label, value in bad
        )
        raise AssertionError(
            "Rollback runbook references PromQL label values that don't "
            "appear in the metric's documented closed enum. Either the "
            "value was renamed/removed in the registry, or the runbook "
            "has a typo — operators will see empty results.\n"
            f"{details}"
        )


# ---- Settings tokens: must map to real Settings fields --------------------


def _settings_envs() -> set[str]:
    prefix = Settings.model_config.get("env_prefix", "")
    return {f"{prefix}{name.upper()}" for name in Settings.model_fields}


def test_runbook_env_tokens_map_to_real_settings_fields() -> None:
    valid = _settings_envs()
    text = _runbook_text()
    unknown: set[str] = set()
    for m in _ENV_TOKEN.finditer(text):
        if m.group(0) not in valid:
            unknown.add(m.group(0))
    assert not unknown, (
        "Rollback runbook references EVEYS_OCPP_* env vars that don't "
        "exist as Settings fields. Either the field was renamed/removed, "
        "or the runbook has a typo — an operator setting one of these "
        "during a rollback will see no effect.\n"
        f"  Unknown: {sorted(unknown)}\n"
        "Source of truth: src/eveys_ocpp/settings.py"
    )


# ---- API paths in curl commands: must resolve in OpenAPI ------------------


def _openapi_routes() -> set[str]:
    """Routes from the committed OpenAPI snapshot, normalised so a
    placeholder like ``{cp_id}`` matches a runbook ``<CP_TARGET>``
    after both are reduced to ``*``."""
    spec = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
    out: set[str] = set()
    for path in spec["paths"]:
        out.add(re.sub(r"\{[^}]+\}", "*", path))
    return out


def test_runbook_curl_api_paths_resolve_in_openapi() -> None:
    valid = _openapi_routes()
    unknown: dict[int, set[str]] = {}
    for lang, body, line in _code_blocks():
        if lang.lower() not in {"bash", "sh", "shell", ""}:
            continue
        if "curl" not in body:
            continue
        for m in _API_PATH.finditer(body):
            raw = m.group(0)
            # Normalise both placeholder shapes to `*`. Strip a
            # trailing colon/quote/backslash artefact.
            normalised = re.sub(r"<[^>]+>", "*", raw)
            normalised = re.sub(r"\{[^}]+\}", "*", normalised)
            normalised = normalised.rstrip("\\\"' ")
            if normalised not in valid:
                unknown.setdefault(line, set()).add(raw)

    if unknown:
        details = "\n".join(
            f"  block at line {ln}: {sorted(paths)}" for ln, paths in sorted(unknown.items())
        )
        raise AssertionError(
            "Rollback runbook references /api/v1/... paths that don't "
            "resolve in the OpenAPI snapshot. Either the route was "
            "renamed/removed, or the runbook has a typo — an operator "
            "running the curl will see a 404.\n"
            f"{details}\n\n"
            "Source of truth: docs/api/openapi.json"
        )


# ---- Markdown relative-path links must resolve ----------------------------


def test_runbook_relative_links_resolve() -> None:
    text = _runbook_text()
    bad: set[str] = set()
    for m in _REL_LINK.finditer(text):
        rel = m.group("path")
        target = (RUNBOOK_PATH.parent / rel).resolve()
        if not target.exists():
            bad.add(rel)
    assert not bad, (
        "Rollback runbook has relative-path links that don't resolve. "
        "Either the file was moved/renamed, or the link has a typo.\n"
        f"  Unresolved: {sorted(bad)}"
    )
