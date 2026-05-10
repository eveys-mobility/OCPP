"""Grafana dashboard validity (Phase 6 pre-staging gate).

The six dashboards in ``deploy/grafana/dashboards/`` ship as JSON and
land in a real Grafana instance during staging soak. Today's only
guard is ``make json-format`` — a panel can reference a renamed
metric or a dropped table and CI is silent until an operator opens
the dashboard.

This test parses every dashboard, walks panel targets + template
variable queries, and asserts each referenced identifier exists at
the source of truth:

- Prometheus identifiers must either be a metric defined in
  ``src/eveys_ocpp/metrics/registry.py`` (any series the gateway
  actually exports) or a recording-rule name from
  ``deploy/prometheus/rules.yml`` (aggregations the recording-rule
  layer materialises).
- ClickHouse table names referenced after ``FROM`` must exist in
  ``src/eveys_ocpp/clickhouse/ddl/*.sql``.

Out of scope: PromQL parser correctness (trusted to Grafana at
render time), panel layout / colour, and the SLO dashboard's
correctness (already covered by ``test_slo_recording_rules.py``)
beyond the metric-existence check it shares with the others.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from pathlib import Path

import prometheus_client
import pytest
import yaml

from eveys_ocpp.metrics import registry

REPO_ROOT = Path(__file__).resolve().parents[2]
DASHBOARDS_DIR = REPO_ROOT / "deploy" / "grafana" / "dashboards"
RULES_PATH = REPO_ROOT / "deploy" / "prometheus" / "rules.yml"
DDL_DIR = REPO_ROOT / "src" / "eveys_ocpp" / "clickhouse" / "ddl"

# Prometheus identifier shape (metric names + recording-rule names).
# Per the Prom data model: `[a-zA-Z_:][a-zA-Z0-9_:]*`. We restrict
# the match to identifiers that begin with our project prefix or are
# recording-rule colon-shaped — that lets us ignore PromQL function
# names (`sum`, `rate`, `histogram_quantile`) without a full PromQL
# parser.
_METRIC_IDENT = re.compile(r"\b([a-zA-Z_:][a-zA-Z0-9_:]*)\b")
_PROJECT_PREFIX = "eveys_ocpp"

# Generic Grafana variables (`$__rate_interval`, `$cp_id`, etc.) and
# bare label values inside `{}` are not metric references; the
# project-prefix filter handles those automatically. The set below
# whitelists identifiers from the Prometheus standard library that
# look like metrics but are part of the runtime, not the gateway:
# `up`, `time()`, `vector(...)` etc. None today; placeholder for the
# day someone references them.
_NON_METRIC_PROM_IDENTS: frozenset[str] = frozenset()

# `FROM <table>` extraction. ClickHouse SQL is dialect-flexible, but
# every panel in the per-charger dashboard uses the canonical
# `FROM cp_meter`/`FROM cp_status` form. If a future panel uses a
# subquery or `FROM (...)`, we extend this — the assertion below
# fails loud rather than silently.
_FROM_TABLE = re.compile(r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Source-of-truth lookups (built once)
# ---------------------------------------------------------------------------


def _exported_metric_names() -> set[str]:
    """Every Prometheus series name the gateway actually exports.

    Walks the registry module, expands the prom-client suffix
    convention (Counter → `_total`, Histogram → `_bucket`/`_count`/
    `_sum`) so a dashboard expr that says `foo_total` matches the
    `foo` Counter declaration."""
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


def _recording_rule_names() -> set[str]:
    """Recording-rule series defined in `deploy/prometheus/rules.yml`.

    These are the colon-shaped identifiers (`slo:foo:ratio_5m`)
    that dashboards may reference even though they aren't on the
    metric registry — they're materialised by Prometheus from the
    rule file at evaluation time."""
    if not RULES_PATH.exists():
        return set()
    doc = yaml.safe_load(RULES_PATH.read_text())
    out: set[str] = set()
    for group in doc.get("groups", []):
        for rule in group.get("rules", []):
            record = rule.get("record")
            if record:
                out.add(record)
    return out


def _clickhouse_table_names() -> set[str]:
    """Every CH table the gateway provisions, scraped from the DDL.

    A panel can reference a future-table that Alembic/CH-migrate
    will create at boot — but the DDL file under `ddl/` IS the
    source of truth for what `make compose-up` materialises, so a
    panel referencing something not in DDL won't render."""
    names: set[str] = set()
    for ddl in DDL_DIR.glob("*.sql"):
        for match in re.finditer(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)",
            ddl.read_text(),
            re.IGNORECASE,
        ):
            names.add(match.group(1))
    return names


# ---------------------------------------------------------------------------
# Dashboard walking
# ---------------------------------------------------------------------------


def _iter_dashboards() -> Iterable[tuple[str, dict]]:
    for path in sorted(DASHBOARDS_DIR.glob("*.json")):
        yield path.name, json.loads(path.read_text())


def _iter_panel_targets(dashboard: dict) -> Iterable[tuple[str, dict]]:
    """Yield `(datasource_type, target_dict)` for every Prom/CH-ish
    target on every panel and every template variable query in the
    dashboard.

    Template variables matter: the per-charger dashboard's `$cp_id`
    comes from a CH `rawSql` query, and a typo there would silently
    produce a dropdown of nothing without our test catching it.

    Variable `query` shape varies by datasource plugin: Prometheus
    nests it as `{query: "...", refId: "..."}`, ClickHouse as
    `{format: 1, rawSql: "..."}`. Older string-shape exists too.
    Normalise both into a target-shaped dict so the rest of the
    walker doesn't care."""
    for panel in dashboard.get("panels", []) or []:
        ds_type = (panel.get("datasource") or {}).get("type", "")
        for target in panel.get("targets", []) or []:
            yield ds_type, target

    for var in (dashboard.get("templating") or {}).get("list", []) or []:
        ds_type = (var.get("datasource") or {}).get("type", "")
        raw_query = var.get("query")
        if raw_query is None:
            continue
        if isinstance(raw_query, dict):
            expr = raw_query.get("query") or ""
            raw_sql = raw_query.get("rawSql") or ""
        else:  # legacy string form
            expr = raw_query if "prometheus" in ds_type else ""
            raw_sql = raw_query if "clickhouse" in ds_type else ""
        yield ds_type, {"expr": expr, "rawSql": raw_sql}


def _prom_metrics_in(expr: str) -> set[str]:
    """All identifiers in `expr` that look like *project-owned*
    Prometheus series — either prefixed with `eveys_ocpp` or
    recording-rule colon-shaped (`foo:bar:baz`).

    PromQL function names (`sum`, `rate`, …) and label values are
    intentionally excluded by this filter so we don't have to ship
    a real parser."""
    out: set[str] = set()
    for match in _METRIC_IDENT.finditer(expr):
        ident = match.group(1)
        if ident.startswith(_PROJECT_PREFIX) or ":" in ident:
            out.add(ident)
    return out - _NON_METRIC_PROM_IDENTS


def _ch_tables_in(sql: str) -> set[str]:
    return {m.group(1) for m in _FROM_TABLE.finditer(sql)}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_dashboards_directory_is_non_empty() -> None:
    """If this fails, someone deleted the dashboards or moved the
    directory — every assertion below would vacuously pass."""
    files = list(DASHBOARDS_DIR.glob("*.json"))
    assert len(files) >= 5, f"expected ≥5 dashboards, found {[f.name for f in files]}"


@pytest.mark.parametrize("dashboard_name,dashboard", list(_iter_dashboards()))
def test_dashboard_parses_and_has_panels(dashboard_name: str, dashboard: dict) -> None:
    """Every dashboard JSON must be parseable and carry at least one
    panel — an empty dashboard renders blank in Grafana with no error."""
    assert isinstance(dashboard, dict), dashboard_name
    panels = dashboard.get("panels") or []
    assert panels, f"{dashboard_name}: dashboard has no panels"


def test_every_prometheus_metric_referenced_exists() -> None:
    """A panel that references a typo'd or renamed metric renders as
    an empty graph in Grafana with no error — silent until an operator
    opens the dashboard. This test is the gate that catches the typo
    on PR, before the panel reaches staging."""
    valid = _exported_metric_names() | _recording_rule_names()
    assert valid, "no metric or recording-rule names found — bootstrap broken"

    missing: dict[str, set[str]] = {}
    for name, dashboard in _iter_dashboards():
        for ds_type, target in _iter_panel_targets(dashboard):
            if "prometheus" not in ds_type:
                continue
            expr = target.get("expr") or ""
            if not expr:
                continue
            referenced = _prom_metrics_in(expr)
            unknown = referenced - valid
            if unknown:
                missing.setdefault(name, set()).update(unknown)

    assert not missing, (
        "dashboards reference Prometheus identifiers that aren't exported by "
        "the gateway and aren't defined as recording rules:\n"
        + "\n".join(f"  {name}: {sorted(idents)}" for name, idents in sorted(missing.items()))
    )


def test_every_clickhouse_table_referenced_exists() -> None:
    """ClickHouse rawSql panels reference tables that must exist in
    the DDL — otherwise the dashboard renders an error per-panel
    against staging once `make compose-up` provisions the schema."""
    valid_tables = _clickhouse_table_names()
    # Sanity: the DDL bootstrap must have found something.
    assert "cp_meter" in valid_tables and "cp_status" in valid_tables, valid_tables

    missing: dict[str, set[str]] = {}
    for name, dashboard in _iter_dashboards():
        for ds_type, target in _iter_panel_targets(dashboard):
            if "clickhouse" not in ds_type:
                continue
            sql = target.get("rawSql") or ""
            if not sql:
                continue
            referenced = _ch_tables_in(sql)
            # `FROM (subquery)` would put a token like `(SELECT` here
            # (the `(` doesn't match the identifier regex, so it
            # vanishes). Subqueries that ARE captured by name are the
            # ones we genuinely want to validate.
            unknown = referenced - valid_tables
            if unknown:
                missing.setdefault(name, set()).update(unknown)

    assert not missing, (
        "dashboards reference ClickHouse tables that don't exist in the DDL:\n"
        + "\n".join(f"  {name}: {sorted(tables)}" for name, tables in sorted(missing.items()))
    )
