"""Per-charger dashboard sanity check.

`deploy/grafana/dashboards/03-per-charger.json` reads from ClickHouse
event tables (`cp_status`, `cp_meter`, `cp_boot`, `tx_started`) for
true per-cp_id drill-down — the Prometheus metrics deliberately
carry no `cp_id` label. A future edit could typo a table name, drop
the `$cp_id` filter (cardinality bomb), or wire to the wrong
datasource.

This test parses the dashboard JSON and asserts:

  - every panel references the `clickhouse` datasource UID
  - every CH table referenced exists in the DDL under
    `src/eveys_ocpp/clickhouse/ddl/`
  - every panel's SQL filters by the `$cp_id` template variable
    (the whole point of the dashboard) — exception: the template
    query itself which populates the dropdown
"""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD_PATH = REPO_ROOT / "deploy" / "grafana" / "dashboards" / "03-per-charger.json"
DDL_DIR = REPO_ROOT / "src" / "eveys_ocpp" / "clickhouse" / "ddl"


def _dashboard() -> dict:
    return json.loads(DASHBOARD_PATH.read_text())


def _known_tables() -> set[str]:
    """Parse `CREATE TABLE` names out of the DDL files. Source of
    truth for which tables exist."""
    pattern = re.compile(r"CREATE TABLE IF NOT EXISTS\s+(\w+)", re.IGNORECASE)
    tables: set[str] = set()
    for f in DDL_DIR.glob("*.sql"):
        for match in pattern.finditer(f.read_text()):
            tables.add(match.group(1))
    return tables


def _query_panels(spec: dict) -> list[tuple[str, str]]:
    """Return (panel_title, rawSql) tuples for every panel target.
    Skips the markdown text panel."""
    out: list[tuple[str, str]] = []
    for panel in spec.get("panels", []):
        if panel.get("type") == "text":
            continue
        for target in panel.get("targets", []):
            sql = target.get("rawSql")
            if sql:
                out.append((panel["title"], sql))
    return out


def test_dashboard_is_valid_json() -> None:
    spec = _dashboard()
    assert spec["uid"] == "eveys-ocpp-per-charger"
    assert spec["templating"]["list"][0]["name"] == "cp_id"


def test_every_query_panel_uses_clickhouse_datasource() -> None:
    spec = _dashboard()
    for panel in spec["panels"]:
        if panel.get("type") == "text":
            continue
        ds = panel.get("datasource", {})
        assert ds.get("uid") == "clickhouse", (
            f"panel {panel['title']!r} references {ds!r}, expected clickhouse"
        )


def test_every_referenced_ch_table_exists_in_ddl() -> None:
    """Catches a typo like `cp_meters` → would silently produce no
    data on a real query and a green test otherwise."""
    known = _known_tables()
    # Grep table names out of the FROM / UNION ALL clauses in every
    # rawSql. Restrict to the leading `cp_*` / `tx_*` shape so we
    # don't false-positive on subqueries, ORDER BY, etc.
    table_pattern = re.compile(r"\bFROM\s+(\w+)\b", re.IGNORECASE)
    referenced: set[str] = set()
    for _, sql in _query_panels(_dashboard()):
        referenced.update(table_pattern.findall(sql))
    # Drop the `(SELECT ...)` subquery alias case — `FROM (SELECT ...`
    # captures `(` which is not a table.
    referenced = {t for t in referenced if t.isidentifier()}
    missing = referenced - known
    assert not missing, (
        f"dashboard references unknown CH tables: {sorted(missing)}; known: {sorted(known)}"
    )


def test_every_panel_filters_by_cp_id() -> None:
    """The whole purpose of the dashboard is the per-charger filter.
    A panel without it would scan the entire fleet and either time
    out or quietly drown in noise."""
    for title, sql in _query_panels(_dashboard()):
        assert "$cp_id" in sql, f"panel {title!r} SQL does not filter by $cp_id: {sql!r}"


def test_template_variable_query_targets_cp_status() -> None:
    """The cp_id picker reads from cp_status (smaller table than
    cp_meter, same partition window). A future edit pointing it at
    cp_meter would still work but be slower; check it stays put."""
    spec = _dashboard()
    var = spec["templating"]["list"][0]
    sql = var["query"]["rawSql"]
    assert "FROM cp_status" in sql
    assert "DISTINCT cp_id" in sql
