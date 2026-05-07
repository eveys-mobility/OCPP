"""SLO recording-rule sanity check.

Per E4-8 the SLOs are pure config, but a future hand-edit could
typo a metric name, drop a rule, or misalign with the dashboard's
record references. This test parses `deploy/prometheus/rules.yml`
and asserts:

  - the file is valid YAML with the expected `groups[0].rules` shape
  - every rule we promise in `docs/14-slos.md` is present
  - every recorded series referenced by `06-slos.json` resolves to
    a `record:` line in the file (catches typos in either direction)
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
RULES_PATH = REPO_ROOT / "deploy" / "prometheus" / "rules.yml"
DASHBOARD_PATH = REPO_ROOT / "deploy" / "grafana" / "dashboards" / "06-slos.json"


def test_rules_file_parses_and_has_one_group() -> None:
    doc = yaml.safe_load(RULES_PATH.read_text())
    assert isinstance(doc, dict)
    groups = doc.get("groups")
    assert isinstance(groups, list) and len(groups) == 1
    assert groups[0]["name"] == "eveys_ocpp_slos"


def _recorded_names() -> set[str]:
    doc = yaml.safe_load(RULES_PATH.read_text())
    return {r["record"] for r in doc["groups"][0]["rules"]}


def test_every_promised_slo_has_a_record_rule() -> None:
    """Each of the five SLOs from `docs/14-slos.md` has a long-window
    recording rule. The 5m short-window companions are nice-to-have,
    not load-bearing for the SLO contract — but they're checked too
    so a future hand-edit doesn't drop one without notice."""
    expected_long_window = {
        "slo:boot_acceptance:ratio_30d",
        "slo:authorize_latency:p95_7d",
        "slo:remote_start_latency:p95_7d",
        "slo:transaction_durability:ratio_30d",
        "slo:webhook_delivery:ratio_7d",
    }
    expected_short_window = {
        "slo:boot_acceptance:ratio_5m",
        "slo:authorize_latency:p95_5m",
        "slo:remote_start_latency:p95_5m",
        # SLO 4 (durability) is single-window by design — a 5m
        # durability number would be too noisy to be useful.
        "slo:webhook_delivery:ratio_5m",
    }
    recorded = _recorded_names()
    expected = expected_long_window | expected_short_window
    missing = expected - recorded
    assert not missing, f"missing recording rules: {sorted(missing)}"


def test_dashboard_only_references_existing_records() -> None:
    """Every `slo:...` series the dashboard panels read must
    resolve to a recorded rule. Catches typos in either file."""
    dashboard = json.loads(DASHBOARD_PATH.read_text())
    referenced: set[str] = set()
    pattern = re.compile(r"\bslo:[a-z0-9_:]+\b")
    for panel in dashboard.get("panels", []):
        for target in panel.get("targets", []) or []:
            expr = target.get("expr") or ""
            for match in pattern.findall(expr):
                referenced.add(match)
    recorded = _recorded_names()
    unknown = referenced - recorded
    assert not unknown, f"dashboard references unknown rules: {sorted(unknown)}"


def test_records_use_known_eveys_ocpp_metrics() -> None:
    """Each rule's expression should reference at least one
    `eveys_ocpp_*` metric. Catches a refactor that accidentally
    leaves a rule dangling on a renamed source metric."""
    doc = yaml.safe_load(RULES_PATH.read_text())
    rules = doc["groups"][0]["rules"]
    for rule in rules:
        expr = rule["expr"]
        assert "eveys_ocpp_" in expr, (
            f"rule {rule['record']!r} expression doesn't reference any "
            f"eveys_ocpp_* metric — likely broken after a rename"
        )
