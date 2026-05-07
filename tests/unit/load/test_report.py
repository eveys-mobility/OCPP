"""Report renderer — Markdown shape on canned scenario results."""

from __future__ import annotations

from tools.load.report import render_markdown
from tools.load.scenario import Criterion, ScenarioResult


def _scenario(name: str, *, all_pass: bool) -> ScenarioResult:
    return ScenarioResult(
        name=name,
        started_at="2026-05-07T11:00:00+00:00",
        duration_seconds=12.4,
        criteria=[
            Criterion(
                name="all chargers booted",
                expression="counters.boots >= 10",
                threshold=">= 10",
                actual="10",
                passed=True,
            ),
            Criterion(
                name="P99 < 3s",
                expression="histogram_quantile(0.99, ...)",
                threshold="< 3s",
                actual="2.4s" if all_pass else "5.1s",
                passed=all_pass,
            ),
        ],
        notes=["fleet count=10 ramp=2s hold=8s"],
    )


def test_renders_no_scenarios_message_for_empty_input() -> None:
    out = render_markdown([])
    assert "No scenarios ran." in out


def test_top_summary_counts_passes() -> None:
    out = render_markdown(
        [
            _scenario("a", all_pass=True),
            _scenario("b", all_pass=False),
            _scenario("c", all_pass=True),
        ]
    )
    assert "**2 of 3 scenarios passed.**" in out


def test_each_scenario_gets_a_pass_or_fail_heading() -> None:
    out = render_markdown(
        [
            _scenario("a", all_pass=True),
            _scenario("b", all_pass=False),
        ]
    )
    assert "## a — PASS" in out
    assert "## b — FAIL" in out


def test_criterion_table_shows_actual_and_threshold() -> None:
    out = render_markdown([_scenario("a", all_pass=False)])
    # Failing criterion's actual value shows up in the table.
    assert "5.1s" in out
    # Threshold rendered as inline code.
    assert "`< 3s`" in out


def test_notes_section_emitted_when_scenario_has_notes() -> None:
    out = render_markdown([_scenario("a", all_pass=True)])
    assert "Notes:" in out
    assert "fleet count=10 ramp=2s hold=8s" in out


def test_measurement_expressions_in_collapsible_block() -> None:
    """The PromQL / shell expression each criterion measured is in a
    `<details>` block so reviewers can see what was queried without
    bloating the table."""
    out = render_markdown([_scenario("a", all_pass=True)])
    assert "<details>" in out
    assert "histogram_quantile(0.99, ...)" in out
