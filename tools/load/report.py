"""Render `ScenarioResult`s as a Markdown report a reviewer can drop
into a PR or wiki.

Pure function — no I/O. Caller writes the result to disk or stdout.
"""

from __future__ import annotations

from collections.abc import Iterable

from tools.load.scenario import ScenarioResult


def render_markdown(results: Iterable[ScenarioResult]) -> str:
    """One section per scenario, plus a one-line top summary.

    Output shape:

        # Load test report

        **2 of 3 scenarios passed.**

        ## boot_storm — PASS
        Started 2026-05-07T11:00:00+00:00, ran for 12.4s.

        | Criterion | Threshold | Actual | Result |
        | --- | --- | --- | --- |
        | all chargers booted | >= 10 | 10 | PASS |
        | ...

        Notes:
        - fleet: count=10 ramp=2s hold=8s
        - counters: boots=10 txns=0 errors=0
    """
    results_list = list(results)
    if not results_list:
        return "# Load test report\n\n**No scenarios ran.**\n"

    passed_count = sum(1 for r in results_list if r.passed)
    lines: list[str] = [
        "# Load test report",
        "",
        f"**{passed_count} of {len(results_list)} scenarios passed.**",
        "",
    ]
    for result in results_list:
        verdict = "PASS" if result.passed else "FAIL"
        lines.append(f"## {result.name} — {verdict}")
        lines.append(f"Started `{result.started_at}`, ran for `{result.duration_seconds:.1f}s`.")
        lines.append("")
        lines.append("| Criterion | Threshold | Actual | Result |")
        lines.append("| --- | --- | --- | --- |")
        for c in result.criteria:
            row_verdict = "PASS" if c.passed else "FAIL"
            lines.append(f"| {c.name} | `{c.threshold}` | `{c.actual}` | {row_verdict} |")
        lines.append("")
        if result.notes:
            lines.append("Notes:")
            for note in result.notes:
                lines.append(f"- {note}")
            lines.append("")
        # Show the PromQL / shell expression each criterion measured
        # so a reviewer can re-run the query on their own Prometheus.
        lines.append("<details><summary>Measurement expressions</summary>")
        lines.append("")
        for c in result.criteria:
            lines.append(f"- **{c.name}**: `{c.expression}`")
        lines.append("")
        lines.append("</details>")
        lines.append("")
    return "\n".join(lines)
