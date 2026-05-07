"""Scenario / Criterion dataclasses + the abstract scenario protocol.

Every concrete scenario in `tools/load/scenarios/` returns a
`ScenarioResult` so the report renderer is scenario-agnostic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class Criterion:
    """One pass/fail check. The roadmap expresses these as English
    sentences (e.g. "P95 RemoteStart end-to-end < 3 seconds"); here
    they're structured so the renderer can format pass/fail tables
    and a CI step can grep machine-readable JSON if needed.
    """

    name: str
    # The PromQL or shell expression used to derive `actual`. Stored
    # so the report shows reviewers exactly what was measured.
    expression: str
    # Free-form human description of the threshold (e.g. "< 3 seconds").
    threshold: str
    actual: str
    passed: bool


@dataclass(slots=True)
class ScenarioResult:
    """One scenario run's outcome."""

    name: str
    started_at: str  # ISO-8601
    duration_seconds: float
    criteria: list[Criterion] = field(default_factory=list)
    # Free-form notes the scenario authors can drop in (e.g. "saw
    # 5xx burst at t=42s — see Sentry issue X").
    notes: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """True iff every criterion passed. An empty criteria list is
        treated as failure — a scenario that doesn't check anything
        is a bug, not a pass."""
        if not self.criteria:
            return False
        return all(c.passed for c in self.criteria)

    def to_dict(self) -> dict[str, Any]:
        """Machine-readable form for `--json` output / CI parsing."""
        return {
            "name": self.name,
            "started_at": self.started_at,
            "duration_seconds": self.duration_seconds,
            "passed": self.passed,
            "criteria": [
                {
                    "name": c.name,
                    "expression": c.expression,
                    "threshold": c.threshold,
                    "actual": c.actual,
                    "passed": c.passed,
                }
                for c in self.criteria
            ],
            "notes": list(self.notes),
        }
