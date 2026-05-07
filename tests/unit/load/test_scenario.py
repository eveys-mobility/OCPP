"""ScenarioResult / Criterion — pass logic and serialisation."""

from __future__ import annotations

from tools.load.scenario import Criterion, ScenarioResult


def _criterion(passed: bool, name: str = "test") -> Criterion:
    return Criterion(
        name=name,
        expression="dummy",
        threshold="< X",
        actual="Y",
        passed=passed,
    )


def test_passed_is_true_iff_all_criteria_pass() -> None:
    r = ScenarioResult(
        name="s",
        started_at="2026-05-07T11:00:00+00:00",
        duration_seconds=1.0,
        criteria=[_criterion(True, "a"), _criterion(True, "b")],
    )
    assert r.passed is True


def test_passed_is_false_when_any_criterion_fails() -> None:
    r = ScenarioResult(
        name="s",
        started_at="2026-05-07T11:00:00+00:00",
        duration_seconds=1.0,
        criteria=[_criterion(True), _criterion(False)],
    )
    assert r.passed is False


def test_empty_criteria_is_treated_as_failure() -> None:
    """A scenario that doesn't check anything is a bug, not a green run.
    The renderer relies on this to flag misconfigured scenarios."""
    r = ScenarioResult(
        name="s",
        started_at="2026-05-07T11:00:00+00:00",
        duration_seconds=1.0,
    )
    assert r.passed is False


def test_to_dict_round_trip_carries_every_field() -> None:
    r = ScenarioResult(
        name="boot_storm",
        started_at="2026-05-07T11:00:00+00:00",
        duration_seconds=12.4,
        criteria=[_criterion(True, "all booted")],
        notes=["fleet count=10"],
    )
    d = r.to_dict()
    assert d["name"] == "boot_storm"
    assert d["passed"] is True
    assert d["duration_seconds"] == 12.4
    assert d["criteria"][0]["name"] == "all booted"
    assert d["notes"] == ["fleet count=10"]
