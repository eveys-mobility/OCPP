"""Behaviour profile presets — closed enum, sane shapes."""

from __future__ import annotations

import pytest
from tools.sim.profiles import (
    CHURNING,
    IDLE,
    PROFILES,
    REALISTIC,
    BehaviourProfile,
    get_profile,
)


def test_three_profiles_registered() -> None:
    assert set(PROFILES.keys()) == {"realistic", "idle", "churning"}


def test_get_profile_returns_named_preset() -> None:
    assert get_profile("realistic") is REALISTIC
    assert get_profile("idle") is IDLE
    assert get_profile("churning") is CHURNING


def test_get_profile_raises_on_unknown() -> None:
    with pytest.raises(KeyError):
        get_profile("nonsense")


def test_idle_profile_never_starts_transactions_or_disconnects() -> None:
    """`idle` is the smallest profile — connect, heartbeat, nothing else."""
    assert IDLE.transaction_start_per_minute == 0.0
    assert IDLE.disconnect_per_minute == 0.0


def test_churning_profile_drops_about_once_per_minute() -> None:
    assert CHURNING.disconnect_per_minute == pytest.approx(1.0)
    # Churning doesn't drive transactions — its job is connection churn.
    assert CHURNING.transaction_start_per_minute == 0.0


def test_realistic_profile_session_length_and_meter_cadence() -> None:
    """Anchor the realistic profile's headline numbers so an
    accidental edit blows up the test instead of silently changing
    every load run downstream."""
    assert REALISTIC.session_length_seconds_mean == pytest.approx(30 * 60.0)
    assert REALISTIC.meter_values_period_seconds == 30.0
    assert REALISTIC.heartbeat_period_seconds == 60.0


def test_profile_is_frozen() -> None:
    """`@dataclass(frozen=True)` so a profile can't be mutated by one
    charger and seen by another."""
    with pytest.raises((AttributeError, TypeError)):  # frozen → FrozenInstanceError subclass
        REALISTIC.transaction_start_per_minute = 1.0  # type: ignore[misc]


def test_custom_profile_construction() -> None:
    """Power users should be able to build a profile inline."""
    p = BehaviourProfile(
        name="custom",
        transaction_start_per_minute=5.0,
        session_length_seconds_mean=120.0,
        meter_values_period_seconds=10.0,
        heartbeat_period_seconds=30.0,
        disconnect_per_minute=0.5,
    )
    assert p.name == "custom"
    assert p.transaction_start_per_minute == 5.0
