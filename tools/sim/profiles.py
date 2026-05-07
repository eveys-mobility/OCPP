"""Behaviour profiles — what a virtual charger does once connected.

A `BehaviourProfile` is a frozen bag of probabilities and cadences
the `VirtualCharger` consults to decide what to do next. Three
presets cover the common shapes:

- `realistic`  — modelled on the dev fleet: 90% idle, 10% in-session
- `idle`       — connect, heartbeat, never start a transaction
- `churning`   — disconnect / reconnect on a ~60s mean

Custom profiles are constructed directly via the dataclass — the
presets are convenience.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True, slots=True)
class BehaviourProfile:
    """Per-charger behaviour knobs.

    All cadences are seconds; all probabilities are floats in `[0, 1]`
    interpreted per minute (e.g. `disconnect_per_minute=0.0167` =
    once every 60 minutes on average).
    """

    name: str
    # Probability per minute that an idle charger starts a transaction.
    # 0 → never starts; the charger only heartbeats.
    transaction_start_per_minute: float
    # Mean session length in seconds (Stop fires after this).
    session_length_seconds_mean: float
    # Cadence of MeterValues during an active session.
    meter_values_period_seconds: float
    # Cadence of Heartbeat between activities.
    heartbeat_period_seconds: float
    # Probability per minute the charger drops its WS and reconnects.
    # 0 → never drops; the charger holds its WS forever.
    disconnect_per_minute: float


REALISTIC: Final = BehaviourProfile(
    name="realistic",
    # 1 transaction / charger / hour ≈ 0.0167 / minute
    transaction_start_per_minute=0.0167,
    # 30-minute mean session — covers a typical lunch-break top-up
    session_length_seconds_mean=30 * 60.0,
    meter_values_period_seconds=30.0,
    heartbeat_period_seconds=60.0,
    disconnect_per_minute=0.0,
)


IDLE: Final = BehaviourProfile(
    name="idle",
    transaction_start_per_minute=0.0,
    session_length_seconds_mean=0.0,
    meter_values_period_seconds=0.0,
    heartbeat_period_seconds=60.0,
    disconnect_per_minute=0.0,
)


CHURNING: Final = BehaviourProfile(
    name="churning",
    transaction_start_per_minute=0.0,
    session_length_seconds_mean=0.0,
    meter_values_period_seconds=0.0,
    heartbeat_period_seconds=60.0,
    # 60-second mean reconnect → 1/60 = 0.0167 per second = 1.0 per minute
    disconnect_per_minute=1.0,
)


PROFILES: Final[dict[str, BehaviourProfile]] = {
    "realistic": REALISTIC,
    "idle": IDLE,
    "churning": CHURNING,
}


def get_profile(name: str) -> BehaviourProfile:
    """Look up a preset by name. Raises `KeyError` on unknown — the CLI
    surfaces this as an `argparse` choice violation before we get
    here, but defensive lookup is still cheap."""
    return PROFILES[name]
