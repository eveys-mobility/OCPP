"""Measurand-aware sanity-range validation for `MeterValues` samples (E5-4).

A charger reporting nonsense — an energy register that jumps 100 MWh
between two samples, a 50 kV bus voltage, a -200°C battery
temperature — is almost always one of three things:

1. A unit-confusion bug on the charger's firmware (kWh vs Wh).
2. A sensor wedge / overflow (pinned `0xFFFF`, `INT_MAX`, etc.).
3. An attacker probing the system.

We do not want any of those reaching ClickHouse, Prometheus
dashboards, or — most importantly — billing. The sample is dropped,
the rest of the envelope continues, and a Prometheus counter
records what got dropped and why.

**Drop policy** is per-sample, not per-envelope: if a single
`MeterValues` call carries one bad voltage reading and ten good
energy readings, we keep the ten good ones. The OCPP response is
unaffected — chargers do not get a 4xx-equivalent for a bad
sample, because OCPP has no such response code, and silently
dropping is the only behaviour that doesn't break the protocol.

**Unknown measurands accept by default.** Vendor extensions and
future OCPP versions add measurands; rejecting an unknown name
would be worse than letting one through unvalidated.

The numbers below are **physics-grounded, not operator-tunable**:
they're meant to catch obvious garbage, not to enforce
deployment-specific limits. A site that needs a 50 A current cap
on a connector enforces that elsewhere; this module rejects only
values that no charger could plausibly produce regardless of site.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class _Range:
    """Closed inclusive range in a canonical unit per measurand class."""

    minimum: float
    maximum: float
    canonical_unit: str  # for the log line; not enforced


# --- Range tables ------------------------------------------------------------
#
# Keys are the OCPP 1.6 § 7.20 measurand strings. Values are the
# acceptable range *after* unit conversion to the canonical unit
# named on each entry. Unit handling (Wh ↔ kWh ↔ MWh, W ↔ kW ↔ MW,
# °F ↔ °C ↔ K, etc.) lives in `_to_canonical` below — keep the
# physics in this table and the unit math in the converter.

# Energy registers: 10 GWh ceiling. A register is the meter's lifetime
# total, not one session, so it has to fit years of a busy DC charger:
# DE0240B1GPAC00004L read 177 MWh in October 2026 and grows ~140 MWh a
# year, and a 400 kW charger at full duty adds ~3.5 GWh a year. 10 GWh
# still rejects a Wh value labelled kWh on any register above 10 MWh.
# A single bad register sample inside a transaction is caught by
# `RegisterJumpGuard` below, which compares it to the session's own
# readings instead of a fixed number. Negative is allowed (export to
# grid / V2G accumulators). Symmetric.
_ENERGY_REGISTER_RANGE = _Range(
    minimum=-10_000_000_000, maximum=10_000_000_000, canonical_unit="Wh"
)

# Energy intervals: 100 MWh ceiling carried over from the pre-E5-4 cap.
# An interval is the energy of one sampling period; the largest
# passenger EV battery today is ~150 kWh and commercial fleet tops out
# around 1 MWh, so 100 MWh = ~600x margin. Symmetric.
_ENERGY_INTERVAL_RANGE = _Range(minimum=-100_000_000, maximum=100_000_000, canonical_unit="Wh")

# Power: ±1 MW. Megawatt charging (MCS) draft spec tops at ~3.75 MW
# but commercial deployment is capped at well under 1 MW today, and
# vehicle-to-grid export rarely exceeds 100 kW. 1 MW is a generous
# ceiling that catches W ↔ kW ↔ MW unit confusions.
_POWER_RANGE = _Range(minimum=-1_000_000, maximum=1_000_000, canonical_unit="W")

# Voltage: 0..1500 V. EU AC is 230/400 V; CCS DC tops at 1000 V; MCS
# at 1250 V; 1500 V leaves headroom. Negative voltage on a charger
# bus indicates a sensor fault or a sign-convention bug.
_VOLTAGE_RANGE = _Range(minimum=0, maximum=1500, canonical_unit="V")

# Current: ±1500 A. MCS spec lists higher peaks per-conductor but
# typical fast-charge today is 500 A. Bidirectional for export.
_CURRENT_RANGE = _Range(minimum=-1500, maximum=1500, canonical_unit="A")

# Frequency: 45..65 Hz. Grid is 50 (EU) or 60 (US/JP) Hz; ±10% covers
# every realistic operating condition. A reading outside this band is
# a sensor wedge, not a real measurement.
_FREQUENCY_RANGE = _Range(minimum=45, maximum=65, canonical_unit="Hz")

# Temperature: -40..+200 °C. Battery thermal runaway lives in this
# range; below -40 is a stuck-low sensor; above +200 is a stuck-high
# sensor (typical pin values: -32768, 0, 65535).
_TEMPERATURE_RANGE = _Range(minimum=-40, maximum=200, canonical_unit="Celsius")

# State of charge: 0..100 %. By definition.
_SOC_RANGE = _Range(minimum=0, maximum=100, canonical_unit="Percent")

# Power factor: -1..1, dimensionless. By definition.
_POWER_FACTOR_RANGE = _Range(minimum=-1, maximum=1, canonical_unit="")

# RPM: 0..30000. Defensive cap; OCPP rarely uses this measurand.
_RPM_RANGE = _Range(minimum=0, maximum=30_000, canonical_unit="RPM")


# Map each OCPP measurand string to its applicable range. Energy*
# variants share the same range; same for Power*, Current*, etc.
# Strings match OCPP 1.6 § 7.20 case-sensitively.
_MEASURAND_RANGES: dict[str, _Range] = {
    # Energy registers (lifetime totals) and intervals (one period).
    "Energy.Active.Export.Register": _ENERGY_REGISTER_RANGE,
    "Energy.Active.Import.Register": _ENERGY_REGISTER_RANGE,
    "Energy.Reactive.Export.Register": _ENERGY_REGISTER_RANGE,
    "Energy.Reactive.Import.Register": _ENERGY_REGISTER_RANGE,
    "Energy.Active.Export.Interval": _ENERGY_INTERVAL_RANGE,
    "Energy.Active.Import.Interval": _ENERGY_INTERVAL_RANGE,
    "Energy.Reactive.Export.Interval": _ENERGY_INTERVAL_RANGE,
    "Energy.Reactive.Import.Interval": _ENERGY_INTERVAL_RANGE,
    # Power.
    "Power.Active.Export": _POWER_RANGE,
    "Power.Active.Import": _POWER_RANGE,
    "Power.Offered": _POWER_RANGE,
    "Power.Reactive.Export": _POWER_RANGE,
    "Power.Reactive.Import": _POWER_RANGE,
    "Power.Factor": _POWER_FACTOR_RANGE,
    # Current.
    "Current.Import": _CURRENT_RANGE,
    "Current.Export": _CURRENT_RANGE,
    "Current.Offered": _CURRENT_RANGE,
    # Other physical measurands.
    "Voltage": _VOLTAGE_RANGE,
    "Frequency": _FREQUENCY_RANGE,
    "Temperature": _TEMPERATURE_RANGE,
    "SoC": _SOC_RANGE,
    "RPM": _RPM_RANGE,
}


# --- Unit conversion ---------------------------------------------------------

# Multiplicative conversion factors to the canonical unit. Anything
# not listed here uses the raw value (e.g. unitless quantities like
# SoC or Power.Factor). Keys are case-insensitive (we lower() before
# lookup) so vendor variations like `KWh` / `kWh` / `kwh` all match.
_UNIT_SCALE: dict[str, float] = {
    # Energy → Wh.
    "wh": 1.0,
    "kwh": 1_000.0,
    "mwh": 1_000_000.0,
    # Power → W.
    "w": 1.0,
    "kw": 1_000.0,
    "mw": 1_000_000.0,
    "kva": 1_000.0,  # apparent power; treated like W for sanity bounds
    "var": 1.0,
    "kvar": 1_000.0,
    # Current → A.
    "a": 1.0,
    # Voltage → V.
    "v": 1.0,
    # Frequency → Hz. (Spec also allows omitting the unit on
    # Frequency, in which case we accept the raw value.)
    "hz": 1.0,
    # SoC, Power.Factor, RPM, Phase Angle — leave as raw.
    "percent": 1.0,
    "%": 1.0,
}


def _temperature_to_celsius(value: float, unit_lower: str) -> float:
    """Temperature is the only measurand needing a non-multiplicative
    conversion. OCPP 1.6 lists `Celsius`, `Fahrenheit`, and `K` as
    valid; default (no unit) is Celsius."""
    if unit_lower in ("fahrenheit", "f"):
        return (value - 32.0) * 5.0 / 9.0
    if unit_lower in ("k", "kelvin"):
        return value - 273.15
    # Celsius / "celsius" / unknown → assume Celsius.
    return value


def _to_canonical(measurand: str, value: float, unit: str) -> float:
    """Convert ``value`` from its reported unit to the canonical unit
    for ``measurand``. Returns the unconverted value when the unit is
    unknown — the range check below will then either let it through
    (small unit-less values, e.g. Power.Factor) or reject it (large
    values that fail any plausible range)."""
    unit_lower = unit.lower().strip()
    if measurand == "Temperature":
        return _temperature_to_celsius(value, unit_lower)
    scale = _UNIT_SCALE.get(unit_lower)
    if scale is None:
        return value
    return value * scale


# --- Public validator --------------------------------------------------------


@dataclass(frozen=True)
class SanityResult:
    """Outcome of a single-sample check.

    `accepted=True` means the sample is fine to forward. `accepted=False`
    means quarantine; `reason` carries the machine-readable label that
    feeds the Prometheus counter (`out_of_range`, `unparseable`,
    `not_finite`, `register_jump`).
    """

    accepted: bool
    reason: str = ""
    measurand: str = ""
    canonical_value: float = 0.0


# Reasons. Closed enum so the metrics label cardinality stays bounded.
REASON_OUT_OF_RANGE = "out_of_range"
REASON_UNPARSEABLE = "unparseable"
REASON_NOT_FINITE = "not_finite"
REASON_REGISTER_JUMP = "register_jump"


def check_sample(raw: dict[str, Any]) -> SanityResult:
    """Check one OCPP `sampledValue` dict.

    The handler calls this for every entry under
    `meterValue[*].sampledValue[*]`. Returns `accepted=True` for
    samples that pass; `False` with a reason for samples to drop.

    Defaults match OCPP 1.6 § 7.20: missing `measurand` →
    `Energy.Active.Import.Register`; missing `unit` on energy → `Wh`.
    """
    measurand = str(raw.get("measurand") or "Energy.Active.Import.Register")
    unit = str(raw.get("unit") or "")

    # Parse the value first — non-numeric strings fail fast.
    try:
        magnitude = float(raw.get("value") or 0)
    except (TypeError, ValueError):
        return SanityResult(accepted=False, reason=REASON_UNPARSEABLE, measurand=measurand)

    # NaN / inf never represents a real measurement; quarantine.
    if not math.isfinite(magnitude):
        return SanityResult(accepted=False, reason=REASON_NOT_FINITE, measurand=measurand)

    canonical = _to_canonical(measurand, magnitude, unit)

    # Unknown measurand: accept by default. See module docstring.
    range_ = _MEASURAND_RANGES.get(measurand)
    if range_ is None:
        return SanityResult(
            accepted=True,
            measurand=measurand,
            canonical_value=canonical,
        )

    if not (range_.minimum <= canonical <= range_.maximum):
        return SanityResult(
            accepted=False,
            reason=REASON_OUT_OF_RANGE,
            measurand=measurand,
            canonical_value=canonical,
        )

    return SanityResult(accepted=True, measurand=measurand, canonical_value=canonical)


# --- Per-transaction register check ------------------------------------------

# Largest step an energy register may take between two samples of one
# transaction: 2 MWh. A truck-sized battery is ~1 MWh, so even a
# charger that was offline for a whole session cannot move further
# between two samples, while the garbage this exists for (a Wh value
# labelled kWh, a pinned 0xFFFFFFFF, a bit flip) is orders of magnitude
# larger. Replaying the 1.37M register samples the gateway received
# from May to October 2026 gave a largest step of ~36 kWh and no drops.
_MAX_REGISTER_STEP_WH = 2_000_000.0

# A register that stays away from the reference for this many
# consecutive samples, each within one step of the previous one, has
# really moved (meter swap, a wrong first sample) and becomes the new
# reference instead of being dropped for the rest of the transaction.
_REGISTER_CONFIRM_SAMPLES = 3


@dataclass
class _RegisterSeries:
    transaction_id: int
    reference: float
    candidate: float | None = None
    candidate_count: int = 0


class RegisterJumpGuard:
    """Drop single register samples that jump away from their transaction.

    One instance per charger connection (`EveysChargePoint`). It keeps,
    per connector and register series (measurand, phase, location), the
    highest reading accepted in the current transaction and drops a
    reading more than `_MAX_REGISTER_STEP_WH` away from it.

    Deliberately not a strict monotonicity check: live chargers flush
    queued frames out of order after a reconnect (steps back of up to
    ~17 kWh in receipt order, May to October 2026), and dropping those
    real readings would do more harm than forwarding them. Time-based
    limits (power x elapsed time) are not used either, because queued
    frames carry the same or a wrong charger timestamp and arrive in
    bursts.

    Zero and negative readings pass untouched: several chargers send
    `0` on clock-aligned samples between real readings, and they must
    neither become the reference nor be dropped. Samples outside a
    transaction (`transaction_id` missing or 0) are not checked. State
    is in memory only; a reconnect starts from the next reading.
    """

    def __init__(
        self,
        *,
        max_step_wh: float = _MAX_REGISTER_STEP_WH,
        confirm_samples: int = _REGISTER_CONFIRM_SAMPLES,
    ) -> None:
        self._max_step_wh = max_step_wh
        self._confirm_samples = confirm_samples
        self._series: dict[tuple[int, str, str, str], _RegisterSeries] = {}

    def check(
        self,
        verdict: SanityResult,
        *,
        connector_id: int,
        transaction_id: int | None,
        phase: str = "",
        location: str = "",
    ) -> SanityResult:
        """Return `verdict` unchanged, or a `register_jump` rejection."""
        if not verdict.accepted or not verdict.measurand.endswith(".Register"):
            return verdict
        if not transaction_id:
            return verdict
        value = verdict.canonical_value
        if value <= 0:
            return verdict

        key = (connector_id, verdict.measurand, phase, location)
        series = self._series.get(key)
        if series is None or series.transaction_id != transaction_id:
            self._series[key] = _RegisterSeries(transaction_id=transaction_id, reference=value)
            return verdict

        if abs(value - series.reference) <= self._max_step_wh:
            series.reference = max(series.reference, value)
            series.candidate = None
            series.candidate_count = 0
            return verdict

        if series.candidate is not None and abs(value - series.candidate) <= self._max_step_wh:
            series.candidate_count += 1
        else:
            series.candidate_count = 1
        series.candidate = value
        if series.candidate_count >= self._confirm_samples:
            series.reference = value
            series.candidate = None
            series.candidate_count = 0
            return verdict

        return SanityResult(
            accepted=False,
            reason=REASON_REGISTER_JUMP,
            measurand=verdict.measurand,
            canonical_value=value,
        )
