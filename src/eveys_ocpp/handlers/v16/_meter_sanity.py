"""Measurand-aware sanity-range validation for `MeterValues` samples (E5-4).

A charger reporting nonsense — a 100 MWh single-sample energy reading,
a 50 kV bus voltage, a -200°C battery temperature — is almost always
one of three things:

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

# Energy: 100 MWh ceiling carried over from the pre-E5-4 cap. The
# largest passenger EV battery today is ~150 kWh; commercial fleet
# tops out around 1 MWh; 100 MWh = ~600x margin. Negative is
# allowed (export to grid / V2G accumulators). Symmetric.
_ENERGY_RANGE = _Range(minimum=-100_000_000, maximum=100_000_000, canonical_unit="Wh")

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
    # Energy registers + intervals — same physical quantity.
    "Energy.Active.Export.Register": _ENERGY_RANGE,
    "Energy.Active.Import.Register": _ENERGY_RANGE,
    "Energy.Reactive.Export.Register": _ENERGY_RANGE,
    "Energy.Reactive.Import.Register": _ENERGY_RANGE,
    "Energy.Active.Export.Interval": _ENERGY_RANGE,
    "Energy.Active.Import.Interval": _ENERGY_RANGE,
    "Energy.Reactive.Export.Interval": _ENERGY_RANGE,
    "Energy.Reactive.Import.Interval": _ENERGY_RANGE,
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
    `not_finite`).
    """

    accepted: bool
    reason: str = ""
    measurand: str = ""
    canonical_value: float = 0.0


# Reasons. Closed enum so the metrics label cardinality stays bounded.
REASON_OUT_OF_RANGE = "out_of_range"
REASON_UNPARSEABLE = "unparseable"
REASON_NOT_FINITE = "not_finite"


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
