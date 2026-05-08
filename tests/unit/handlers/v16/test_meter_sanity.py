"""Unit tests for the E5-4 sanity validator (`_meter_sanity.check_sample`).

Pure function — no fixtures, no async. One test per behaviour, named
after what's being asserted, not the inputs.
"""

from __future__ import annotations

from typing import Any

import pytest

from eveys_ocpp.handlers.v16 import _meter_sanity
from eveys_ocpp.handlers.v16._meter_sanity import (
    REASON_NOT_FINITE,
    REASON_OUT_OF_RANGE,
    REASON_UNPARSEABLE,
    check_sample,
)


def _sample(
    value: str,
    *,
    measurand: str = "Energy.Active.Import.Register",
    unit: str | None = None,
) -> dict[str, Any]:
    raw: dict[str, Any] = {"value": value, "measurand": measurand}
    if unit is not None:
        raw["unit"] = unit
    return raw


# -- Energy -------------------------------------------------------------------


def test_energy_in_wh_accepts_typical_session_total() -> None:
    # 30 kWh session: typical passenger-EV charge.
    result = check_sample(_sample("30000", unit="Wh"))
    assert result.accepted is True
    assert result.measurand == "Energy.Active.Import.Register"


def test_energy_in_kwh_unit_converts_before_range_check() -> None:
    # 50 kWh = 50_000 Wh — well under the 100 MWh ceiling.
    result = check_sample(_sample("50", unit="kWh"))
    assert result.accepted is True
    assert result.canonical_value == 50_000


def test_energy_above_100_mwh_quarantined() -> None:
    # 101 MWh — the textbook unit-confusion bug.
    result = check_sample(_sample("101", unit="MWh"))
    assert result.accepted is False
    assert result.reason == REASON_OUT_OF_RANGE


def test_energy_below_negative_100_mwh_quarantined() -> None:
    # Symmetric ceiling for export accumulators.
    result = check_sample(_sample("-101000000", unit="Wh"))
    assert result.accepted is False
    assert result.reason == REASON_OUT_OF_RANGE


def test_energy_default_measurand_when_missing() -> None:
    # OCPP § 7.20 defaults missing measurand to Energy.Active.Import.Register.
    result = check_sample({"value": "1234"})
    assert result.accepted is True
    assert result.measurand == "Energy.Active.Import.Register"


# -- Voltage ------------------------------------------------------------------


def test_voltage_400v_typical_three_phase_line_accepted() -> None:
    result = check_sample(_sample("400", measurand="Voltage", unit="V"))
    assert result.accepted is True


def test_voltage_above_1500v_quarantined() -> None:
    result = check_sample(_sample("2000", measurand="Voltage", unit="V"))
    assert result.accepted is False
    assert result.reason == REASON_OUT_OF_RANGE


def test_voltage_negative_quarantined() -> None:
    # No reasonable charger reports negative voltage; it's a sensor wedge.
    result = check_sample(_sample("-50", measurand="Voltage", unit="V"))
    assert result.accepted is False
    assert result.reason == REASON_OUT_OF_RANGE


# -- Current ------------------------------------------------------------------


def test_current_500a_fast_charger_accepted() -> None:
    result = check_sample(_sample("500", measurand="Current.Import", unit="A"))
    assert result.accepted is True


def test_current_negative_for_export_accepted() -> None:
    # V2G / export: current goes negative.
    result = check_sample(_sample("-100", measurand="Current.Export", unit="A"))
    assert result.accepted is True


def test_current_above_1500a_quarantined() -> None:
    result = check_sample(_sample("3000", measurand="Current.Import", unit="A"))
    assert result.accepted is False
    assert result.reason == REASON_OUT_OF_RANGE


# -- Power --------------------------------------------------------------------


def test_power_in_kw_unit_converts() -> None:
    # 150 kW DC fast charge.
    result = check_sample(_sample("150", measurand="Power.Active.Import", unit="kW"))
    assert result.accepted is True
    assert result.canonical_value == 150_000


def test_power_above_1mw_quarantined() -> None:
    # 2 MW report — unit confusion or sensor fault.
    result = check_sample(_sample("2", measurand="Power.Active.Import", unit="MW"))
    assert result.accepted is False
    assert result.reason == REASON_OUT_OF_RANGE


# -- Frequency ----------------------------------------------------------------


def test_frequency_50hz_accepted() -> None:
    result = check_sample(_sample("50", measurand="Frequency", unit="Hz"))
    assert result.accepted is True


def test_frequency_120hz_quarantined() -> None:
    # No grid runs at 120 Hz; sensor wedge or doubled-reading bug.
    result = check_sample(_sample("120", measurand="Frequency", unit="Hz"))
    assert result.accepted is False


# -- Temperature --------------------------------------------------------------


def test_temperature_25c_accepted() -> None:
    result = check_sample(_sample("25", measurand="Temperature", unit="Celsius"))
    assert result.accepted is True


def test_temperature_77f_converts_to_25c_accepted() -> None:
    # 77 °F = 25 °C.
    result = check_sample(_sample("77", measurand="Temperature", unit="Fahrenheit"))
    assert result.accepted is True
    assert result.canonical_value == pytest.approx(25.0, rel=1e-3)


def test_temperature_kelvin_converts_to_celsius() -> None:
    # 298.15 K = 25 °C.
    result = check_sample(_sample("298.15", measurand="Temperature", unit="K"))
    assert result.accepted is True
    assert result.canonical_value == pytest.approx(25.0, rel=1e-3)


def test_temperature_below_minus_40c_quarantined() -> None:
    # Stuck-low sensor.
    result = check_sample(_sample("-100", measurand="Temperature", unit="Celsius"))
    assert result.accepted is False


def test_temperature_above_200c_quarantined() -> None:
    # Stuck-high or pinned-int sensor.
    result = check_sample(_sample("500", measurand="Temperature", unit="Celsius"))
    assert result.accepted is False


# -- SoC, Power Factor, RPM ---------------------------------------------------


def test_soc_100_percent_accepted() -> None:
    result = check_sample(_sample("100", measurand="SoC", unit="Percent"))
    assert result.accepted is True


def test_soc_above_100_quarantined() -> None:
    result = check_sample(_sample("125", measurand="SoC", unit="Percent"))
    assert result.accepted is False


def test_power_factor_within_unit_circle_accepted() -> None:
    assert check_sample(_sample("0.95", measurand="Power.Factor")).accepted is True
    assert check_sample(_sample("-0.5", measurand="Power.Factor")).accepted is True


def test_power_factor_outside_unit_circle_quarantined() -> None:
    assert check_sample(_sample("1.5", measurand="Power.Factor")).accepted is False


# -- Pathological inputs ------------------------------------------------------


def test_unparseable_value_string_returns_unparseable_reason() -> None:
    result = check_sample(_sample("not-a-number"))
    assert result.accepted is False
    assert result.reason == REASON_UNPARSEABLE


def test_nan_returns_not_finite_reason() -> None:
    result = check_sample(_sample("nan"))
    assert result.accepted is False
    assert result.reason == REASON_NOT_FINITE


def test_inf_returns_not_finite_reason() -> None:
    result = check_sample(_sample("inf"))
    assert result.accepted is False
    assert result.reason == REASON_NOT_FINITE


def test_unknown_measurand_accepts_by_default() -> None:
    # Vendor extension or future OCPP measurand: pass through.
    result = check_sample(_sample("1234", measurand="Vendor.Custom.Metric", unit="custom-unit"))
    assert result.accepted is True
    assert result.measurand == "Vendor.Custom.Metric"


def test_unknown_unit_does_not_scale_value() -> None:
    # If the unit isn't in the conversion table, we use the raw value.
    # 500 raw on Voltage with unit "kV" would be 500 in the canonical
    # check (not scaled to kilovolts) — quarantine kicks in only when
    # the raw value already exceeds the range.
    result = check_sample(_sample("500", measurand="Voltage", unit="kV"))
    # 500 V (raw, unscaled because "kV" isn't in our scale table)
    # falls inside 0..1500, so this passes — defensible default.
    assert result.accepted is True


def test_missing_value_treated_as_zero_and_accepted() -> None:
    # `value` absent → default 0 → in range for energy/voltage/etc.
    result = check_sample({"measurand": "Voltage"})
    assert result.accepted is True


def test_case_insensitive_unit_lookup() -> None:
    # Vendors emit "KWh" / "kwh" / "kWh"; all should normalise.
    for unit in ("KWh", "kwh", "kWh"):
        result = check_sample(_sample("50", unit=unit))
        assert result.accepted is True, f"unit={unit!r} should normalise"
        assert result.canonical_value == 50_000


# -- Module surface guard -----------------------------------------------------


def test_reason_constants_form_closed_enum() -> None:
    """Cardinality on the Prometheus counter depends on the reason
    label being a small closed set. Catch a sneaky new value."""
    expected = {REASON_OUT_OF_RANGE, REASON_UNPARSEABLE, REASON_NOT_FINITE}
    assert expected == {v for k, v in vars(_meter_sanity).items() if k.startswith("REASON_")}
