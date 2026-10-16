"""Unit tests for the E5-4 sanity validator (`_meter_sanity.check_sample`)
and the per-transaction register check (`RegisterJumpGuard`).

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
    REASON_REGISTER_JUMP,
    REASON_UNPARSEABLE,
    RegisterJumpGuard,
    SanityResult,
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


@pytest.mark.parametrize("value", ["177038500", "138921890"])
def test_energy_register_above_100_mwh_accepted(value: str) -> None:
    # Lifetime registers of DE0240B1GPAC00004L in October 2026. The old
    # 100 MWh cap quarantined every energy sample of this charger.
    result = check_sample(_sample(value, unit="Wh"))
    assert result.accepted is True
    assert result.canonical_value == float(value)


def test_energy_register_at_10_gwh_accepted() -> None:
    result = check_sample(_sample("10000", unit="MWh"))
    assert result.accepted is True


def test_energy_register_above_10_gwh_quarantined() -> None:
    result = check_sample(_sample("10001", unit="MWh"))
    assert result.accepted is False
    assert result.reason == REASON_OUT_OF_RANGE


def test_energy_register_wh_value_labelled_kwh_quarantined() -> None:
    # 177 MWh register sent in Wh but labelled kWh: 177 GWh.
    result = check_sample(_sample("177038500", unit="kWh"))
    assert result.accepted is False
    assert result.reason == REASON_OUT_OF_RANGE


def test_energy_register_below_negative_10_gwh_quarantined() -> None:
    # Symmetric ceiling for export accumulators.
    result = check_sample(_sample("-10000000001", unit="Wh"))
    assert result.accepted is False
    assert result.reason == REASON_OUT_OF_RANGE


@pytest.mark.parametrize(
    "measurand",
    ["Energy.Active.Import.Interval", "Energy.Active.Export.Interval"],
)
def test_energy_interval_keeps_100_mwh_cap(measurand: str) -> None:
    # An interval is one sampling period, not a lifetime total.
    assert check_sample(_sample("50", measurand=measurand, unit="kWh")).accepted is True
    result = check_sample(_sample("101", measurand=measurand, unit="MWh"))
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
    expected = {
        REASON_OUT_OF_RANGE,
        REASON_UNPARSEABLE,
        REASON_NOT_FINITE,
        REASON_REGISTER_JUMP,
    }
    assert expected == {v for k, v in vars(_meter_sanity).items() if k.startswith("REASON_")}


# -- Register jumps inside a transaction --------------------------------------


def _feed(
    guard: RegisterJumpGuard,
    value: str,
    *,
    transaction_id: int | None = 7,
    connector_id: int = 1,
    measurand: str = "Energy.Active.Import.Register",
    unit: str = "Wh",
    phase: str = "",
    location: str = "Outlet",
) -> SanityResult:
    return guard.check(
        check_sample(_sample(value, measurand=measurand, unit=unit)),
        connector_id=connector_id,
        transaction_id=transaction_id,
        phase=phase,
        location=location,
    )


def _accepted(guard: RegisterJumpGuard, values: list[str], **kwargs: Any) -> list[bool]:
    return [_feed(guard, value, **kwargs).accepted for value in values]


def test_register_guard_accepts_first_reading_and_normal_steps() -> None:
    # Real 15 s samples of DE0240B1GPAC00004L connector 2, tx 5033.
    guard = RegisterJumpGuard()
    values = ["138921390", "138921593", "138921703", "138921890"]
    assert _accepted(guard, values, transaction_id=5033) == [True] * 4


def test_register_guard_accepts_out_of_order_step_back() -> None:
    # Queued Sample.Clock frames flushed after a reconnect arrive out of
    # order (AE0022A1GP4C00725J, 5 Oct 2026): real readings, keep them.
    guard = RegisterJumpGuard()
    values = ["43202492", "43213244", "43207885", "43197123", "43186392", "43218650"]
    assert _accepted(guard, values) == [True] * 6


def test_register_guard_drops_single_spike_and_keeps_following_readings() -> None:
    guard = RegisterJumpGuard()
    assert _feed(guard, "70903093").accepted is True
    spike = _feed(guard, "4294967295")
    assert spike.accepted is False
    assert spike.reason == REASON_REGISTER_JUMP
    assert spike.measurand == "Energy.Active.Import.Register"
    assert spike.canonical_value == 4294967295
    assert _feed(guard, "70903300").accepted is True


def test_register_guard_drops_kwh_value_labelled_wh() -> None:
    # 177038.5 kWh sent with unit Wh: 177 kWh, far below the session.
    guard = RegisterJumpGuard()
    assert _feed(guard, "177038500").accepted is True
    result = _feed(guard, "177038.5")
    assert result.accepted is False
    assert result.reason == REASON_REGISTER_JUMP


def test_register_guard_accepts_step_up_to_limit() -> None:
    guard = RegisterJumpGuard()
    assert _accepted(guard, ["10000000", "12000000", "14000001"]) == [True, True, False]


def test_register_guard_reference_is_highest_reading() -> None:
    # Steps back are measured from the highest reading, so a register
    # cannot walk down in accepted steps.
    guard = RegisterJumpGuard()
    assert _accepted(guard, ["10000000", "8200000", "6500000"]) == [True, True, False]


def test_register_guard_moves_reference_after_three_consistent_readings() -> None:
    # Meter swapped mid-transaction, or the first reading was the bad one.
    guard = RegisterJumpGuard()
    values = ["177038500", "500000", "500200", "500400", "500600", "177038600"]
    assert _accepted(guard, values) == [True, False, False, True, True, False]


def test_register_guard_never_moves_reference_to_interleaved_garbage() -> None:
    guard = RegisterJumpGuard()
    values = ["1000000", "4294967295"] * 5
    assert _accepted(guard, values) == [True, False] * 5


def test_register_guard_restarts_count_when_candidates_disagree() -> None:
    guard = RegisterJumpGuard()
    values = ["1000000", "9000000", "50000000", "9000100", "9000200", "9000300"]
    assert _accepted(guard, values) == [True, False, False, False, False, True]


def test_register_guard_lets_zero_through_without_using_it() -> None:
    # DE0240B1GPAC00004L sends 0 on Sample.Clock next to its real register.
    guard = RegisterJumpGuard()
    values = ["0", "111047296", "0", "111047400", "0"]
    assert _accepted(guard, values) == [True] * 5


def test_register_guard_starts_over_on_new_transaction() -> None:
    guard = RegisterJumpGuard()
    assert _feed(guard, "1000000", transaction_id=1).accepted is True
    assert _feed(guard, "9000000", transaction_id=2).accepted is True
    assert _feed(guard, "9000100", transaction_id=2).accepted is True


@pytest.mark.parametrize("transaction_id", [None, 0])
def test_register_guard_skips_samples_outside_transaction(transaction_id: int | None) -> None:
    guard = RegisterJumpGuard()
    values = ["1000000", "9000000", "1000000"]
    assert _accepted(guard, values, transaction_id=transaction_id) == [True] * 3


def test_register_guard_keeps_series_apart() -> None:
    guard = RegisterJumpGuard()
    assert _feed(guard, "177038500", connector_id=1).accepted is True
    assert _feed(guard, "138908000", connector_id=2).accepted is True
    assert _feed(guard, "9000000", connector_id=3).accepted is True
    assert _feed(guard, "3000000", connector_id=3, phase="L1").accepted is True
    assert _feed(guard, "30000000", connector_id=3, location="Inlet").accepted is True
    export = _feed(guard, "500000", connector_id=3, measurand="Energy.Active.Export.Register")
    assert export.accepted is True


@pytest.mark.parametrize(
    ("measurand", "unit"),
    [("Energy.Active.Import.Interval", "Wh"), ("Power.Active.Import", "W")],
)
def test_register_guard_ignores_other_measurands(measurand: str, unit: str) -> None:
    guard = RegisterJumpGuard()
    values = ["1000", "90000000", "1000"] if unit == "Wh" else ["1000", "900000", "1000"]
    assert _accepted(guard, values, measurand=measurand, unit=unit) == [True] * 3


def test_register_guard_passes_earlier_rejection_through_untouched() -> None:
    guard = RegisterJumpGuard()
    assert _feed(guard, "1000000").accepted is True
    result = _feed(guard, "nan")
    assert result.accepted is False
    assert result.reason == REASON_NOT_FINITE
    out_of_range = _feed(guard, "20000000000")
    assert out_of_range.reason == REASON_OUT_OF_RANGE
    # Neither rejection counts toward moving the reference.
    assert _accepted(guard, ["1000100", "1000200"]) == [True, True]


def test_register_guard_converts_units_before_comparing() -> None:
    guard = RegisterJumpGuard()
    assert _feed(guard, "70903093", unit="Wh").accepted is True
    assert _feed(guard, "70903.2", unit="kWh").accepted is True
