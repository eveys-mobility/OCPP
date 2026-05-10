"""Round-trip + unknown-value behaviour for the OCPP enum translation
tables (#136).

The maps are derived: `OCPP_BY_*_NAME` is built by inverting
`*_BY_OCPP` at import. A typo in one would silently desync from the
other; these tests prove the round-trip holds for every entry."""

from __future__ import annotations

import pytest

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp._ocpp_enums import (
    MEASURAND_BY_OCPP,
    OCPP_BY_MEASURAND_NAME,
    OCPP_BY_PHASE_NAME,
    OCPP_BY_UNIT_NAME,
    PHASE_BY_OCPP,
    UNIT_BY_OCPP,
    ocpp_string_for,
    proto_enum_name_for_measurand,
    proto_enum_name_for_phase,
)


@pytest.mark.parametrize(
    ("by_ocpp", "by_proto_name", "enum_descriptor_name"),
    [
        (MEASURAND_BY_OCPP, OCPP_BY_MEASURAND_NAME, "Measurand"),
        (PHASE_BY_OCPP, OCPP_BY_PHASE_NAME, "Phase"),
        (UNIT_BY_OCPP, OCPP_BY_UNIT_NAME, "Unit"),
    ],
)
def test_round_trip_holds_for_every_mapped_value(
    by_ocpp: dict[str, int],
    by_proto_name: dict[str, str],
    enum_descriptor_name: str,
) -> None:
    """For each OCPP wire string, mapping wire→proto→name→wire must
    return the original. A typo in one direction would desync silently
    without this guard."""
    desc = events_pb2.DESCRIPTOR.enum_types_by_name[enum_descriptor_name]
    for wire, proto_value in by_ocpp.items():
        proto_name = desc.values_by_number[proto_value].name
        assert by_proto_name[proto_name] == wire, (
            f"{enum_descriptor_name}: {wire!r} → proto value {proto_value} "
            f"→ name {proto_name!r} → wire {by_proto_name.get(proto_name)!r}"
            f" (expected {wire!r})"
        )


def test_proto_enum_name_for_measurand_handles_known_unknown_and_none() -> None:
    """API filter helper: known wire string → proto name; unknown
    wire string → None (caller short-circuits the page); None → None."""
    assert proto_enum_name_for_measurand("Voltage") == "MEASURAND_VOLTAGE"
    assert proto_enum_name_for_measurand("SoC") == "MEASURAND_SOC"
    assert proto_enum_name_for_measurand("Vendor.Custom.Reading") is None
    assert proto_enum_name_for_measurand(None) is None


def test_proto_enum_name_for_phase_handles_dashed_form() -> None:
    """Phase has dash-form variants (`L1-N`) that aren't a mechanical
    underscore-replace of the proto enum name (`PHASE_L1_N`)."""
    assert proto_enum_name_for_phase("L1") == "PHASE_L1"
    assert proto_enum_name_for_phase("L1-N") == "PHASE_L1_N"
    assert proto_enum_name_for_phase("L2-L3") == "PHASE_L2_L3"
    assert proto_enum_name_for_phase("Vendor.Phase") is None


def test_ocpp_string_for_unspecified_returns_none() -> None:
    """The internal `*_UNSPECIFIED` sentinel must NOT leak to API
    clients — it's how the storage layer encodes "the charger sent
    something we don't recognise." Surface as `null` so consumers
    treat it the same as an absent dimension."""
    assert ocpp_string_for("measurand", "MEASURAND_UNSPECIFIED") is None
    assert ocpp_string_for("phase", "PHASE_UNSPECIFIED") is None
    assert ocpp_string_for("unit", "UNIT_UNSPECIFIED") is None


def test_ocpp_string_for_empty_or_none_returns_none() -> None:
    """Storage-side empty strings (column never written) and None
    (missing key) both surface as `null`, identical to UNSPECIFIED."""
    assert ocpp_string_for("measurand", "") is None
    assert ocpp_string_for("measurand", None) is None


def test_ocpp_string_for_unknown_proto_name_returns_none() -> None:
    """A vendor-extension string the ingestor stored verbatim (no
    proto mapping) won't appear in the inverse table — surface as
    `null` rather than leak the raw string we can't promise to be
    OCPP-spec-compliant."""
    assert ocpp_string_for("measurand", "Vendor.Custom.Reading") is None


def test_ocpp_string_for_round_trips_known_proto_names() -> None:
    assert ocpp_string_for("measurand", "MEASURAND_VOLTAGE") == "Voltage"
    assert ocpp_string_for("phase", "PHASE_L1") == "L1"
    assert ocpp_string_for("phase", "PHASE_L1_N") == "L1-N"
    assert ocpp_string_for("unit", "UNIT_V") == "V"
    assert ocpp_string_for("context", "CONTEXT_SAMPLE_PERIODIC") == "Sample.Periodic"
    assert ocpp_string_for("location", "LOCATION_OUTLET") == "Outlet"


def test_ocpp_string_for_unknown_field_raises() -> None:
    with pytest.raises(KeyError):
        ocpp_string_for("not_a_real_field", "MEASURAND_VOLTAGE")
