"""OCPP wire-string ↔ proto enum translation tables.

The proto schema (`proto/events/v1/events.proto`) models MeterValue
dimensions (measurand, phase, unit, context, format, location) as
strongly-typed enums. OCPP 1.6 §6.21 serializes them on the wire as
string literals — `"Voltage"`, `"L1-N"`, `"Sample.Periodic"`, etc.

Two translation paths share these tables:

- **Ingest** (`handlers/v16/meter_values.py`): wire string → proto enum
  value. Stored in ClickHouse as the proto enum *name* (e.g.
  `MEASURAND_VOLTAGE`).
- **Read** (`api/timeseries.py`, `clickhouse/read_client.py`): proto
  enum name → OCPP wire string for API responses, and OCPP wire string
  → proto enum name for query-param filters.

Co-locating both directions in one module guarantees the round-trip
stays consistent. Adding a new measurand means editing one table.

Unknown values:

- ingest: fall through to `*_UNSPECIFIED` (vendor extensions; the raw
  `value` is still captured).
- read: surface `None` rather than leak the proto enum name to API
  clients.
"""

from __future__ import annotations

from eveys_ocpp._generated.events.v1 import events_pb2

# --- Wire string → proto enum value (used at ingest) ------------------------
#
# Comparisons are case-sensitive per the spec; chargers reporting
# non-canonical casing land in `*_UNSPECIFIED`.

MEASURAND_BY_OCPP: dict[str, int] = {
    "Energy.Active.Export.Register": events_pb2.MEASURAND_ENERGY_ACTIVE_EXPORT_REGISTER,
    "Energy.Active.Import.Register": events_pb2.MEASURAND_ENERGY_ACTIVE_IMPORT_REGISTER,
    "Energy.Reactive.Export.Register": events_pb2.MEASURAND_ENERGY_REACTIVE_EXPORT_REGISTER,
    "Energy.Reactive.Import.Register": events_pb2.MEASURAND_ENERGY_REACTIVE_IMPORT_REGISTER,
    "Energy.Active.Export.Interval": events_pb2.MEASURAND_ENERGY_ACTIVE_EXPORT_INTERVAL,
    "Energy.Active.Import.Interval": events_pb2.MEASURAND_ENERGY_ACTIVE_IMPORT_INTERVAL,
    "Energy.Reactive.Export.Interval": events_pb2.MEASURAND_ENERGY_REACTIVE_EXPORT_INTERVAL,
    "Energy.Reactive.Import.Interval": events_pb2.MEASURAND_ENERGY_REACTIVE_IMPORT_INTERVAL,
    "Power.Active.Export": events_pb2.MEASURAND_POWER_ACTIVE_EXPORT,
    "Power.Active.Import": events_pb2.MEASURAND_POWER_ACTIVE_IMPORT,
    "Power.Offered": events_pb2.MEASURAND_POWER_OFFERED,
    "Power.Reactive.Export": events_pb2.MEASURAND_POWER_REACTIVE_EXPORT,
    "Power.Reactive.Import": events_pb2.MEASURAND_POWER_REACTIVE_IMPORT,
    "Power.Factor": events_pb2.MEASURAND_POWER_FACTOR,
    "Current.Import": events_pb2.MEASURAND_CURRENT_IMPORT,
    "Current.Export": events_pb2.MEASURAND_CURRENT_EXPORT,
    "Current.Offered": events_pb2.MEASURAND_CURRENT_OFFERED,
    "Voltage": events_pb2.MEASURAND_VOLTAGE,
    "Frequency": events_pb2.MEASURAND_FREQUENCY,
    "Temperature": events_pb2.MEASURAND_TEMPERATURE,
    "SoC": events_pb2.MEASURAND_SOC,
    "RPM": events_pb2.MEASURAND_RPM,
}

PHASE_BY_OCPP: dict[str, int] = {
    "L1": events_pb2.PHASE_L1,
    "L2": events_pb2.PHASE_L2,
    "L3": events_pb2.PHASE_L3,
    "N": events_pb2.PHASE_N,
    "L1-N": events_pb2.PHASE_L1_N,
    "L2-N": events_pb2.PHASE_L2_N,
    "L3-N": events_pb2.PHASE_L3_N,
    "L1-L2": events_pb2.PHASE_L1_L2,
    "L2-L3": events_pb2.PHASE_L2_L3,
    "L3-L1": events_pb2.PHASE_L3_L1,
}

UNIT_BY_OCPP: dict[str, int] = {
    "Wh": events_pb2.UNIT_WH,
    "kWh": events_pb2.UNIT_KWH,
    "varh": events_pb2.UNIT_VARH,
    "kvarh": events_pb2.UNIT_KVARH,
    "W": events_pb2.UNIT_W,
    "kW": events_pb2.UNIT_KW,
    "var": events_pb2.UNIT_VAR,
    "kvar": events_pb2.UNIT_KVAR,
    "VA": events_pb2.UNIT_VA,
    "kVA": events_pb2.UNIT_KVA,
    "A": events_pb2.UNIT_A,
    "V": events_pb2.UNIT_V,
    "Celsius": events_pb2.UNIT_CELSIUS,
    "Fahrenheit": events_pb2.UNIT_FAHRENHEIT,
    "K": events_pb2.UNIT_K,
    "Percent": events_pb2.UNIT_PERCENT,
    "Hertz": events_pb2.UNIT_HERTZ,
}

CONTEXT_BY_OCPP: dict[str, int] = {
    "Interruption.Begin": events_pb2.CONTEXT_INTERRUPTION_BEGIN,
    "Interruption.End": events_pb2.CONTEXT_INTERRUPTION_END,
    "Other": events_pb2.CONTEXT_OTHER,
    "Sample.Clock": events_pb2.CONTEXT_SAMPLE_CLOCK,
    "Sample.Periodic": events_pb2.CONTEXT_SAMPLE_PERIODIC,
    "Transaction.Begin": events_pb2.CONTEXT_TRANSACTION_BEGIN,
    "Transaction.End": events_pb2.CONTEXT_TRANSACTION_END,
    "Trigger": events_pb2.CONTEXT_TRIGGER,
}

FORMAT_BY_OCPP: dict[str, int] = {
    "Raw": events_pb2.FORMAT_RAW,
    "SignedData": events_pb2.FORMAT_SIGNED_DATA,
}

LOCATION_BY_OCPP: dict[str, int] = {
    "Cable": events_pb2.LOCATION_CABLE,
    "EV": events_pb2.LOCATION_EV,
    "Inlet": events_pb2.LOCATION_INLET,
    "Outlet": events_pb2.LOCATION_OUTLET,
    "Body": events_pb2.LOCATION_BODY,
}


# --- Proto enum *name* → OCPP wire string (used at read) --------------------
#
# Storage layer (ingestor → ClickHouse) keeps the proto enum *name*
# string, e.g. `"MEASURAND_VOLTAGE"`. The read path translates back to
# the OCPP wire form (`"Voltage"`) for API responses; the inbound
# query-param form (`?measurand=Voltage`) is the same OCPP wire form,
# so the read path also uses these wire strings as the canonical
# "user-facing" representation.
#
# Built once at import by inverting the *_BY_OCPP tables and prefixing
# each value with its proto enum name. Hand-editing the inverse would
# mean two places to remember; deriving it keeps the round-trip honest.


def _proto_name(value: int, enum_descriptor) -> str:  # type: ignore[no-untyped-def]
    return enum_descriptor.values_by_number[value].name


def _invert_with_proto_names(by_ocpp: dict[str, int], enum_descriptor) -> dict[str, str]:  # type: ignore[no-untyped-def]
    return {_proto_name(v, enum_descriptor): k for k, v in by_ocpp.items()}


_DESC = events_pb2.DESCRIPTOR

OCPP_BY_MEASURAND_NAME: dict[str, str] = _invert_with_proto_names(
    MEASURAND_BY_OCPP, _DESC.enum_types_by_name["Measurand"]
)
OCPP_BY_PHASE_NAME: dict[str, str] = _invert_with_proto_names(
    PHASE_BY_OCPP, _DESC.enum_types_by_name["Phase"]
)
OCPP_BY_UNIT_NAME: dict[str, str] = _invert_with_proto_names(
    UNIT_BY_OCPP, _DESC.enum_types_by_name["Unit"]
)
OCPP_BY_CONTEXT_NAME: dict[str, str] = _invert_with_proto_names(
    CONTEXT_BY_OCPP, _DESC.enum_types_by_name["Context"]
)
OCPP_BY_FORMAT_NAME: dict[str, str] = _invert_with_proto_names(
    FORMAT_BY_OCPP, _DESC.enum_types_by_name["Format"]
)
OCPP_BY_LOCATION_NAME: dict[str, str] = _invert_with_proto_names(
    LOCATION_BY_OCPP, _DESC.enum_types_by_name["Location"]
)


def proto_enum_name_for_measurand(ocpp_wire: str | None) -> str | None:
    """OCPP wire string (`"Voltage"`) → proto enum name
    (`"MEASURAND_VOLTAGE"`) for SQL filter use. `None` when the input
    is unknown — caller should treat as "no filter possible," not as
    "match everything"."""
    if ocpp_wire is None:
        return None
    proto_value = MEASURAND_BY_OCPP.get(ocpp_wire)
    if proto_value is None:
        return None
    return _proto_name(proto_value, _DESC.enum_types_by_name["Measurand"])


def proto_enum_name_for_phase(ocpp_wire: str | None) -> str | None:
    """OCPP wire string (`"L1"`) → proto enum name (`"PHASE_L1"`).
    See `proto_enum_name_for_measurand` for `None` semantics."""
    if ocpp_wire is None:
        return None
    proto_value = PHASE_BY_OCPP.get(ocpp_wire)
    if proto_value is None:
        return None
    return _proto_name(proto_value, _DESC.enum_types_by_name["Phase"])


def ocpp_string_for(field: str, proto_name: str | None) -> str | None:
    """Stored proto enum name → OCPP wire string for one of the six
    sampled-value enum dimensions.

    `field` is `"measurand"` / `"phase"` / `"unit"` / `"context"` /
    `"format"` / `"location"`.

    Empty / `None` / unrecognised proto name (including `*_UNSPECIFIED`
    and any vendor-extension string the ingestor stored verbatim from
    the wire) → `None`. We deliberately do NOT surface
    `\"MEASURAND_UNSPECIFIED\"` to API clients — that's an internal
    sentinel."""
    if not proto_name:
        return None
    table = _READ_TABLES.get(field)
    if table is None:
        raise KeyError(f"unknown sampled-value enum field: {field!r}")
    return table.get(proto_name)


_READ_TABLES: dict[str, dict[str, str]] = {
    "measurand": OCPP_BY_MEASURAND_NAME,
    "phase": OCPP_BY_PHASE_NAME,
    "unit": OCPP_BY_UNIT_NAME,
    "context": OCPP_BY_CONTEXT_NAME,
    "format": OCPP_BY_FORMAT_NAME,
    "location": OCPP_BY_LOCATION_NAME,
}
