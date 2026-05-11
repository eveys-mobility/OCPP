"""Pydantic models that drive the OpenAPI schema (E3-7 OpenAPI add-on).

Why this module exists, in one paragraph: the gateway's REST routes
return plain dicts, not Pydantic models, because the contract was
hand-written first in `docs/integration/02-gateway-rest-api.md` and
the routes literally `return {…}`. That keeps the route bodies
readable but leaves OpenAPI with `Any` schemas. Annotating each
route with `response_model=` would force runtime validation and
risks behavioural drift on a surface that's already in production.

So instead, every model here is a **schema-only** declaration. Routes
opt in via `responses={200: {"model": …}}` on the decorator — that
populates OpenAPI without changing the runtime serialization path.
The trade-off: schemas can drift from reality if a route's response
shape changes without a corresponding model edit. The
`tests/unit/api/test_openapi.py` snapshot test catches the drift on
CI; reviewers spot the rest.

The models here cover the headline shapes (charge-point, transaction,
reservation, charging profile, meter sample, status event, health,
error envelope, plus the cursor-paginated wrappers and a handful of
command shapes). The long tail of command endpoints (~15 of the 19)
ships with a generic `CommandAcceptedResponse` placeholder — a
follow-up PR can typify each one as the contract solidifies.

Examples (the `json_schema_extra={"example": …}` blobs) are what
operators see in the Swagger UI's "Try it out" — they should be
plausible, copy-pasteable, and never include real PII.
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, Field

# --------------------------------------------------------------------------
# Errors and envelopes
# --------------------------------------------------------------------------


class ErrorEnvelope(BaseModel):
    """Stable error shape per `docs/integration/02-gateway-rest-api.md`.

    All non-2xx responses from `/api/v1/*` carry this body. The 11
    `error_code` values are the closed enum from
    `src/eveys_ocpp/api/_errors.py` (BAD_REQUEST, UNAUTHORIZED,
    FORBIDDEN, UNKNOWN_CP_ID, UNKNOWN_TRANSACTION_ID,
    UNKNOWN_RESERVATION_ID, CHARGER_OFFLINE, CHARGER_TIMEOUT,
    WINDOW_TOO_LARGE, RATE_LIMITED, INTERNAL_ERROR).
    """

    error: str = Field(description="Human-readable description for logs.")
    error_code: str = Field(description="Stable machine code; one of the 11 closed-enum values.")
    request_id: str = Field(description="Correlation id mirroring the X-Request-ID header.")

    model_config = {
        "json_schema_extra": {
            "example": {
                "error": "unknown cp_id: CP_DOES_NOT_EXIST",
                "error_code": "UNKNOWN_CP_ID",
                "request_id": "9b3c5d18-1f7c-4b6a-8e0e-5b9a3c4f2e10",
            }
        }
    }


# --------------------------------------------------------------------------
# Health
# --------------------------------------------------------------------------


class HealthComponents(BaseModel):
    postgres: str = Field(description="`ok` or `unavailable`.")
    redis: str = Field(description="`ok` or `unavailable`.")


class HealthResponse(BaseModel):
    """`GET /api/v1/health`. Liveness + per-component probe."""

    status: str = Field(description="`ok` when every component is `ok`; `degraded` otherwise.")
    version: str = Field(description="Gateway package version string.")
    components: HealthComponents
    request_id: str

    model_config = {
        "json_schema_extra": {
            "example": {
                "status": "ok",
                "version": "0.1.0",
                "components": {"postgres": "ok", "redis": "ok"},
                "request_id": "9b3c5d18-1f7c-4b6a-8e0e-5b9a3c4f2e10",
            }
        }
    }


# --------------------------------------------------------------------------
# Charge points
# --------------------------------------------------------------------------


class ConnectorStatus(BaseModel):
    """One row from the `cp_status` ClickHouse table — the most-recent
    StatusNotification per connector."""

    connector_id: int = Field(
        ge=0, description="0 is the charger itself; 1+ are physical connectors."
    )
    status: str = Field(description="OCPP 1.6 §3.1.1 enum: Available, Preparing, Charging, …")
    error_code: str | None = None
    timestamp: str | None = Field(default=None, description="ISO 8601 of the StatusNotification.")
    info: str | None = None
    vendor_id: str | None = None
    vendor_error_code: str | None = None


class Reservation(BaseModel):
    reservation_id: int
    connector_id: int
    id_tag: str
    expiry_date: str | None = None
    status: str = Field(description="`active` or `cancelled`.")


class ChargingProfile(BaseModel):
    """Mirror of the OCPP 1.6 SetChargingProfile request payload, as
    persisted server-side. The shape is dynamic per the OCPP spec —
    we treat it as opaque-ish for OpenAPI purposes."""

    charging_profile_id: int
    stack_level: int
    charging_profile_purpose: str
    charging_profile_kind: str
    recurrency_kind: str | None = None
    valid_from: str | None = None
    valid_to: str | None = None
    charging_schedule: dict[str, Any]


class ChargePoint(BaseModel):
    """One row of `GET /api/v1/charge-points`. Detail endpoint adds
    `active_reservations` and `active_charging_profiles`."""

    cp_id: str = Field(description="Stable identifier the charger sent in BootNotification.")
    vendor: str | None = None
    model: str | None = None
    firmware_version: str | None = None
    serial_number: str | None = None
    last_status: str | None = Field(
        default=None,
        description=(
            "Last-write-wins across connectors. For multi-connector chargers, prefer "
            "`connectors[]` (the per-connector array) — `last_status` is a single-string "
            "convenience for callers that don't need per-connector resolution."
        ),
    )
    last_status_at: str | None = None
    last_heartbeat_at: str | None = None
    online: bool = Field(description="True iff some pod currently holds the charger's WS.")
    pod_id: str | None = Field(
        default=None,
        description="Which gateway pod owns the WS. None when offline.",
    )
    connectors: list[ConnectorStatus] = Field(default_factory=list)


class ChargePointDetail(ChargePoint):
    active_reservations: list[Reservation] = Field(default_factory=list)
    active_charging_profiles: list[ChargingProfile] = Field(default_factory=list)
    request_id: str

    model_config = {
        "json_schema_extra": {
            "example": {
                "cp_id": "CP_BERLIN_001",
                "vendor": "ACME",
                "model": "X1",
                "firmware_version": "1.2.3",
                "serial_number": "SN-12345",
                "last_status": "Charging",
                "last_status_at": "2026-05-07T12:34:56+00:00",
                "last_heartbeat_at": "2026-05-07T12:35:00+00:00",
                "online": True,
                "pod_id": "ocpp-gw-7f9c4-x2k8q",
                "connectors": [
                    {
                        "connector_id": 1,
                        "status": "Charging",
                        "error_code": "NoError",
                        "timestamp": "2026-05-07T12:34:56+00:00",
                        "info": None,
                        "vendor_id": None,
                        "vendor_error_code": None,
                    }
                ],
                "active_reservations": [],
                "active_charging_profiles": [],
                "request_id": "9b3c5d18-1f7c-4b6a-8e0e-5b9a3c4f2e10",
            }
        }
    }


class PaginationBlock(BaseModel):
    """Offset-pagination metadata returned alongside list responses when
    the caller passes `page` + `page_size`. Cursor-mode callers see
    `next_cursor` instead; the two are mutually exclusive on a single
    response.

    Shape is frozen — every field below is part of the wire contract.
    `total_pages` is 0 when `total == 0`; otherwise it's
    `ceil(total / page_size)`.
    """

    page: int = Field(description="1-indexed current page number.")
    page_size: int = Field(description="Rows per page on this response.")
    total: int = Field(description="Total rows across every page, given the active filters.")
    total_pages: int = Field(description="Number of pages at the current `page_size`.")
    has_next: bool = Field(description="True if a subsequent page exists.")
    has_prev: bool = Field(description="True if a prior page exists.")

    model_config = {
        "json_schema_extra": {
            "example": {
                "page": 2,
                "page_size": 100,
                "total": 4523,
                "total_pages": 46,
                "has_next": True,
                "has_prev": True,
            }
        }
    }


class ChargePointListResponse(BaseModel):
    """`GET /api/v1/charge-points`. Two pagination modes — cursor or offset:

    - **Cursor mode** (default): response carries `next_cursor`.
      `next_cursor=null` means no more pages.
    - **Offset mode**: when the caller passes `page` + `page_size`,
      response carries `pagination` instead.

    The two shapes are mutually exclusive on a single response.
    """

    charge_points: list[ChargePoint]
    next_cursor: str | None = None
    pagination: PaginationBlock | None = None
    request_id: str

    model_config = {
        "json_schema_extra": {
            "example": {
                "charge_points": [
                    {
                        "cp_id": "CP_BERLIN_001",
                        "vendor": "ACME",
                        "model": "X1",
                        "firmware_version": "1.2.3",
                        "serial_number": "SN-12345",
                        "last_status": "Available",
                        "last_status_at": "2026-05-07T12:30:00+00:00",
                        "last_heartbeat_at": "2026-05-07T12:35:00+00:00",
                        "online": True,
                        "pod_id": "ocpp-gw-7f9c4-x2k8q",
                        "connectors": [],
                    }
                ],
                "next_cursor": "eyJpZCI6IDQyfQ==",
                "request_id": "9b3c5d18-1f7c-4b6a-8e0e-5b9a3c4f2e10",
            }
        }
    }


# --------------------------------------------------------------------------
# Transactions
# --------------------------------------------------------------------------


class SocSummary(BaseModel):
    """State-of-charge summary for one transaction.

    `start_pct` is the earliest SoC sample inside the transaction's
    window; `last_pct` is the most recent. For a stopped transaction,
    `last_pct` is effectively the SoC at stop. Any field is `null`
    when the charger never reported SoC."""

    start_pct: float | None = None
    last_pct: float | None = None
    last_at: str | None = Field(
        default=None,
        description="ISO 8601 timestamp of the most recent SoC sample.",
    )


class PhaseSnapshot(BaseModel):
    """Most-recent per-phase electrical snapshot for one transaction.

    `argMax(value, occurred_at)` per measurand on the named phase
    (`L1`, `L2`, `L3`). Each field is `null` when the charger never
    reported that measurand on that phase — single-phase chargers
    populate only one phase, DC chargers may populate none."""

    voltage_v: float | None = None
    current_a: float | None = None
    power_w: float | None = None
    last_at: str | None = Field(
        default=None,
        description="ISO 8601 timestamp of the most recent sample on this phase.",
    )


class TransactionTelemetry(BaseModel):
    """ClickHouse-backed telemetry summary attached to the transaction
    detail endpoint.

    Bounded shape — SoC start/last and one snapshot per phase — so the
    response stays small regardless of session length. Callers wanting
    the full curve should hit
    `GET /api/v1/charge-points/{cp_id}/meter-values` with a window."""

    soc: SocSummary
    phases: dict[str, PhaseSnapshot] = Field(
        description=(
            "Keyed by OCPP 1.6 phase name (`L1`, `L2`, `L3`). Phases the "
            "charger never reported are absent from the map."
        ),
    )


class Transaction(BaseModel):
    transaction_id: int = Field(description="OCPP 1.6 transaction id assigned by the gateway.")
    cp_id: str
    connector_id: int
    id_tag: str = Field(description="RFID / token used to authorize the session.")
    meter_start_wh: int
    started_at: str = Field(description="ISO 8601 timestamp of the StartTransaction.")
    meter_stop_wh: int | None = None
    stopped_at: str | None = None
    stop_reason: str | None = None
    open: bool = Field(description="True until StopTransaction lands.")


class TransactionDetail(Transaction):
    telemetry: TransactionTelemetry | None = Field(
        default=None,
        description=(
            "ClickHouse-backed snapshot — SoC start/last and per-phase "
            "voltage/current/power. `null` when the gateway has no "
            "ClickHouse read client configured."
        ),
    )
    request_id: str

    model_config = {
        "json_schema_extra": {
            "example": {
                "transaction_id": 12345,
                "cp_id": "CP_BERLIN_001",
                "connector_id": 1,
                "id_tag": "RFID_VALID_001",
                "meter_start_wh": 1000000,
                "started_at": "2026-05-07T12:00:00+00:00",
                "meter_stop_wh": 1012345,
                "stopped_at": "2026-05-07T12:30:00+00:00",
                "stop_reason": "Local",
                "open": False,
                "telemetry": {
                    "soc": {
                        "start_pct": 38.0,
                        "last_pct": 81.0,
                        "last_at": "2026-05-07T12:29:58+00:00",
                    },
                    "phases": {
                        "L1": {
                            "voltage_v": 231.4,
                            "current_a": 14.8,
                            "power_w": 3417.3,
                            "last_at": "2026-05-07T12:29:58+00:00",
                        },
                        "L2": {
                            "voltage_v": 230.9,
                            "current_a": 14.6,
                            "power_w": 3370.5,
                            "last_at": "2026-05-07T12:29:58+00:00",
                        },
                        "L3": {
                            "voltage_v": 231.1,
                            "current_a": 14.7,
                            "power_w": 3393.0,
                            "last_at": "2026-05-07T12:29:58+00:00",
                        },
                    },
                },
                "request_id": "9b3c5d18-1f7c-4b6a-8e0e-5b9a3c4f2e10",
            }
        }
    }


class TransactionListResponse(BaseModel):
    """`GET /api/v1/charge-points/{cp_id}/transactions` and
    `GET /api/v1/transactions`. Cursor- or page-paginated; see
    `PaginationBlock` for the offset-mode shape."""

    transactions: list[Transaction]
    next_cursor: str | None = None
    pagination: PaginationBlock | None = None
    request_id: str

    model_config = {
        "json_schema_extra": {
            "example": {
                "transactions": [
                    {
                        "transaction_id": 12345,
                        "cp_id": "CP_BERLIN_001",
                        "connector_id": 1,
                        "id_tag": "RFID_VALID_001",
                        "meter_start_wh": 1000000,
                        "started_at": "2026-05-07T12:00:00+00:00",
                        "meter_stop_wh": 1012345,
                        "stopped_at": "2026-05-07T12:30:00+00:00",
                        "stop_reason": "Local",
                        "open": False,
                    }
                ],
                "next_cursor": None,
                "request_id": "9b3c5d18-1f7c-4b6a-8e0e-5b9a3c4f2e10",
            }
        }
    }


# --------------------------------------------------------------------------
# Reservations / charging profiles list endpoints
# --------------------------------------------------------------------------


class ReservationListResponse(BaseModel):
    reservations: list[Reservation]
    request_id: str


class ChargingProfileListResponse(BaseModel):
    charging_profiles: list[ChargingProfile]
    request_id: str


# --------------------------------------------------------------------------
# Time-series (ClickHouse-backed)
# --------------------------------------------------------------------------


class MeterValueSample(BaseModel):
    timestamp: str
    connector_id: int
    transaction_id: int | None = None
    measurand: str = Field(description="OCPP 1.6 measurand: Energy.Active.Import.Register, …")
    value: float
    unit: str = Field(description="Wh, W, A, V, °C, …")
    context: str | None = None
    location: str | None = None
    phase: str | None = None


class MeterValuesResponse(BaseModel):
    """`GET /api/v1/charge-points/{cp_id}/meter-values?from=…&to=…`."""

    cp_id: str
    samples: list[MeterValueSample]
    request_id: str


class StatusEvent(BaseModel):
    timestamp: str
    connector_id: int
    status: str
    error_code: str | None = None
    info: str | None = None
    vendor_id: str | None = None
    vendor_error_code: str | None = None


class StatusHistoryResponse(BaseModel):
    """`GET /api/v1/charge-points/{cp_id}/status-history?from=…&to=…`."""

    cp_id: str
    events: list[StatusEvent]
    request_id: str


# --------------------------------------------------------------------------
# Command surface (E3-8) — typed shapes for the most-used 3 of 19
# --------------------------------------------------------------------------


class RemoteStartRequest(BaseModel):
    id_tag: str = Field(
        description="The RFID / token the charger should associate with the session."
    )
    connector_id: int | None = Field(
        default=None,
        description="Optional. Charger picks one if omitted.",
    )
    charging_profile: dict[str, Any] | None = None

    model_config = {
        "json_schema_extra": {"example": {"id_tag": "RFID_VALID_001", "connector_id": 1}}
    }


class RemoteStopRequest(BaseModel):
    transaction_id: int

    model_config = {"json_schema_extra": {"example": {"transaction_id": 12345}}}


class ResetRequest(BaseModel):
    type: Annotated[str, Field(pattern="^(Soft|Hard)$")] = Field(
        description="Soft = restart software; Hard = full power cycle."
    )

    model_config = {"json_schema_extra": {"example": {"type": "Soft"}}}


class CommandAcceptedResponse(BaseModel):
    """Generic shape for the typed OCPP command responses. Specific
    commands extend this; the generic form is what FastAPI advertises
    for routes whose typed shape is a follow-up task."""

    status: str = Field(
        description=(
            "OCPP-side response (`Accepted` / `Rejected` / `Unknown` for most commands; "
            "`Scheduled` for some). The exact enum is per OCPP 1.6."
        )
    )
    request_id: str

    model_config = {
        "json_schema_extra": {
            "example": {"status": "Accepted", "request_id": "9b3c5d18-1f7c-4b6a-8e0e-5b9a3c4f2e10"}
        }
    }


__all__ = [
    "ChargePoint",
    "ChargePointDetail",
    "ChargePointListResponse",
    "ChargingProfile",
    "ChargingProfileListResponse",
    "CommandAcceptedResponse",
    "ConnectorStatus",
    "ErrorEnvelope",
    "HealthComponents",
    "HealthResponse",
    "MeterValueSample",
    "MeterValuesResponse",
    "PaginationBlock",
    "PhaseSnapshot",
    "RemoteStartRequest",
    "RemoteStopRequest",
    "Reservation",
    "ReservationListResponse",
    "ResetRequest",
    "SocSummary",
    "StatusEvent",
    "StatusHistoryResponse",
    "Transaction",
    "TransactionDetail",
    "TransactionListResponse",
    "TransactionTelemetry",
]
