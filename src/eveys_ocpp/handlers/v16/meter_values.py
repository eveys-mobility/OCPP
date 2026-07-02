"""MeterValues handler.

OCPP 1.6 reference: MeterValues.req / MeterValuesResponse.
Spec section: TODO (task C-1).

Charger-initiated periodic energy/power/current/voltage samples.
Highest-volume action by far at fleet scale (~10^5 to 10^6 rows/min at
10k chargers).

Persistence path (per AGENTS rule 4 + ADR-0004):
1. Build a `CpMeter` payload + `EventEnvelope`.
2. Publish to Kafka topic `cp.meter` keyed by `cp_id` (single
   ordered partition per charger, per AGENTS rule).
3. ClickHouse consumes the topic via the Kafka table engine (E2-14).

**Never written to Postgres.** Postgres holds only transactional
state. MeterValues at production volume would bloat any relational
store; the time-series store is ClickHouse.

The OCPP response is a fixed empty body (`MeterValuesResponse`).

Behavior:

* Validate input shape via the `mobilityhouse/ocpp` library's JSON
  schema (already enforced by the time we get here).
* If `event_producer` is None (unit tests, Kafka-less local dev),
  log + return — no error. Same pattern as the registry.
* Sanity-check meter values per E5-4 (`_meter_sanity.check_sample`).
  Each sample is checked against a measurand-aware physical range
  (energy, power, voltage, current, frequency, temperature, SoC,
  power factor, RPM); failures drop just that sample, log with
  measurand+reason, and bump the
  `eveys_ocpp_meter_value_quarantined_total{measurand,reason}`
  counter. Unknown measurands accept by default (vendor extensions).

Deviations from the OCA spec to verify before W2 / OCTT
(see `docs/08-ocpp-conformance.md`):
- Sanity ranges are a project defensive layer, not OCA-mandated. See
  `_meter_sanity.py` for the per-measurand bounds and reasoning.
- We accept and forward `transaction_id` if present, but don't yet
  cross-check that the transaction is open in Postgres.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ocpp.exceptions import SecurityError
from ocpp.v16 import call_result

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp._ocpp_enums import (
    CONTEXT_BY_OCPP,
    FORMAT_BY_OCPP,
    LOCATION_BY_OCPP,
    MEASURAND_BY_OCPP,
    PHASE_BY_OCPP,
    UNIT_BY_OCPP,
)
from eveys_ocpp.handlers.v16 import _meter_sanity
from eveys_ocpp.metrics import record_handler_error, time_handler
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, get_logger

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


def _build_envelope(*, cp_id: str, payload: events_pb2.CpMeter) -> bytes:
    envelope = events_pb2.EventEnvelope(
        event_id=str(uuid.uuid4()),
        occurred_at=datetime.now(UTC).isoformat(),
        cp_id=cp_id,
        schema_version="v1",
        cp_meter=payload,
    )
    return envelope.SerializeToString()


def _to_proto_sampled_value(raw: dict[str, Any]) -> events_pb2.SampledValue:
    """Translate one OCPP `sampledValue` into the proto type.

    OCPP 1.6 sampledValue uses camelCase JSON keys; the
    mobilityhouse/ocpp library converts them to snake_case Python
    kwargs by the time they reach us, but the inner dicts in
    `meter_value[*].sampled_value` may stay as raw dicts (depending
    on the library's marshaling). Tolerate both.

    Per OCPP 1.6 §6.21.4 an absent `measurand` defaults to
    `Energy.Active.Import.Register` — applied here so consumers
    filtering by that measurand don't miss the bare-energy samples
    chargers commonly emit.

    Unknown enum values land in `*_UNSPECIFIED` (vendor extensions);
    the raw `value` is still captured.
    """

    def _g(*names: str) -> str:
        for name in names:
            if name in raw and raw[name] is not None:
                return str(raw[name])
        return ""

    measurand_str = _g("measurand")
    if not measurand_str:
        # OCPP 1.6 §6.21.4 default.
        measurand_str = "Energy.Active.Import.Register"

    return events_pb2.SampledValue(
        value=_g("value"),
        context=CONTEXT_BY_OCPP.get(_g("context"), events_pb2.CONTEXT_UNSPECIFIED),
        format=FORMAT_BY_OCPP.get(_g("format"), events_pb2.FORMAT_UNSPECIFIED),
        measurand=MEASURAND_BY_OCPP.get(measurand_str, events_pb2.MEASURAND_UNSPECIFIED),
        phase=PHASE_BY_OCPP.get(_g("phase"), events_pb2.PHASE_UNSPECIFIED),
        location=LOCATION_BY_OCPP.get(_g("location"), events_pb2.LOCATION_UNSPECIFIED),
        unit=UNIT_BY_OCPP.get(_g("unit"), events_pb2.UNIT_UNSPECIFIED),
    )


async def handle(
    cp: EveysChargePoint,
    *,
    connector_id: int,
    meter_value: list[dict[str, Any]],
    transaction_id: int | None = None,
    **_: object,
) -> call_result.MeterValues:
    """Charger-initiated periodic samples. Forward to Kafka."""
    bind_contextvars(cp_id=cp.id, action="MeterValues", direction="rx")

    # Pending-authorization gate — only BootNotification is honoured
    # while the operator hasn't approved this cp_id.
    if cp.is_pending:
        raise SecurityError(
            details={"reason": "authorization pending; operator has not authorized this cp_id"}
        )

    metrics_registry.METER_VALUES_TOTAL.inc()
    with time_handler("MeterValues"):
        try:
            return await _meter_values_inner(
                cp,
                connector_id=connector_id,
                meter_value=meter_value,
                transaction_id=transaction_id,
            )
        except Exception as exc:
            record_handler_error("MeterValues", exc)
            raise


async def _meter_values_inner(
    cp: EveysChargePoint,
    *,
    connector_id: int,
    meter_value: list[dict[str, Any]],
    transaction_id: int | None,
) -> call_result.MeterValues:
    sampled_values: list[events_pb2.SampledValue] = []
    charger_reported_at: str = ""
    quarantined = 0
    for entry in meter_value:
        if not isinstance(entry, dict):
            continue
        # The first non-empty timestamp wins; OCPP allows multiple
        # `meterValue` entries with different timestamps. Phase-2 we
        # forward one CpMeter per call — if needed we'll split into
        # one envelope per timestamp in a later refinement.
        if not charger_reported_at:
            charger_reported_at = str(entry.get("timestamp") or "")
        for raw in entry.get("sampled_value") or []:
            if not isinstance(raw, dict):
                continue
            verdict = _meter_sanity.check_sample(raw)
            if not verdict.accepted:
                quarantined += 1
                metrics_registry.METER_VALUE_QUARANTINED_TOTAL.labels(
                    measurand=verdict.measurand,
                    reason=verdict.reason,
                ).inc()
                log.warning(
                    "meter_values.sample_quarantined",
                    sample_value=raw.get("value"),
                    unit=raw.get("unit"),
                    measurand=verdict.measurand,
                    reason=verdict.reason,
                )
                continue
            metrics_registry.METER_VALUE_SAMPLES_TOTAL.labels(measurand=verdict.measurand).inc()
            sampled_values.append(_to_proto_sampled_value(raw))

    log.info(
        "meter_values",
        connector_id=connector_id,
        transaction_id=transaction_id,
        sample_count=len(sampled_values),
        quarantined=quarantined,
    )

    if cp.event_producer is not None and sampled_values:
        payload = events_pb2.CpMeter(
            connector_id=connector_id,
            transaction_id=transaction_id or 0,
            sampled_values=sampled_values,
            charger_reported_at=charger_reported_at,
        )
        envelope_bytes = _build_envelope(cp_id=cp.id, payload=payload)
        # Best-effort publish — a Kafka broker drop must not crash the
        # OCPP handler. Without this guard, a flaky broker would DoS the
        # gateway: the OCPP library treats handler exceptions as crashes,
        # the charger gets no MeterValuesResponse, and chargers retry
        # aggressively. Same pattern the other E2-8 emitters use.
        try:
            await cp.event_producer.publish(
                topic=cp.settings.kafka_topic_cp_meter,
                key=cp.id,
                value=envelope_bytes,
            )
        except Exception as exc:
            log.warning("meter_values.publish_failed", error=str(exc))

    return call_result.MeterValues()
