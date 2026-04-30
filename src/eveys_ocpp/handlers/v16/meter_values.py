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
* Sanity-check meter values per AGENTS rule 6: a single sample with
  an absolute energy reading > 100 MWh is suspicious. Log + drop the
  sample. Don't crash; the charger isn't waiting on validation.

Deviations from the OCA spec to verify before W2 / OCTT
(see `docs/08-ocpp-conformance.md`):
- Sanity-range bounds (100 MWh single-sample) are a project policy,
  not OCA-mandated. Real bounds will be tuned in Phase 5 (E5-4).
- We accept and forward `transaction_id` if present, but don't yet
  cross-check that the transaction is open in Postgres.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ocpp.v16 import call_result

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.observability import bind_contextvars, get_logger

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)

# A single sample claiming more than this many Wh is almost certainly
# a bug or attack. Quarantine — do not publish, do not bill. AGENTS
# rule 6. 100 MWh = 100_000 kWh; the largest passenger EV battery
# today is ~150 kWh, so this gives ~600x margin.
_SANITY_MAX_WH = 100_000_000


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
    """

    def _g(*names: str) -> str:
        for name in names:
            if name in raw and raw[name] is not None:
                return str(raw[name])
        return ""

    return events_pb2.SampledValue(
        value=_g("value"),
        # OCPP uses string enums; we drop them through as opaque
        # strings — the proto enum mapping happens downstream in
        # ClickHouse. Forward-compat with vendor extensions.
        # Empty enum field encodes as the *_UNSPECIFIED 0 value.
    )


def _is_value_in_sanity_range(raw: dict[str, Any]) -> bool:
    """Return False if the sample claims an absurd absolute Wh."""
    unit = str(raw.get("unit") or "Wh").lower()
    try:
        magnitude = float(raw.get("value") or 0)
    except (TypeError, ValueError):
        return False
    if unit in {"kwh"}:
        magnitude *= 1_000
    elif unit in {"mwh"}:
        magnitude *= 1_000_000
    return abs(magnitude) <= _SANITY_MAX_WH


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
            if not _is_value_in_sanity_range(raw):
                quarantined += 1
                log.warning(
                    "meter_values.sample_quarantined",
                    sample_value=raw.get("value"),
                    unit=raw.get("unit"),
                )
                continue
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
        # Broker errors (down, leader election, network) must NOT crash
        # the WS — the charger isn't waiting on Kafka, and a flaky broker
        # would otherwise DoS the gateway. Log + drop the sample; the
        # charger gets a clean MeterValuesResponse and keeps streaming.
        # Reconnect/retry tuning lives in E2-7.
        try:
            await cp.event_producer.publish(
                topic=cp.settings.kafka_topic_cp_meter,
                key=cp.id,
                value=envelope_bytes,
            )
        except Exception as exc:
            log.warning("meter_values.publish_failed", error=str(exc))

    return call_result.MeterValues()
