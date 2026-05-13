"""Publish every OCPP frame on the WS to the `cp.ocpp_frames` Kafka topic.

The gateway has a settled pattern for digest events (cp.boot, cp.status,
cp.meter, tx.started, …): each handler builds a typed proto payload,
wraps it in `EventEnvelope`, and calls `event_producer.publish()`. This
module exposes the same shape for raw OCPP frames, called from the
two WebSocket chokepoints in `connection.py`:

  - inbound  (CP → gateway): right after `unpack()` succeeds, before
    the library routes to a handler. The full raw JSON string is the
    frame's exact wire bytes.
  - outbound (gateway → CP): right before the library writes to the
    socket. We reconstruct the wire JSON from `(message_type_id,
    unique_id, action, payload)` so the audit log holds what the
    charger actually saw.

Best-effort: every publish is wrapped in try/except. Kafka failures
log + increment a Prometheus counter and return; the WS path never
blocks on Kafka.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    from eveys_ocpp.events import EventProducer
    from eveys_ocpp.settings import Settings

log = get_logger(__name__)

# OCPP MessageTypeId values, mirrored here so we don't have to import
# the library's enum just to label the proto field.
_MT_CALL = 2
_MT_CALLRESULT = 3
_MT_CALLERROR = 4


async def publish_inbound(
    *,
    producer: EventProducer | None,
    settings: Settings,
    cp_id: str,
    raw_msg: str,
    message_type_id: int,
    action: str,
    message_id: str | None,
) -> None:
    """Publish a frame received from the charger.

    `raw_msg` is the string the WS read verbatim. We don't re-encode;
    the audit log holds exactly what the wire delivered.
    """
    if producer is None or not settings.kafka_publish_ocpp_frames:
        return
    action_name = action if message_type_id == _MT_CALL else ""
    await _publish(
        producer=producer,
        topic=settings.kafka_topic_cp_ocpp_frames,
        cp_id=cp_id,
        direction="inbound",
        raw_payload=raw_msg,
        message_id=message_id or "",
        action=action_name,
        message_type=message_type_id,
    )


async def publish_outbound(
    *,
    producer: EventProducer | None,
    settings: Settings,
    cp_id: str,
    payload: Any,
    unique_id: str | None,
) -> None:
    """Publish a frame the gateway is about to send.

    The library serialises the OCPP message just before the WS write
    using `[message_type_id, unique_id, action, payload_dict]`. We
    rebuild the same shape here so the audit log carries what the
    charger sees. Library version mismatches that change the wire
    encoding would be caught by the integration tests.

    Outbound `call()` always sends a CALL — CALLRESULT / CALLERROR
    are produced by the library's own response path and don't pass
    through this hook today.
    """
    if producer is None or not settings.kafka_publish_ocpp_frames:
        return
    action = type(payload).__name__
    try:
        payload_dict = _payload_to_dict(payload)
    except Exception as exc:  # pragma: no cover - guard against future payload shapes
        log.warning(
            "ocpp_frame.outbound_serialise_failed",
            cp_id=cp_id,
            action=action,
            error=str(exc),
        )
        metrics_registry.OCPP_FRAMES_PUBLISH_FAILURES_TOTAL.labels(direction="outbound").inc()
        return
    raw_payload = json.dumps(
        [_MT_CALL, unique_id or "", action, payload_dict],
        separators=(",", ":"),
    )
    await _publish(
        producer=producer,
        topic=settings.kafka_topic_cp_ocpp_frames,
        cp_id=cp_id,
        direction="outbound",
        raw_payload=raw_payload,
        message_id=unique_id or "",
        action=action,
        message_type=_MT_CALL,
    )


async def _publish(
    *,
    producer: EventProducer,
    topic: str,
    cp_id: str,
    direction: str,
    raw_payload: str,
    message_id: str,
    action: str,
    message_type: int,
) -> None:
    envelope = events_pb2.EventEnvelope(
        event_id=str(uuid.uuid4()),
        occurred_at=datetime.now(UTC).isoformat(),
        cp_id=cp_id,
        schema_version="v1",
        cp_ocpp_frame=events_pb2.CpOcppFrame(
            direction=direction,
            raw_payload=raw_payload,
            message_id=message_id,
            action=action,
            message_type=message_type,
            ocpp_version="ocpp1.6",
        ),
    )
    try:
        await producer.publish(
            topic=topic,
            key=cp_id,
            value=envelope.SerializeToString(),
        )
    except Exception as exc:
        # Best-effort: log + count, never propagate. A broker outage
        # MUST NOT take the OCPP WS path down with it.
        log.warning(
            "ocpp_frame.publish_failed",
            cp_id=cp_id,
            direction=direction,
            action=action,
            error=str(exc),
        )
        metrics_registry.OCPP_FRAMES_PUBLISH_FAILURES_TOTAL.labels(direction=direction).inc()


def _payload_to_dict(payload: Any) -> dict[str, Any]:
    """Convert a library payload object into the dict shape the WS sends.

    The mobilityhouse/ocpp library uses `@dataclass` for v1.6 message
    classes; `asdict()` produces the wire-equivalent dict. Anything
    that isn't a dataclass falls back to `vars()` which works for the
    library's other shapes we've seen, and ultimately to `repr` so we
    never raise from the audit path.
    """
    if is_dataclass(payload) and not isinstance(payload, type):
        return asdict(payload)
    if hasattr(payload, "__dict__"):
        return {k: v for k, v in vars(payload).items() if not k.startswith("_")}
    return {"_repr": repr(payload)}
