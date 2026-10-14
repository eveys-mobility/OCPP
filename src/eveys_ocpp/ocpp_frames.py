"""Publish every OCPP frame on the WS to the `cp.ocpp_frames` Kafka topic.

The gateway has a settled pattern for digest events (cp.boot, cp.status,
cp.meter, tx.started, …): each handler builds a typed proto payload,
wraps it in `EventEnvelope`, and calls `event_producer.publish()`. This
module exposes the same shape for raw OCPP frames, called from the
two WebSocket chokepoints in `connection.py`:

  - inbound  (CP → gateway): right after `unpack()` succeeds, before
    the library routes to a handler. The full raw JSON string is the
    frame's exact wire bytes.
  - outbound (gateway → CP): after the socket send succeeds. The
    library's exact serialized JSON carries the actual correlation ID.
    CALLRESULT/CALLERROR are included; send success does not prove receipt.

Best-effort: every publish is wrapped in try/except. Kafka failures
log + increment a Prometheus counter and return. Publishing is awaited
within a short hard deadline; timeout can leave an audit gap or an
ambiguous delivery, while external cancellation still propagates.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

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
        timeout_seconds=settings.ocpp_frame_publish_timeout_seconds,
    )


async def publish_outbound(
    *,
    producer: EventProducer | None,
    settings: Settings,
    cp_id: str,
    raw_msg: str,
) -> None:
    """Publish the exact serialized frame after a successful socket send."""
    if producer is None or not settings.kafka_publish_ocpp_frames:
        return
    try:
        frame = json.loads(raw_msg)
        if not isinstance(frame, list) or len(frame) < 3 or frame[0] not in {2, 3, 4}:
            raise ValueError("invalid outbound frame shape")
        message_type = int(frame[0])
        message_id = str(frame[1])
        action = str(frame[2]) if message_type == _MT_CALL else ""
    except Exception as exc:
        log.warning(
            "ocpp_frame.outbound_parse_failed",
            cp_id=cp_id,
            error_class=type(exc).__name__,
        )
        metrics_registry.OCPP_FRAMES_PUBLISH_FAILURES_TOTAL.labels(direction="outbound").inc()
        return
    await _publish(
        producer=producer,
        topic=settings.kafka_topic_cp_ocpp_frames,
        cp_id=cp_id,
        direction="outbound",
        raw_payload=raw_msg,
        message_id=message_id,
        action=action,
        message_type=message_type,
        timeout_seconds=settings.ocpp_frame_publish_timeout_seconds,
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
    timeout_seconds: float,
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
        async with asyncio.timeout(timeout_seconds):
            await producer.publish(topic=topic, key=cp_id, value=envelope.SerializeToString())
    except Exception as exc:
        # Best-effort: log + count, never propagate. A broker outage
        # MUST NOT take the OCPP WS path down with it.
        log.warning(
            "ocpp_frame.publish_failed",
            cp_id=cp_id,
            direction=direction,
            action=action,
            error_class=type(exc).__name__,
        )
        metrics_registry.OCPP_FRAMES_PUBLISH_FAILURES_TOTAL.labels(direction=direction).inc()
