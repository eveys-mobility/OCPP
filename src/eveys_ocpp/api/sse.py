"""Server-Sent Events stream for the per-CP detail page (ADR-0030).

`GET /api/v1/charge-points/{cp_id}/events` opens a long-lived
`text/event-stream` response and pushes one SSE event per relevant
Kafka envelope keyed on this `cp_id`. The endpoint is feature-flagged
behind `EVEYS_OCPP_SSE_ENABLED` (default False); the singleton
`SseBus` (started/stopped on the app lifespan) owns the Kafka
consumer and fan-out.

The endpoint itself is a thin streaming-response wrapper:

- 404 UNKNOWN_CP_ID upfront so a typo doesn't open an infinite stream
  of nothing.
- Subscribe to the bus, then yield SSE-framed lines as messages arrive
  in the per-subscriber queue.
- Heartbeat comment line every `sse_heartbeat_seconds` so intermediate
  proxies don't kill an idle stream.
- Sentinel `None` from the bus → close the stream with an `error`
  event (slow consumer drop, app shutdown).
- Always unsubscribe in a `finally` so client disconnects drain
  cleanly.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from eveys_ocpp.api._errors import ERR_INTERNAL_ERROR, ERR_UNKNOWN_CP_ID, ApiError
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import get_charge_point_pk

if TYPE_CHECKING:
    from eveys_ocpp.sse_bus import SseBus

router = APIRouter(tags=["sse"])


def _format_sse(event: str, data: dict[str, object]) -> bytes:
    """Render one SSE message as bytes.

    Format follows the W3C SSE spec: `event:` line + `data:` line +
    blank line. `data:` is single-line JSON; if a payload ever needs
    multi-line we'd split on `\\n` and prefix each chunk — not needed
    today, the payloads are flat dicts.
    """
    payload = json.dumps(data, separators=(",", ":"), default=str)
    return f"event: {event}\ndata: {payload}\n\n".encode()


@router.get(
    "/charge-points/{cp_id}/events",
    summary="Server-Sent Events stream of per-CP lifecycle events",
    responses={
        200: {
            "content": {"text/event-stream": {}},
            "description": (
                "Open SSE stream. Each event has the shape "
                "`event: <type>\\ndata: <json>\\n\\n`. Event types: "
                "`connected`, `disconnected`, `offline_duration`, "
                "`boot`, `status`, `meter`, `firmware_status_changed`, "
                "`diagnostics_status_changed`, `tx_started`, "
                "`tx_stopped`, `security_event`. The stream sends a "
                "comment-line heartbeat every "
                "`sse_heartbeat_seconds` so intermediate proxies "
                "keep the connection open. A terminal `error` event "
                "signals a slow-consumer drop or a server shutdown; "
                "the client should reconnect."
            ),
        },
        404: {"description": "Unknown cp_id."},
        503: {"description": "SSE not enabled on this gateway pod."},
    },
)
async def stream_charge_point_events(request: Request, cp_id: str) -> StreamingResponse:
    settings = request.app.state.settings
    if not settings.sse_enabled:
        # Feature-flag off: the bus was never started, so refuse.
        # 503 (Service Unavailable) rather than 404 because the
        # endpoint exists in the spec; it's the runtime that opted
        # out.
        raise ApiError(
            status_code=503,
            error_code=ERR_INTERNAL_ERROR,
            message="SSE not enabled on this gateway pod",
        )

    # 404 upfront for typo'd cp_ids — same shape as `/meter-values`.
    async with session_scope(request.app.state.session_factory) as session:
        pk = await get_charge_point_pk(session, cp_id=cp_id)
    if pk is None:
        raise ApiError(
            status_code=404,
            error_code=ERR_UNKNOWN_CP_ID,
            message=f"unknown cp_id: {cp_id}",
        )

    bus: SseBus | None = getattr(request.app.state, "sse_bus", None)
    if bus is None or not bus.running:
        raise ApiError(
            status_code=503,
            error_code=ERR_INTERNAL_ERROR,
            message="SSE bus not running",
        )

    subscription = await bus.subscribe(cp_id)

    async def event_stream() -> AsyncIterator[bytes]:
        """Pump messages from the subscriber's queue into SSE bytes.

        Two concurrent waiters: the next message and the heartbeat
        timer. The first one to fire wins; the loop drains it and
        loops back. Client disconnect cancels the generator → the
        `finally` block detaches the subscription so the bus doesn't
        leak a queue.
        """
        heartbeat_seconds = settings.sse_heartbeat_seconds
        try:
            # First byte primes Starlette's response — without it the
            # response headers don't ship until the first real event.
            yield b": connected\n\n"
            while True:
                try:
                    msg = await asyncio.wait_for(
                        subscription.queue.get(),
                        timeout=heartbeat_seconds,
                    )
                except TimeoutError:
                    # Idle — send a heartbeat comment.
                    yield b": heartbeat\n\n"
                    continue

                if msg is None:
                    # Sentinel: either the bus shut down or we got
                    # dropped for slow-consumer. Tell the client which.
                    reason = "slow_consumer" if subscription.dropped else "server_closed"
                    yield _format_sse("error", {"reason": reason})
                    return

                event = msg.get("event")
                data = msg.get("data")
                if not isinstance(event, str) or not isinstance(data, dict):
                    # Bus invariant violated — drop the message and
                    # keep the stream alive rather than crash.
                    continue
                yield _format_sse(event, data)
        finally:
            await bus.unsubscribe(subscription)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            # Tell intermediates not to buffer (nginx, Envoy with
            # `response_buffer_limit_bytes` set, …) so events ship
            # promptly.
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
