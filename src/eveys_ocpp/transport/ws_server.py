"""WebSocket server.

Listens on `EVEYS_OCPP_WS_HOST:EVEYS_OCPP_WS_PORT`. The path component is
the charger ID: `ws://host:port/<cp_id>`. Subprotocol must be `ocpp1.6`.

Auth, per-IP rate limiting, and TLS termination live at the edge (Envoy)
in production. Locally we accept everything.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from websockets import Subprotocol
from websockets.asyncio.server import ServerConnection, serve

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.connection import EveysChargePoint
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, clear_contextvars, get_logger

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.connections import ConnectionMap
    from eveys_ocpp.events import EventProducer
    from eveys_ocpp.idempotency import IdempotencyCache
    from eveys_ocpp.platform import AuthorizeCache, BackendHTTPClient
    from eveys_ocpp.registry import Registry
    from eveys_ocpp.settings import Settings
    from eveys_ocpp.transport._rate_limiter import RateLimiter

log = get_logger(__name__)

OCPP_SUBPROTOCOL = Subprotocol("ocpp1.6")


async def _publish_lifecycle_event(
    *,
    event_producer: EventProducer | None,
    cp_id: str,
    topic: str,
    payload_field: str,
    payload: object,
) -> None:
    """Publish one lifecycle envelope on the WS connect / disconnect path.

    Best-effort: a Kafka outage MUST NOT break the connection lifecycle
    (otherwise a flaky broker would make every disconnect raise out of
    the finally-block, masking the real disconnect reason). Same shape
    of guard the OCPP handlers (BootNotification, MeterValues, etc.)
    use around their own publishes.

    `payload_field` is the EventEnvelope oneof field name to set; pass
    the proto message in `payload`.
    """
    if event_producer is None:
        return
    try:
        envelope = events_pb2.EventEnvelope(
            event_id=str(uuid.uuid4()),
            occurred_at=datetime.now(UTC).isoformat(),
            cp_id=cp_id,
            schema_version="v1",
        )
        getattr(envelope, payload_field).CopyFrom(payload)
        await event_producer.publish(
            topic=topic,
            key=cp_id,
            value=envelope.SerializeToString(),
        )
    except Exception as exc:
        log.warning(
            "ws.lifecycle_publish_failed",
            cp_id=cp_id,
            topic=topic,
            error=str(exc),
        )


async def _publish_offline_duration(
    *,
    event_producer: EventProducer | None,
    cp_id: str,
    topic: str,
    marker: dict[str, str],
    came_online_at: datetime,
) -> None:
    """Publish the cp.offline_duration envelope for one outage.

    `marker` is the read-and-delete payload from the registry —
    `went_offline_at` (ISO-8601 UTC), `pod_id`, `reason`. A marker
    written before this field was set carries an empty reason; we
    forward that through unchanged.

    Same best-effort shape as `_publish_lifecycle_event` — a Kafka
    outage logs and continues; a malformed marker (stale schema,
    typo'd ISO timestamp) logs and continues. Either way the WS
    upgrade still completes.
    """
    went_offline_raw = marker.get("went_offline_at", "")
    if not went_offline_raw:
        log.warning("ws.offline_duration_skipped_no_timestamp", cp_id=cp_id)
        return
    try:
        went_offline_dt = datetime.fromisoformat(went_offline_raw)
    except ValueError as exc:
        log.warning(
            "ws.offline_duration_bad_timestamp",
            cp_id=cp_id,
            value=went_offline_raw,
            error=str(exc),
        )
        return
    # Both timestamps are server-receive (UTC). Negative gaps can only
    # happen if the marker's pod ran with a skewed clock vs ours;
    # publish anyway, downstream can filter on offline_seconds >= 0.
    offline_seconds = int((came_online_at - went_offline_dt).total_seconds())
    await _publish_lifecycle_event(
        event_producer=event_producer,
        cp_id=cp_id,
        topic=topic,
        payload_field="cp_offline_duration",
        payload=events_pb2.CpOfflineDuration(
            went_offline_at=went_offline_raw,
            came_online_at=came_online_at.isoformat(),
            offline_seconds=offline_seconds,
            prior_pod_id=marker.get("pod_id", ""),
            prior_reason=marker.get("reason", ""),
        ),
    )


async def _on_connect(
    connection: ServerConnection,
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    registry: Registry | None,
    connections: ConnectionMap | None,
    event_producer: EventProducer | None = None,
    idempotency: IdempotencyCache | None = None,
    backend_client: BackendHTTPClient | None = None,
    authorize_cache: AuthorizeCache | None = None,
    rate_limiter: RateLimiter | None = None,
) -> None:
    """Per-connection coroutine. Lives for the duration of the WS."""
    if connection.subprotocol != OCPP_SUBPROTOCOL:
        log.warning("ws.subprotocol_mismatch", got=connection.subprotocol)
        metrics_registry.WS_HANDSHAKE_FAILURES_TOTAL.labels(reason="subprotocol").inc()
        await connection.close(1002, f"unsupported subprotocol; want {OCPP_SUBPROTOCOL}")
        return

    if connection.request is None:  # defensive — should never happen post-handshake
        metrics_registry.WS_HANDSHAKE_FAILURES_TOTAL.labels(reason="no_request").inc()
        await connection.close(1008, "no request handshake")
        return
    cp_id = connection.request.path.strip("/")
    if not cp_id:
        metrics_registry.WS_HANDSHAKE_FAILURES_TOTAL.labels(reason="empty_cp_id").inc()
        await connection.close(1008, "cp_id missing in URL path")
        return

    bind_contextvars(cp_id=cp_id)
    log.info("ws.connected")
    metrics_registry.WS_CONNECTS_TOTAL.inc()
    metrics_registry.WS_CONNECTIONS_ACTIVE.inc()

    # Offline-duration window closes on the connect side. Read-and-
    # delete the marker BEFORE mark_online: a marker left by a prior
    # disconnect is, by construction, the matching opening side of
    # this window. Do it before the cp.connected publish so the
    # offline-duration event arrives first in stream order. Best-
    # effort — a Redis hiccup here just means we miss one duration.
    came_online_at_dt = datetime.now(UTC)
    if registry is not None:
        try:
            marker = await registry.pop_offline_marker(cp_id)
        except Exception as exc:
            log.warning("ws.offline_marker_read_failed", cp_id=cp_id, error=str(exc))
            marker = None
        if marker is not None:
            await _publish_offline_duration(
                event_producer=event_producer,
                cp_id=cp_id,
                topic=settings.kafka_topic_cp_offline_duration,
                marker=marker,
                came_online_at=came_online_at_dt,
            )

    if registry is not None:
        await registry.mark_online(cp_id)

    # cp.online lifecycle event. Published only after the registry
    # mark succeeds so a downstream consumer never sees online events
    # for chargers that never reached the registry. Best-effort —
    # broker drop logs a warning, doesn't break the WS path.
    await _publish_lifecycle_event(
        event_producer=event_producer,
        cp_id=cp_id,
        topic=settings.kafka_topic_cp_connected,
        payload_field="cp_connected",
        payload=events_pb2.CpConnected(
            subprotocol=str(connection.subprotocol or ""),
            pod_id=settings.pod_id,
        ),
    )

    cp = EveysChargePoint(
        cp_id,
        connection,
        session_factory=session_factory,
        settings=settings,
        registry=registry,
        event_producer=event_producer,
        idempotency=idempotency,
        backend_client=backend_client,
        authorize_cache=authorize_cache,
        rate_limiter=rate_limiter,
    )
    if connections is not None:
        connections.add(cp)
    disconnect_reason = "clean"
    try:
        await cp.start()
    except Exception:
        # Any unhandled exception out of cp.start() means the
        # connection terminated abnormally — broker error, runtime
        # bug, etc. Tag the metric so an alert can fire on a sustained
        # `error` rate distinct from clean disconnects.
        disconnect_reason = "error"
        raise
    finally:
        if connections is not None:
            connections.remove(cp)
        was_ours = False
        if registry is not None:
            # Compare-and-delete: only clear if we still own the key.
            # A reconnect to a different pod between disconnect and
            # this call must not clobber the new owner.
            was_ours = await registry.mark_offline(cp_id)
        metrics_registry.WS_CONNECTIONS_ACTIVE.dec()
        metrics_registry.WS_DISCONNECTS_TOTAL.labels(reason=disconnect_reason).inc()
        # cp.offline lifecycle event. Only published when *we* still
        # held the registry key — a reconnect-to-different-pod race
        # already handed ownership over, so emitting offline would
        # confuse presence consumers (offline immediately followed by
        # online from the other pod, with no real outage). The
        # `was_ours` gate is also why we don't publish in the
        # registry-is-None branch — without a registry there's no way
        # to distinguish a real departure from a race.
        if was_ours:
            # Record the offline-marker so the next connect can compute
            # the gap. Same `was_ours` gate as cp.disconnected — a
            # reconnect-to-different-pod race must not overwrite the
            # new pod's marker with our stale one.
            if registry is not None:
                try:
                    await registry.record_offline_marker(cp_id, reason=disconnect_reason)
                except Exception as exc:
                    log.warning("ws.offline_marker_write_failed", cp_id=cp_id, error=str(exc))
            await _publish_lifecycle_event(
                event_producer=event_producer,
                cp_id=cp_id,
                topic=settings.kafka_topic_cp_disconnected,
                payload_field="cp_disconnected",
                payload=events_pb2.CpDisconnected(
                    pod_id=settings.pod_id,
                    reason=disconnect_reason,
                ),
            )
        log.info("ws.disconnected")
        clear_contextvars()


async def serve_forever(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    registry: Registry | None = None,
    connections: ConnectionMap | None = None,
    event_producer: EventProducer | None = None,
    idempotency: IdempotencyCache | None = None,
    backend_client: BackendHTTPClient | None = None,
    authorize_cache: AuthorizeCache | None = None,
    rate_limiter: RateLimiter | None = None,
) -> None:
    """Start the WS server and block until cancelled.

    All Redis/Kafka/HTTP dependencies are optional so unit tests + the
    W1-style local stack can opt out. Production wiring (`__main__.py`)
    always passes all of them — `connections` is how gRPC RemoteStart
    finds the live WS, `event_producer` is how events reach Kafka,
    `registry` is how cross-pod ownership is tracked, `idempotency` is
    how `BootNotification`/`StopTransaction` replays are dropped
    (E2-11), and `backend_client` is how `Authorize` /
    `StartTransaction` / `StopTransaction` consult the backend (E3-3
    onwards).
    """

    async def handler(connection: ServerConnection) -> None:
        await _on_connect(
            connection,
            session_factory=session_factory,
            settings=settings,
            registry=registry,
            connections=connections,
            event_producer=event_producer,
            idempotency=idempotency,
            backend_client=backend_client,
            authorize_cache=authorize_cache,
            rate_limiter=rate_limiter,
        )

    # E5-5 — mTLS context when the operator has wired one. None
    # means plain WS (dev / compose / e2e). The helper raises a
    # clean error at boot if `ws_mtls_enabled=True` but a path is
    # missing — better than a half-initialised SSLContext.
    from eveys_ocpp.transport._tls import build_server_ssl_context

    ssl_ctx = build_server_ssl_context(settings)

    # E5-6 — Basic Auth check at the WS edge. Hooks into
    # `process_request`, the websockets pre-handshake callback. On
    # reject we return a 401 Response and the upgrade never
    # completes — the OCPP handler stack never sees the connection.
    from websockets.datastructures import Headers
    from websockets.http11 import Response

    from eveys_ocpp.transport._basic_auth import verify_basic_auth

    async def _process_request(
        connection: ServerConnection,
        request: object,  # websockets.http11.Request — typed loose to avoid import churn
    ) -> Response | None:
        # The path is the cp_id (gateway URL convention). Strip the
        # leading slash; reject empty cp_id like the existing
        # `_on_connect` does so the metric label stays bounded.
        path = getattr(request, "path", "")
        cp_id = path.strip("/")
        if not cp_id:
            # Empty cp_id is handled by `_on_connect`'s own check;
            # let the upgrade through so that branch fires (with
            # its existing metric).
            return None

        auth_header = request.headers.get("authorization") if hasattr(request, "headers") else None
        result = await verify_basic_auth(
            cp_id=cp_id,
            auth_header=auth_header,
            session_factory=session_factory,
            settings=settings,
        )
        metrics_registry.WS_BASIC_AUTH_TOTAL.labels(outcome=result.outcome).inc()
        if result.accepted:
            return None
        log.info(
            "ws.basic_auth_rejected",
            cp_id=cp_id,
            outcome=result.outcome,
        )
        # 401 with WWW-Authenticate so a charger that simply
        # forgot to send creds gets the right hint.
        return Response(
            status_code=401,
            reason_phrase="Unauthorized",
            headers=Headers([("WWW-Authenticate", 'Basic realm="ocpp"')]),
            body=b"",
        )

    async with serve(
        handler,
        host=settings.ws_host,
        port=settings.ws_port,
        subprotocols=[OCPP_SUBPROTOCOL],
        ssl=ssl_ctx,
        process_request=_process_request,
    ) as server:
        log.info(
            "ws.listening",
            host=settings.ws_host,
            port=settings.ws_port,
            mtls=ssl_ctx is not None,
            basic_auth_required=settings.ws_basic_auth_required,
        )
        await server.serve_forever()
