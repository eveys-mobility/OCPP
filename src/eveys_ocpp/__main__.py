"""Entry point: `python -m eveys_ocpp`.

Boots WS + gRPC servers in the same event loop. Either failing causes
the whole process to exit (no half-up state).
"""

from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

from redis.asyncio import Redis

from eveys_ocpp import __version__
from eveys_ocpp.bus import CommandBus
from eveys_ocpp.clickhouse.read_client import ClickHouseReadClient
from eveys_ocpp.connections import ConnectionMap
from eveys_ocpp.events import KafkaEventProducer
from eveys_ocpp.idempotency import IdempotencyCache
from eveys_ocpp.metrics import MetricsServer
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import (
    configure_logging,
    configure_tracing,
    get_logger,
    init_sentry,
    shutdown_tracing,
)
from eveys_ocpp.persistence.db import make_engine, make_session_factory
from eveys_ocpp.platform import AuthorizeCache, BackendHTTPClient
from eveys_ocpp.registry import Registry
from eveys_ocpp.settings import Settings, get_settings
from eveys_ocpp.transport.grpc_server import OcppGatewayService
from eveys_ocpp.transport.grpc_server import serve_forever as serve_grpc_forever
from eveys_ocpp.transport.rest_server import serve_forever as serve_rest_forever
from eveys_ocpp.transport.ws_server import serve_forever as serve_ws_forever
from eveys_ocpp.webhooks import WebhookDispatcher

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


async def _serve_all(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    redis: Redis,
    registry: Registry,
    connections: ConnectionMap,
    event_producer: KafkaEventProducer,
    bus: CommandBus,
    idempotency: IdempotencyCache,
    backend_client: BackendHTTPClient | None,
    authorize_cache: AuthorizeCache | None,
) -> None:
    """Run WS and gRPC servers concurrently; cancel both if either fails.

    Servers share the same ``ConnectionMap``, ``Registry``,
    ``CommandBus``, and ``IdempotencyCache`` so cross-pod gRPC commands
    can find the WS opened by another pod's WS server, and replay
    detection works regardless of which pod ack'd the original.
    """
    log = get_logger(__name__)
    log.info(
        "servers.starting",
        ws_port=settings.ws_port,
        grpc_port=settings.grpc_port,
        rest_port=settings.rest_port,
        rest_enabled=settings.rest_enabled,
        pod_id=settings.pod_id,
    )

    # Build the gRPC service once and share it with the REST command
    # surface (E3-8). Both transports dispatch through the same
    # ConnectionMap / Registry / CommandBus, so a charger connected on
    # this pod is reachable from either entry point. The service's
    # __init__ also wires the bus's owning-side dispatcher; constructing
    # it here ensures that hook fires before any inbound bus traffic.
    command_service = OcppGatewayService(
        session_factory=session_factory,
        settings=settings,
        connections=connections,
        registry=registry,
        bus=bus,
    )

    # E3-7d: ClickHouse read client backs the timeseries endpoints
    # (meter-values, status-history). Only constructed when REST is
    # enabled — sidecar shapes that don't serve HTTP don't need it.
    ch_client: ClickHouseReadClient | None = None
    if settings.rest_enabled:
        ch_client = ClickHouseReadClient(settings)
        await ch_client.start()

    # E3-9: webhook dispatcher tails the Kafka event topics and POSTs
    # signed deliveries at backend-configured URLs. Only constructed
    # when `webhook_base_url` is set — empty disables the whole
    # subsystem (dev runs without a backend skip it cleanly).
    webhook_dispatcher: WebhookDispatcher | None = None
    if settings.webhook_base_url:
        webhook_dispatcher = WebhookDispatcher(settings)
        await webhook_dispatcher.start()
        log.info("webhook_dispatcher.configured", base_url=settings.webhook_base_url)
    else:
        log.info("webhook_dispatcher.disabled")

    await event_producer.start()
    await bus.start()

    # Phase 4 / E4-1: Prometheus scrape endpoint.
    #
    # MetricsServer runs the HTTP server on its own daemon thread, so
    # nothing is added to the TaskGroup; we just start it before and
    # stop it after. metrics_enabled=False (set by an autouse fixture
    # in tests) skips the bind so a pytest run never reserves 9100.
    metrics_server: MetricsServer | None = None
    if settings.metrics_enabled:
        metrics_server = MetricsServer(
            host=settings.metrics_host,
            port=settings.metrics_port,
            include_python_collectors=settings.metrics_include_python_collectors,
        )
        await metrics_server.start()
        # Set the build_info gauge once. Static labels carry the
        # version + pod id so a Grafana table panel can render the
        # running fleet without needing a separate info-counter.
        metrics_registry.BUILD_INFO.labels(
            version=__version__,
            pod_id=settings.pod_id,
        ).set(1)

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                serve_ws_forever(
                    session_factory=session_factory,
                    settings=settings,
                    registry=registry,
                    connections=connections,
                    event_producer=event_producer,
                    idempotency=idempotency,
                    backend_client=backend_client,
                    authorize_cache=authorize_cache,
                ),
                name="ws_server",
            )
            tg.create_task(
                serve_grpc_forever(settings=settings, service=command_service),
                name="grpc_server",
            )
            # E3-7: gateway-side REST API for the backend's read needs.
            # E3-8: same surface picks up the 19 command endpoints by
            # consuming `command_service`. Gated on `rest_enabled` so
            # shapes that share this image but don't serve HTTP (e.g.
            # the clickhouse-ingestor sidecar) skip booting it. Per
            # ADR-0026.
            if settings.rest_enabled:
                tg.create_task(
                    serve_rest_forever(
                        session_factory=session_factory,
                        settings=settings,
                        registry=registry,
                        redis=redis,
                        command_service=command_service,
                        ch_client=ch_client,
                    ),
                    name="rest_server",
                )
            if webhook_dispatcher is not None:
                tg.create_task(
                    webhook_dispatcher.serve_forever(),
                    name="webhook_dispatcher",
                )
    finally:
        await bus.stop()
        await event_producer.stop()
        if webhook_dispatcher is not None:
            await webhook_dispatcher.stop()
        if backend_client is not None:
            await backend_client.aclose()
        if ch_client is not None:
            await ch_client.aclose()
        # Single close on the shared client; `registry.close()` would
        # close the same connection twice.
        await redis.aclose()
        if metrics_server is not None:
            await metrics_server.stop()
        # Flush in-flight spans to the OTLP exporter. No-op when
        # tracing was never configured. Last in the teardown so spans
        # for the shutdown sequence itself are exported.
        shutdown_tracing()


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--version":
        print(__version__)
        return

    try:
        import uvloop

        uvloop.install()
    except ImportError:
        # Windows or unusual env: fall back to default asyncio loop.
        pass

    settings = get_settings()
    # Sentry first — wires into stdlib logging so init failures and
    # subsequent observability setup errors are captured. No-op when
    # `sentry_dsn=""`, which is the default.
    init_sentry(settings)
    configure_logging(level=settings.log_level, json=settings.log_json)
    # Tracing must be set up before any handler emits a span. Idempotent
    # and a no-op when `tracing_enabled=False`, so safe to call early.
    # The shutdown path in the run() finally block flushes pending spans.
    configure_tracing(settings)
    log = get_logger(__name__)
    log.info(
        "startup",
        version=__version__,
        tracing_enabled=settings.tracing_enabled,
        sentry_enabled=bool(settings.sentry_dsn),
    )

    engine = make_engine(
        settings.db_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )
    session_factory = make_session_factory(engine)

    # Share one Redis client between the registry and the bus to keep
    # connection count flat (otherwise each pod opens 2x pools to Redis).
    redis_client = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        health_check_interval=30,
    )
    registry = Registry(redis_client, settings=settings)
    connections = ConnectionMap()
    event_producer = KafkaEventProducer.from_settings(settings)
    bus = CommandBus(
        redis_client,
        pod_id=settings.pod_id,
        connections=connections,
        request_timeout_seconds=float(settings.bus_request_timeout_seconds),
    )
    idempotency = IdempotencyCache(redis_client, settings=settings)

    # Backend HTTP client (E3-2, ADR-0023). Empty `backend_base_url`
    # leaves it None — the OCPP handlers fall back to their stub
    # behaviour, which is what the W1 / dev-laptop stack wants.
    backend_client: BackendHTTPClient | None = None
    authorize_cache: AuthorizeCache | None = None
    if settings.backend_base_url:
        backend_client = BackendHTTPClient.from_settings(settings)
        log.info(
            "backend_client.configured",
            base_url=settings.backend_base_url,
            authorize_fallback=settings.backend_authorize_fallback,
        )
        # Share the same Redis client used for registry / bus /
        # idempotency. Cache is meaningful only when there's a
        # backend to cache *for* — gating on `backend_client`
        # avoids a stranded cache that nobody reads.
        if settings.backend_authorize_cache_enabled:
            authorize_cache = AuthorizeCache(redis_client, settings=settings)
            log.info(
                "authorize_cache.enabled",
                ttl_seconds=settings.backend_authorize_cache_ttl_seconds,
            )
        else:
            log.info("authorize_cache.disabled")
    else:
        log.info("backend_client.disabled")

    asyncio.run(
        _serve_all(
            session_factory=session_factory,
            settings=settings,
            redis=redis_client,
            registry=registry,
            connections=connections,
            event_producer=event_producer,
            bus=bus,
            idempotency=idempotency,
            backend_client=backend_client,
            authorize_cache=authorize_cache,
        )
    )


if __name__ == "__main__":
    main()
