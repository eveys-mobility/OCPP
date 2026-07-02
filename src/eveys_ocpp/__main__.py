"""Entry point: `python -m eveys_ocpp`.

Boots WS + gRPC servers in the same event loop. Either failing causes
the whole process to exit (no half-up state).
"""

from __future__ import annotations

import asyncio
import signal
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
from eveys_ocpp.pending_authorizations import PendingAuthorizations
from eveys_ocpp.persistence.db import make_engine, make_session_factory
from eveys_ocpp.platform import AuthorizeCache, BackendHTTPClient
from eveys_ocpp.registry import Registry
from eveys_ocpp.settings import Settings, get_settings
from eveys_ocpp.shutdown import DrainController
from eveys_ocpp.transport._ip_rate_limiter import IpRateLimiter
from eveys_ocpp.transport._rate_limiter import RateLimiter
from eveys_ocpp.transport.grpc_server import OcppGatewayService
from eveys_ocpp.transport.grpc_server import serve_forever as serve_grpc_forever
from eveys_ocpp.transport.rest_server import serve_forever as serve_rest_forever
from eveys_ocpp.transport.ws_server import serve_forever as serve_ws_forever
from eveys_ocpp.webhooks import WebhookBacklogDrainer, WebhookDispatcher

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class _DrainTriggered(Exception):
    """Internal sentinel raised inside the TaskGroup once the drain
    grace period has elapsed. Causes asyncio to cancel the sibling
    tasks so the existing finally block in `_serve_all` runs the
    normal teardown sequence (bus stop, kafka flush, redis close,
    span flush). Never propagates past `_serve_all` — the outer
    `try/except*` swallows it.
    """


class _NullAsyncContext:
    """Trivial `async with` no-op."""

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


def _maybe_timeout(
    seconds: float | None,
) -> _NullAsyncContext | asyncio.Timeout:
    """Return `asyncio.timeout(seconds)` when seconds is set; otherwise a
    no-op async context manager. Used so the teardown path is bounded
    when shutting down via drain (signal-driven) but unbounded when
    the gateway exits for an unrelated reason (e.g. WS server crash
    in tests) — there's no safety net to rush past in that case.
    """
    if seconds is None:
        return _NullAsyncContext()
    return asyncio.timeout(seconds)


async def _drain_orchestrator(
    drain_controller: DrainController,
    drain_event: asyncio.Event,
    settings: Settings,
) -> None:
    """Wait for SIGTERM, then orchestrate the readiness-flip phase.

    1. Block on ``drain_event``. The signal handler sets the event.
    2. Flip the drain flag — `/api/v1/ready` starts returning 503.
    3. Sleep ``shutdown_readiness_propagation_seconds`` so the LB's
       readiness probe has time to fail and remove this pod from
       rotation. Existing chargers stay connected during this
       window; only **new** WS upgrades stop arriving here.
    4. Raise ``_DrainTriggered`` to break the TaskGroup. The outer
       finally block in ``_serve_all`` then runs normal teardown.
    """
    log = get_logger(__name__)
    await drain_event.wait()
    if not drain_controller.is_draining:
        drain_controller.begin_drain()
    propagation = settings.shutdown_readiness_propagation_seconds
    log.info(
        "drain.readiness_propagation_start",
        propagation_seconds=propagation,
    )
    if propagation > 0:
        await asyncio.sleep(propagation)
    log.info("drain.readiness_propagation_done")
    raise _DrainTriggered


def _install_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    drain_event: asyncio.Event,
    drain_controller: DrainController,
    settings: Settings,
) -> None:
    """Wire SIGTERM and SIGINT to the drain event.

    On platforms without `add_signal_handler` (Windows), the call
    raises NotImplementedError; the gateway falls back to KeyboardInterrupt
    handling at the asyncio.run boundary.

    When `shutdown_drain_enabled=False`, signals are NOT intercepted —
    asyncio's default behaviour cancels the running tasks, which
    matches the pre-drain behaviour exactly.
    """
    if not settings.shutdown_drain_enabled:
        return
    log = get_logger(__name__)

    def _on_signal(signum: int) -> None:
        signal_name = signal.Signals(signum).name
        if drain_event.is_set():
            log.warning("drain.signal_repeat", signal=signal_name)
            return
        log.info("drain.signal_received", signal=signal_name)
        drain_controller.begin_drain()
        drain_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_signal, sig)
        except (NotImplementedError, RuntimeError):
            # Windows or non-main-thread loops: nothing to install.
            log.warning("drain.signal_handler_unavailable", signal=signal.Signals(sig).name)


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
    rate_limiter: RateLimiter | None,
    pending_store: PendingAuthorizations,
    ip_rate_limiter: IpRateLimiter,
) -> None:
    """Run WS and gRPC servers concurrently; cancel both if either fails.

    Servers share the same ``ConnectionMap``, ``Registry``,
    ``CommandBus``, and ``IdempotencyCache`` so cross-pod gRPC commands
    can find the WS opened by another pod's WS server, and replay
    detection works regardless of which pod ack'd the original.

    Graceful shutdown: on SIGTERM/SIGINT the drain orchestrator flips
    `/api/v1/ready` to 503, waits for the load balancer to remove
    this pod from rotation, then breaks the TaskGroup so the normal
    teardown finally block runs. Bounded by
    ``shutdown_grace_period_seconds`` — anything still hanging at
    that point is force-cancelled.
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

    drain_controller = DrainController()
    drain_event = asyncio.Event()
    _install_signal_handlers(asyncio.get_running_loop(), drain_event, drain_controller, settings)

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
    #
    # The optional `WebhookBacklogDrainer` sibling picks up envelopes
    # the dispatcher failed to deliver in-loop and keeps retrying
    # them on a coarser cadence — see webhooks/backlog_drainer.py.
    # Both share the session factory so the enqueue-side (dispatcher)
    # and the drain-side (drainer) speak to the same Postgres.
    webhook_dispatcher: WebhookDispatcher | None = None
    webhook_backlog_drainer: WebhookBacklogDrainer | None = None
    if settings.webhook_base_url:
        webhook_dispatcher = WebhookDispatcher(settings, session_factory=session_factory)
        await webhook_dispatcher.start()
        log.info("webhook_dispatcher.configured", base_url=settings.webhook_base_url)
        if settings.webhook_backlog_enabled:
            webhook_backlog_drainer = WebhookBacklogDrainer(
                settings, session_factory=session_factory
            )
            await webhook_backlog_drainer.start()
            log.info("webhook_backlog.configured")
        else:
            log.info("webhook_backlog.disabled")
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
                        rate_limiter=rate_limiter,
                        pending_store=pending_store,
                        ip_rate_limiter=ip_rate_limiter,
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
                            drain_controller=drain_controller,
                            connections=connections,
                            pending_store=pending_store,
                        ),
                        name="rest_server",
                    )
                if webhook_dispatcher is not None:
                    tg.create_task(
                        webhook_dispatcher.serve_forever(),
                        name="webhook_dispatcher",
                    )
                if webhook_backlog_drainer is not None:
                    tg.create_task(
                        webhook_backlog_drainer.serve_forever(),
                        name="webhook_backlog_drainer",
                    )
                # Drain orchestrator: idle until SIGTERM, then flips
                # readiness, sleeps for LB propagation, then raises
                # `_DrainTriggered` to break the TaskGroup. Disabled
                # when `shutdown_drain_enabled=False`.
                if settings.shutdown_drain_enabled:
                    tg.create_task(
                        _drain_orchestrator(drain_controller, drain_event, settings),
                        name="drain_orchestrator",
                    )
        except* _DrainTriggered:
            log.info("drain.taskgroup_cancelled")
    finally:
        # Bound the teardown by the configured grace period when the
        # gateway is shutting down because of a drain. asyncio.timeout
        # only applies to the body — already-completed awaits are
        # untouched. On timeout the remaining teardown is skipped, the
        # process exits, and the kubelet's terminationGracePeriodSeconds
        # is the next safety net (it sends SIGKILL after that).
        teardown_budget = (
            settings.shutdown_grace_period_seconds if drain_controller.is_draining else None
        )
        try:
            async with _maybe_timeout(teardown_budget):
                await bus.stop()
                await event_producer.stop()
                if webhook_dispatcher is not None:
                    await webhook_dispatcher.stop()
                if webhook_backlog_drainer is not None:
                    await webhook_backlog_drainer.stop()
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
        except TimeoutError:
            log.warning(
                "drain.teardown_timeout",
                budget_seconds=teardown_budget,
                note="some shutdown steps did not complete within grace period",
            )


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
        sentry_enabled=bool(settings.sentry_dsn.get_secret_value()),
    )

    engine = make_engine(
        # E5-7: db_url is a SecretStr; reach into the wrapper here at
        # the SQLAlchemy boundary. The DSN string itself never lands
        # in any Settings dump.
        settings.db_url.get_secret_value(),
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

    # E5-3 per-charger rate limiter. Disabled via the kill-switch in
    # settings; on a Redis blip the limiter fails open (see
    # _rate_limiter.py) so a transient Redis fault doesn't DoS the
    # fleet by mistake.
    rate_limiter: RateLimiter | None = None
    if settings.ws_rate_limit_enabled:
        rate_limiter = RateLimiter(redis_client, settings=settings)
        log.info(
            "rate_limiter.enabled",
            capacity=settings.ws_rate_limit_capacity,
            refill_per_second=settings.ws_rate_limit_refill_per_second,
        )
    else:
        log.info("rate_limiter.disabled")

    # Pending-authorization Redis store + WS-upgrade IP rate limiter.
    # The pending store holds unauthorized devices; the IP limiter gates
    # WS upgrades from unknown cp_ids. Authorized fleet members bypass
    # the IP limiter entirely (see transport/_authorization.py).
    pending_store = PendingAuthorizations(redis_client, settings=settings)
    ip_rate_limiter = IpRateLimiter(redis_client, settings=settings)
    log.info(
        "pending_authorizations.enabled",
        ttl_seconds=settings.pending_authorization_ttl_seconds,
    )
    log.info(
        "ip_rate_limiter.enabled",
        max_per_minute=settings.ip_rate_limit_requests_per_minute,
        block_seconds=settings.ip_rate_limit_block_seconds,
    )

    # Backend HTTP client (E3-2, ADR-0023). Empty `backend_base_url`
    # leaves it None — the OCPP handlers fall back to their stub
    # behaviour, which is what the W1 / local-dev stack wants.
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
            rate_limiter=rate_limiter,
            pending_store=pending_store,
            ip_rate_limiter=ip_rate_limiter,
        )
    )


if __name__ == "__main__":
    main()
