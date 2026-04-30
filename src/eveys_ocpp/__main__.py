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
from eveys_ocpp.connections import ConnectionMap
from eveys_ocpp.events import KafkaEventProducer
from eveys_ocpp.observability import configure_logging, get_logger
from eveys_ocpp.persistence.db import make_engine, make_session_factory
from eveys_ocpp.registry import Registry
from eveys_ocpp.settings import Settings, get_settings
from eveys_ocpp.transport.grpc_server import serve_forever as serve_grpc_forever
from eveys_ocpp.transport.ws_server import serve_forever as serve_ws_forever

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
) -> None:
    """Run WS and gRPC servers concurrently; cancel both if either fails.

    Servers share the same ``ConnectionMap``, ``Registry``, and
    ``CommandBus`` so cross-pod gRPC commands can find the WS opened
    by another pod's WS server.
    """
    log = get_logger(__name__)
    log.info(
        "servers.starting",
        ws_port=settings.ws_port,
        grpc_port=settings.grpc_port,
        pod_id=settings.pod_id,
    )

    await event_producer.start()
    await bus.start()
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                serve_ws_forever(
                    session_factory=session_factory,
                    settings=settings,
                    registry=registry,
                    connections=connections,
                    event_producer=event_producer,
                ),
                name="ws_server",
            )
            tg.create_task(
                serve_grpc_forever(
                    session_factory=session_factory,
                    settings=settings,
                    connections=connections,
                    registry=registry,
                    bus=bus,
                ),
                name="grpc_server",
            )
    finally:
        await bus.stop()
        await event_producer.stop()
        # Single close on the shared client; `registry.close()` would
        # close the same connection twice.
        await redis.aclose()


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
    configure_logging(level=settings.log_level, json=settings.log_json)
    log = get_logger(__name__)
    log.info("startup", version=__version__)

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

    asyncio.run(
        _serve_all(
            session_factory=session_factory,
            settings=settings,
            redis=redis_client,
            registry=registry,
            connections=connections,
            event_producer=event_producer,
            bus=bus,
        )
    )


if __name__ == "__main__":
    main()
