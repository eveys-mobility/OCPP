"""Entry point: `python -m eveys_ocpp`.

Boots WS + gRPC servers in the same event loop. Either failing causes
the whole process to exit (no half-up state).
"""

from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

from eveys_ocpp import __version__
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
    registry: Registry,
) -> None:
    """Run WS and gRPC servers concurrently; cancel both if either fails."""
    log = get_logger(__name__)
    log.info(
        "servers.starting",
        ws_port=settings.ws_port,
        grpc_port=settings.grpc_port,
        pod_id=settings.pod_id,
    )

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                serve_ws_forever(
                    session_factory=session_factory,
                    settings=settings,
                    registry=registry,
                ),
                name="ws_server",
            )
            tg.create_task(
                serve_grpc_forever(session_factory=session_factory, settings=settings),
                name="grpc_server",
            )
    finally:
        await registry.close()


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
    registry = Registry.from_settings(settings)

    asyncio.run(
        _serve_all(
            session_factory=session_factory,
            settings=settings,
            registry=registry,
        )
    )


if __name__ == "__main__":
    main()
