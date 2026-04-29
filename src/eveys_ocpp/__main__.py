"""Entry point: `python -m eveys_ocpp`."""

from __future__ import annotations

import asyncio
import sys

from eveys_ocpp import __version__
from eveys_ocpp.observability import configure_logging, get_logger
from eveys_ocpp.persistence.db import make_engine, make_session_factory
from eveys_ocpp.settings import get_settings
from eveys_ocpp.transport.ws_server import serve_forever


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

    asyncio.run(serve_forever(session_factory=session_factory, settings=settings))


if __name__ == "__main__":
    main()
