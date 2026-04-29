"""Structured logging setup.

Every log line carries `cp_id`, `message_id`, `action`, `direction` when
those are bound into the contextvars. Per `AGENTS.md` rule and OCPP project
conventions.
"""

from __future__ import annotations

import logging
import sys

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars
from structlog.types import Processor

__all__ = ["bind_contextvars", "clear_contextvars", "configure_logging", "get_logger"]


def configure_logging(*, level: str = "INFO", json: bool = True) -> None:
    """Configure structlog + stdlib logging in one place.

    Idempotent — safe to call multiple times in tests.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    renderer: Processor
    if json:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )

    # Mirror to stdlib so third-party libraries (asyncpg, websockets) flow
    # through the same renderer.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=log_level,
        force=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a configured logger. Use module `__name__` as the argument."""
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    return logger
