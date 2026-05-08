"""Structured logging setup + tracing re-exports.

Every log line carries `cp_id`, `message_id`, `action`, `direction` when
those are bound into the contextvars. Per `AGENTS.md` rule and OCPP project
conventions.

When tracing is active, every log line also picks up `trace_id` and
`span_id` from the current OpenTelemetry context — see the
`_inject_trace_context` processor below. Trace correlation works
*regardless* of whether `configure_tracing` was called: when tracing is
disabled the processor is a no-op (current span is `INVALID`).
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars
from structlog.types import EventDict, Processor

# Re-export the tracing + sentry helpers so callers can
# `from eveys_ocpp.observability import get_tracer, configure_tracing,
# init_sentry` without knowing the submodule layout.
from eveys_ocpp.observability.sentry import bind_sentry_scope, init_sentry
from eveys_ocpp.observability.tracing import (
    configure_tracing,
    current_span_id,
    current_trace_id,
    extract_context,
    get_tracer,
    inject_context,
    shutdown_tracing,
)

__all__ = [
    "apply_log_level",
    "bind_contextvars",
    "bind_sentry_scope",
    "clear_contextvars",
    "configure_logging",
    "configure_tracing",
    "current_span_id",
    "current_trace_id",
    "extract_context",
    "get_logger",
    "get_tracer",
    "init_sentry",
    "inject_context",
    "shutdown_tracing",
]


def _inject_trace_context(_logger: Any, _name: str, event_dict: EventDict) -> EventDict:
    """structlog processor that adds `trace_id` / `span_id` to every log
    line when there's an active OpenTelemetry span.

    Cheap when tracing is off — `current_trace_id()` reads a contextvar
    and bails on `INVALID`. Doesn't add the keys at all in that case
    (vs `trace_id="0"`), so log lines stay tidy in tracing-disabled
    environments.
    """
    trace_id = current_trace_id()
    if trace_id is not None:
        event_dict["trace_id"] = trace_id
        span_id = current_span_id()
        if span_id is not None:
            event_dict["span_id"] = span_id
    return event_dict


def configure_logging(*, level: str = "INFO", json: bool = True) -> None:
    """Configure structlog + stdlib logging in one place.

    Idempotent — safe to call multiple times in tests.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _inject_trace_context,
        bind_sentry_scope,
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


def apply_log_level(level: str) -> None:
    """Apply a runtime log-level change to stdlib loggers.

    The structlog filtering wrapper class is fixed at
    `configure_logging` time and can't be re-bound without
    reconfiguring every cached logger — but most output flows
    through stdlib (asyncpg, websockets, the structlog mirror at
    `logging.basicConfig`), so flipping the stdlib level catches
    the bulk of what an operator wants when they bump verbosity
    via the admin endpoint.

    A future improvement could swap the structlog wrapper too, at
    the cost of dropping the `cache_logger_on_first_use=True`
    optimisation. Not worth it for v0.
    """
    numeric = getattr(logging, level.upper(), None)
    if not isinstance(numeric, int):
        raise ValueError(f"unknown log level: {level!r}")
    logging.getLogger().setLevel(numeric)
