"""Helpers that turn the bare metric primitives in `registry.py` into
something handlers and transports can use without spreading
boilerplate.

The two most repeated patterns in instrumentation:

1. "Time the body, record latency + outcome" → `time_handler`.
2. "Time the body, record latency + outcome, count exceptions" →
   `instrument_handler` (an async wrapper).

Both are tiny on purpose. Anything fancier (per-RPC metadata, span
correlation) belongs in the OpenTelemetry layer (E4-3), not here.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable, Iterator

from eveys_ocpp.metrics import registry as m


@contextlib.contextmanager
def time_handler(action: str) -> Iterator[Callable[[str], None]]:
    """Context manager that times the body and emits one observation
    on `OCPP_HANDLER_LATENCY_SECONDS`.

    Yields a setter the caller uses to override the outcome label
    before the block exits. The default outcome is "ok"; raising lets
    `instrument_handler` (below) tag it as "error" instead. Idiomatic
    use:

        with time_handler("Heartbeat") as set_outcome:
            ...
            if served_from_cache:
                set_outcome("replay")

    Per-handler counters (`HEARTBEATS_TOTAL` etc.) are emitted by the
    handler itself, not from inside the context manager — the latency
    histogram is the only thing this helper owns.
    """
    start = time.perf_counter()
    outcome_holder = ["ok"]

    def set_outcome(value: str) -> None:
        outcome_holder[0] = value

    try:
        yield set_outcome
    finally:
        elapsed = time.perf_counter() - start
        m.OCPP_HANDLER_LATENCY_SECONDS.labels(
            action=action,
            outcome=outcome_holder[0],
        ).observe(elapsed)


@contextlib.contextmanager
def time_outbound(
    histogram: object,
    /,
    **labels: str,
) -> Iterator[None]:
    """Generic latency timer for any histogram with arbitrary labels.

    Used by the backend client, webhook dispatcher, Kafka producer,
    DB and Redis call sites — anywhere that "time the body, observe
    on histogram H labelled L" is the only thing we want.

    Pass the histogram positionally; labels follow as keyword args
    matching the histogram's `labelnames`. If the call raises, the
    latency observation still fires (the surrounding code is
    responsible for incrementing the right error counter).
    """
    # `histogram` is typed as `object` because prometheus_client's
    # Histogram is a Generic that's awkward to import here without a
    # circular dep on registry's bucket constants.
    start = time.perf_counter()
    try:
        yield
    finally:
        histogram.labels(**labels).observe(time.perf_counter() - start)  # type: ignore[attr-defined]


def record_handler_error(action: str, exc: BaseException) -> None:
    """Convenience for the common error path: increment the error
    counter labelled by action + bounded exception class name.

    The class name is bounded because handlers raise from a small set
    of typed exceptions; we never label by `repr(exc)` (which would
    include the message, blowing up cardinality).
    """
    m.OCPP_HANDLER_ERRORS_TOTAL.labels(
        action=action,
        error_type=type(exc).__name__,
    ).inc()


__all__ = [
    "record_handler_error",
    "time_handler",
    "time_outbound",
]
