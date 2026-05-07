"""Helpers that turn the bare metric primitives in `registry.py` into
something handlers and transports can use without spreading
boilerplate.

The two most repeated patterns in instrumentation:

1. "Time the body, record latency + outcome" → `time_handler`.
2. "Time the body, record latency + outcome, count exceptions" →
   `instrument_handler` (an async wrapper).

Both are tiny on purpose.

Tracing piggybacks on these helpers (E4-3): every `time_handler` /
`time_outbound` call also opens an OpenTelemetry span so the metric
observation and the trace stay aligned with no second-pass
instrumentation. When tracing is disabled the span is a few-ns no-op.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable, Iterator

from opentelemetry import trace as _otel_trace
from opentelemetry.trace import Status, StatusCode

from eveys_ocpp.metrics import registry as m

# Resolved per-call rather than cached at import — `get_tracer` returns
# a tracer bound to the *current* global TracerProvider, and tests swap
# that provider in fixtures. Caching at import would freeze the no-op
# tracer that exists before any provider is set.
_TRACER_NAME = "eveys_ocpp.metrics.instrumentation"


@contextlib.contextmanager
def time_handler(action: str) -> Iterator[Callable[[str], None]]:
    """Context manager that times the body and emits one observation
    on `OCPP_HANDLER_LATENCY_SECONDS`.

    Yields a setter the caller uses to override the outcome label
    before the block exits. Default outcome is "ok"; if the body raises,
    the outcome is automatically tagged "error" before the latency
    observation fires (no caller plumbing needed). Idiomatic use:

        with time_handler("Heartbeat") as set_outcome:
            ...
            if served_from_cache:
                set_outcome("replay")

    Per-handler counters (`HEARTBEATS_TOTAL` etc.) are emitted by the
    handler itself, not from inside the context manager — the latency
    histogram is the only thing this helper owns.

    Also opens an OpenTelemetry span named `ocpp.handler.<action>`. The
    final outcome is recorded as a span attribute (`ocpp.outcome`) and
    drives span status (`OK` for "ok"/"replay", `ERROR` otherwise).
    """
    start = time.perf_counter()
    outcome_holder = ["ok"]
    raised = False

    def set_outcome(value: str) -> None:
        outcome_holder[0] = value

    with _otel_trace.get_tracer(_TRACER_NAME).start_as_current_span(
        f"ocpp.handler.{action}",
        attributes={"ocpp.action": action},
    ) as span:
        try:
            yield set_outcome
        except BaseException as exc:
            outcome_holder[0] = "error"
            raised = True
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            raise
        finally:
            elapsed = time.perf_counter() - start
            outcome = outcome_holder[0]
            span.set_attribute("ocpp.outcome", outcome)
            # Mark non-ok / non-replay outcomes (e.g. "rejected") as
            # ERROR so the trace UI surfaces them; the exception path
            # already set ERROR above. OK status is OTel's default.
            if not raised and outcome not in ("ok", "replay"):
                span.set_status(Status(StatusCode.ERROR, outcome))
            m.OCPP_HANDLER_LATENCY_SECONDS.labels(
                action=action,
                outcome=outcome,
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

    Also opens an OpenTelemetry span named after the histogram (e.g.
    `outbound.eveys_ocpp_backend_request_latency_seconds`). The label
    dict becomes span attributes prefixed `outbound.<label>`, giving the
    trace UI the same dimensions Prometheus has.
    """
    # `histogram` is typed as `object` because prometheus_client's
    # Histogram is a Generic that's awkward to import here without a
    # circular dep on registry's bucket constants.
    span_name = f"outbound.{getattr(histogram, '_name', 'unknown')}"
    span_attrs = {f"outbound.{k}": v for k, v in labels.items()}
    start = time.perf_counter()
    with _otel_trace.get_tracer(_TRACER_NAME).start_as_current_span(
        span_name, attributes=span_attrs
    ) as span:
        try:
            yield
        except BaseException as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            raise
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
