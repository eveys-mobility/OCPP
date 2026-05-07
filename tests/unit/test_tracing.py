"""OpenTelemetry tracing helpers and integration with metrics + logs.

These tests use OTel's `InMemorySpanExporter` to capture spans
synchronously instead of standing up a real OTLP collector. The
exporter is wired into a fresh TracerProvider that's swapped in for
each test so cross-test isolation holds — the global provider is
reset back to the default no-op afterwards via the module-level
fixture.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from opentelemetry import trace as _otel_trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from eveys_ocpp.observability.tracing import (
    current_span_id,
    current_trace_id,
    extract_context,
    get_tracer,
    inject_context,
)


@pytest.fixture
def in_memory_exporter() -> Iterator[InMemorySpanExporter]:
    """Stand up a tracer provider whose spans land in memory.

    OTel guards `set_tracer_provider` with a one-shot `Once`, so a
    second call from a later test would silently log a WARNING and
    leave the original provider in place. We reset that guard each
    fixture so tests are genuinely isolated.
    """
    from opentelemetry.util._once import Once

    # Reset the one-shot guard so the next set_tracer_provider takes.
    _otel_trace._TRACER_PROVIDER_SET_ONCE = Once()  # type: ignore[attr-defined]
    _otel_trace._TRACER_PROVIDER = None  # type: ignore[attr-defined]
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    _otel_trace.set_tracer_provider(provider)
    yield exporter
    exporter.clear()


def test_get_tracer_returns_a_tracer() -> None:
    tracer = get_tracer(__name__)
    assert tracer is not None


def test_current_trace_id_returns_none_without_active_span() -> None:
    # Outside any span, the current trace ID is INVALID and we report None.
    assert current_trace_id() is None
    assert current_span_id() is None


def test_current_trace_id_returns_hex_inside_span(
    in_memory_exporter: InMemorySpanExporter,
) -> None:
    tracer = get_tracer(__name__)
    with tracer.start_as_current_span("test_span"):
        tid = current_trace_id()
        sid = current_span_id()
        assert tid is not None
        assert sid is not None
        # 32 hex chars for trace, 16 for span — W3C standard.
        assert len(tid) == 32
        assert len(sid) == 16
        int(tid, 16)  # valid hex
        int(sid, 16)


def test_inject_extract_round_trips_context(
    in_memory_exporter: InMemorySpanExporter,
) -> None:
    tracer = get_tracer(__name__)
    with tracer.start_as_current_span("publisher"):
        carrier: dict[str, str] = {}
        inject_context(carrier)
        publisher_trace_id = current_trace_id()
        # `traceparent` is the W3C-standard key the propagator writes.
        assert "traceparent" in carrier

    # In a fresh "process" (no current span), extract the context and
    # use it as the parent for a new span — the resulting trace ID
    # must match the publisher's.
    parent_ctx = extract_context(carrier)
    with tracer.start_as_current_span("subscriber", context=parent_ctx):
        assert current_trace_id() == publisher_trace_id


def test_time_handler_opens_named_span(
    in_memory_exporter: InMemorySpanExporter,
) -> None:
    """`time_handler` is the central wrapper for OCPP handler dispatch;
    every call also opens an `ocpp.handler.<action>` span."""
    from eveys_ocpp.metrics.instrumentation import time_handler

    with time_handler("Heartbeat"):
        pass

    spans = in_memory_exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "ocpp.handler.Heartbeat"
    assert span.attributes is not None
    assert span.attributes.get("ocpp.action") == "Heartbeat"
    assert span.attributes.get("ocpp.outcome") == "ok"


def test_time_handler_records_exception_on_raise(
    in_memory_exporter: InMemorySpanExporter,
) -> None:
    from eveys_ocpp.metrics.instrumentation import time_handler

    with pytest.raises(ValueError), time_handler("Authorize"):
        raise ValueError("boom")

    spans = in_memory_exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    # ERROR (=2) on the OTel StatusCode enum.
    assert span.status.status_code.value == 2
    assert span.attributes is not None
    assert span.attributes.get("ocpp.outcome") == "error"
    # `record_exception` adds an event named `exception`.
    assert any(ev.name == "exception" for ev in span.events)


def test_time_outbound_opens_span_named_after_histogram(
    in_memory_exporter: InMemorySpanExporter,
) -> None:
    from eveys_ocpp.metrics import registry as m
    from eveys_ocpp.metrics.instrumentation import time_outbound

    with time_outbound(
        m.BACKEND_REQUEST_LATENCY_SECONDS,
        endpoint="authorize",
        outcome="ok",
    ):
        pass

    spans = in_memory_exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name.startswith("outbound.")
    assert span.attributes is not None
    # Labels become `outbound.<label>` attributes for trace-side filtering.
    assert span.attributes.get("outbound.endpoint") == "authorize"
    assert span.attributes.get("outbound.outcome") == "ok"


def test_log_processor_adds_trace_id(
    in_memory_exporter: InMemorySpanExporter,
) -> None:
    """Inside a span, every structlog event picks up `trace_id` /
    `span_id`. Outside a span, the keys are absent — verifies the
    no-op path doesn't write `trace_id="0"`."""
    from eveys_ocpp.observability import _inject_trace_context

    out_no_span = _inject_trace_context(None, "info", {"event": "hello"})
    assert "trace_id" not in out_no_span
    assert "span_id" not in out_no_span

    tracer = get_tracer(__name__)
    with tracer.start_as_current_span("for_log"):
        out_with_span = _inject_trace_context(None, "info", {"event": "hello"})
        assert "trace_id" in out_with_span
        assert "span_id" in out_with_span
        assert len(out_with_span["trace_id"]) == 32
        assert len(out_with_span["span_id"]) == 16


def test_configure_tracing_is_idempotent_when_disabled() -> None:
    """`configure_tracing` is a no-op when `tracing_enabled=False`.

    Verifies the boot path doesn't accidentally activate the SDK in
    tests / dev. We can't assert "global provider unchanged" reliably
    because the in_memory_exporter fixture in other tests mutates the
    global; instead assert the function returns None and doesn't raise.
    """
    from eveys_ocpp.observability import configure_tracing
    from eveys_ocpp.settings import Settings

    settings = Settings(tracing_enabled=False)
    assert configure_tracing(settings) is None
    # Second call still no-op.
    assert configure_tracing(settings) is None
