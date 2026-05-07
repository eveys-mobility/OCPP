"""OpenTelemetry tracing — bootstrap, helpers, context propagation.

Contract for the rest of the codebase:

- `get_tracer(__name__)` returns a `Tracer` you can use unconditionally.
  When tracing is off, every span is a few-ns no-op (`NoOpTracerProvider`
  is the global default).
- `configure_tracing(settings)` is called exactly once at boot from
  `__main__.py`. Idempotent within a process — second calls are a
  no-op so test fixtures can be liberal.
- `current_trace_id()` / `current_span_id()` are read by the structlog
  processor in `observability/__init__.py` to attach trace correlation
  IDs to every log line.
- `inject_context(carrier)` and `extract_context(carrier)` propagate
  W3C trace context across process boundaries: the cross-pod gRPC bus
  uses these so a `RemoteStart` trace started in pod A continues in
  pod B.

The exporter is OTLP/gRPC. We pin to the gRPC variant (not http) for
hot-path efficiency; if a deployment needs HTTP, swap the exporter in
`configure_tracing` — the rest of the file is exporter-agnostic.

Why the SDK lives in this one file: importing `opentelemetry.sdk.*` is
cheap but pulls in protobuf and grpc; keeping it in one module means we
can audit the import graph easily and means non-app code (handlers,
unit tests) only ever touches the API.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import (
    INVALID_SPAN_ID,
    INVALID_TRACE_ID,
    NoOpTracer,
    Tracer,
    get_current_span,
)

if TYPE_CHECKING:
    from eveys_ocpp.settings import Settings


# Set to True the first time `configure_tracing` succeeds. Re-entrant
# calls become no-ops — a second `configure_tracing` would tear down
# the existing TracerProvider and lose any in-flight spans, which is
# never what callers want.
_CONFIGURED = False


def configure_tracing(settings: Settings) -> None:
    """Activate the OpenTelemetry SDK if `settings.tracing_enabled`.

    No-op when tracing is disabled — leaves the global
    `NoOpTracerProvider` in place so `tracer.start_as_current_span(...)`
    everywhere else is a few-ns hop.

    Imports the SDK lazily so the import graph for tracing-disabled
    deployments stays minimal. The SDK pulls in protobuf + grpc; we
    don't want that cost on every test that imports `eveys_ocpp`.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return
    if not settings.tracing_enabled:
        return

    # Lazy imports — keep the SDK out of the import graph until the
    # gateway actually wants tracing. Type-checked code only touches
    # the api package, which is always importable.
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import (
        ParentBased,
        TraceIdRatioBased,
    )

    resource = Resource.create(
        {
            "service.name": settings.tracing_service_name,
            # `service.instance.id` discriminates between replicas of
            # the same service. We reuse `pod_id` so trace + log + metric
            # share the same identifier.
            "service.instance.id": settings.pod_id,
        }
    )
    # ParentBased(TraceIdRatioBased(rate)) honours the sampling
    # decision of an upstream tracer when a parent context is present.
    # For root spans (no parent), the rate decides. This matches the
    # OTel default and keeps sampling consistent across services.
    sampler = ParentBased(root=TraceIdRatioBased(settings.tracing_sample_rate))
    provider = TracerProvider(resource=resource, sampler=sampler)

    exporter = OTLPSpanExporter(endpoint=settings.tracing_otlp_endpoint, insecure=True)
    # Batch processor: spans flush asynchronously in batches; keeps the
    # hot path off the export network. Defaults are fine — 5s flush
    # interval, 512 spans per batch.
    provider.add_span_processor(BatchSpanProcessor(exporter))

    trace.set_tracer_provider(provider)
    _CONFIGURED = True


def shutdown_tracing() -> None:
    """Flush in-flight spans and shut the exporter down cleanly.

    Called from `__main__.py`'s shutdown path. Safe to call when
    tracing was never configured (no-op TracerProvider has no
    `shutdown`). Catches AttributeError to keep teardown silent in
    the never-configured case.
    """
    provider = trace.get_tracer_provider()
    shutdown = getattr(provider, "shutdown", None)
    if shutdown is not None:
        shutdown()


def get_tracer(name: str) -> Tracer:
    """Return a tracer. Use `__name__` of the calling module as `name`.

    When tracing is disabled, returns a `NoOpTracer` — every method
    on the resulting spans is a no-op. Zero-cost from the call site,
    so handlers can use `with tracer.start_as_current_span(...)`
    unconditionally.
    """
    return trace.get_tracer(name)


def current_trace_id() -> str | None:
    """Hex-encoded trace ID of the current span, or None if no span.

    Returns the canonical 32-hex-char form. Used by the structlog
    processor to attach correlation IDs to log lines.
    """
    span = get_current_span()
    ctx = span.get_span_context()
    if ctx.trace_id == INVALID_TRACE_ID:
        return None
    return f"{ctx.trace_id:032x}"


def current_span_id() -> str | None:
    """Hex-encoded span ID of the current span, or None if no span.

    16 hex chars per the W3C trace-context spec. Pairs with
    `current_trace_id()` for log correlation.
    """
    span = get_current_span()
    ctx = span.get_span_context()
    if ctx.span_id == INVALID_SPAN_ID:
        return None
    return f"{ctx.span_id:016x}"


def inject_context(carrier: dict[str, str]) -> None:
    """Inject the current trace context into a carrier dict.

    Used at outbound boundaries — cross-pod gRPC bus, backend HTTP
    client — to propagate the trace through the network. The carrier
    must be a `dict[str, str]`; the OTel propagator writes
    `traceparent` and (optionally) `tracestate`.
    """
    inject(carrier)


def extract_context(carrier: dict[str, str]) -> Any:
    """Extract a trace context from a carrier dict and return it.

    Used at inbound boundaries — gRPC server interceptor, REST middleware,
    Kafka consumer — to attach the upstream trace as the parent of any
    spans we create. Hand the returned context to
    `tracer.start_as_current_span(..., context=ctx)`.
    """
    return extract(carrier)


__all__ = [
    "NoOpTracer",
    "configure_tracing",
    "current_span_id",
    "current_trace_id",
    "extract_context",
    "get_tracer",
    "inject_context",
    "shutdown_tracing",
]
