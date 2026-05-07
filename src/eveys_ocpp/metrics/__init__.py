"""Prometheus metrics for the gateway (Phase 4 / E4-1).

Public surface:

- `MetricsServer` — boot/shutdown the scrape endpoint.
- `registry` — module-level globals for every metric (Counter / Gauge
  / Histogram). Import as `from eveys_ocpp.metrics import registry as m`
  and emit via `m.WS_CONNECTS_TOTAL.inc()` etc.
- `time_handler`, `time_outbound`, `record_handler_error` — small
  helpers in `instrumentation` for the two most-repeated patterns.

All metrics live in the global `prometheus_client.REGISTRY` and are
defined at import time so `/metrics` returns a stable schema from the
first scrape.
"""

from __future__ import annotations

from eveys_ocpp.metrics import registry
from eveys_ocpp.metrics.instrumentation import (
    record_handler_error,
    time_handler,
    time_outbound,
)
from eveys_ocpp.metrics.server import MetricsServer

__all__ = [
    "MetricsServer",
    "record_handler_error",
    "registry",
    "time_handler",
    "time_outbound",
]
