"""Schema-shape tests for the metrics registry.

The instrumentation PRs that follow add per-emitter delta-style tests
(`HEARTBEATS_TOTAL` increments when a Heartbeat is handled, etc.). This
file covers the contract of the registry itself:

- Every metric is in the global REGISTRY at import time so `/metrics`
  returns a stable schema from the very first scrape.
- Every metric name uses the `eveys_ocpp_` prefix.
- Counter / Histogram unit suffixes follow Prometheus conventions
  (`_total`, `_seconds`, `_bytes`).
- The total count matches the inventory documented in the agent's
  E4-1 plan (52 metrics).

These tests are deliberately strict on the schema. Adding a new metric
without updating the inventory comment trips them — the goal is to
keep the registry and the docs in lockstep.
"""

from __future__ import annotations

from prometheus_client import REGISTRY, Counter, Gauge, Histogram

from eveys_ocpp.metrics import registry as m

# Inventory total per E4-1 plan. Update both numbers if the inventory
# grows in a future PR.
EXPECTED_METRIC_COUNT = 54


def _gateway_metric_attrs() -> list[tuple[str, object]]:
    """Iterate every public metric attribute on the registry module."""
    out: list[tuple[str, object]] = []
    for name in dir(m):
        if name.startswith("_"):
            continue
        if "BUCKETS" in name:  # bucket constants, not metrics
            continue
        attr = getattr(m, name)
        if isinstance(attr, Counter | Gauge | Histogram):
            out.append((name, attr))
    return out


def test_inventory_size_matches_plan() -> None:
    """If this fails, the registry has drifted from the E4-1 inventory."""
    metrics = _gateway_metric_attrs()
    assert len(metrics) == EXPECTED_METRIC_COUNT, (
        f"expected {EXPECTED_METRIC_COUNT} metrics, found {len(metrics)}: "
        f"{sorted(name for name, _ in metrics)}"
    )


def _exposed_sample_names(metric: object) -> set[str]:
    """Return the wire-format sample names a Prometheus scrape would
    see for this metric.

    `prometheus_client.Counter` strips the `_total` suffix from the
    constructor name into `_name`, then re-appends `_total` on the wire.
    Histograms emit `_bucket`, `_count`, `_sum` per series. The set of
    sample names produced by `collect()` is the canonical "what does
    Prometheus see" view.
    """
    out: set[str] = set()
    for family in metric.collect():  # type: ignore[attr-defined]
        for sample in family.samples:
            out.add(sample.name)
    return out


def _internal_name(metric: object) -> str:
    """Internal-form name (the `_name` attribute prometheus_client stores).
    Used for prefix checks where suffix-stripping doesn't matter."""
    return next(iter(metric.describe())).name  # type: ignore[attr-defined]


def test_every_metric_name_uses_eveys_ocpp_prefix() -> None:
    for name, metric in _gateway_metric_attrs():
        wire_name = _internal_name(metric)
        assert wire_name.startswith("eveys_ocpp_"), (
            f"{name} ({wire_name=!r}) is missing the eveys_ocpp_ prefix"
        )


def test_counter_names_have_total_suffix_in_python_constant() -> None:
    """Prometheus convention is `_total` on monotonic counters.
    `prometheus_client.Counter` enforces this at construction
    (auto-appending if missing) and exposes it on the wire, but only
    once `.labels(...)` has been called for labelled counters. We
    check our Python-level constants instead — the names operators
    grep in alerts and Grafana dashboards.
    """
    for name, metric in _gateway_metric_attrs():
        if not isinstance(metric, Counter):
            continue
        assert name.endswith("_TOTAL"), (
            f"Counter constant {name!r} should end in _TOTAL to make "
            "the wire `_total` suffix obvious at the import site."
        )


def test_histogram_names_end_in_seconds_or_bytes() -> None:
    """Histograms in this registry observe time (seconds) or volume
    (bytes); the unit must show up in the metric base name. Histograms
    expose `<name>_bucket`, `<name>_count`, `<name>_sum` — the base
    name (the internal `_name`) is what carries the unit."""
    for name, metric in _gateway_metric_attrs():
        if not isinstance(metric, Histogram):
            continue
        wire_name = _internal_name(metric)
        assert wire_name.endswith(("_seconds", "_bytes")), (
            f"histogram {name} ({wire_name=!r}) should end in _seconds or _bytes"
        )


def test_every_metric_is_registered_in_global_registry() -> None:
    """Boot-time registration: every metric the registry module exposes
    must already be a collector in `prometheus_client.REGISTRY` so the
    very first /metrics scrape returns a stable schema. Lazy
    registration (on first emit) would look like a counter reset to
    Prometheus.

    We probe the private `_collector_to_names` map because that's the
    only place that lists *registered* collectors regardless of whether
    they've emitted any sample yet (a labelled `Gauge` with no
    `.labels(...).set(...)` call has zero samples but is still in the
    map).
    """
    registered_collectors = set(REGISTRY._collector_to_names.keys())  # type: ignore[attr-defined]
    for name, metric in _gateway_metric_attrs():
        assert metric in registered_collectors, f"{name} is not a collector in the global REGISTRY"


def test_bucket_sets_are_strictly_increasing() -> None:
    """Histogram buckets must be in strictly ascending order, otherwise
    `prometheus_client` raises at construction. The constructor checks
    pass at import time; this test is the explicit guard so a future
    edit to the bucket tuples doesn't accidentally reorder them.
    """
    for name in ("INPROC_BUCKETS", "OUTBOUND_BUCKETS", "KAFKA_PUBLISH_BUCKETS"):
        buckets = getattr(m, name)
        assert tuple(sorted(buckets)) == buckets, f"{name} is not strictly ascending: {buckets}"
        assert len(buckets) == len(set(buckets)), f"{name} has duplicates"
