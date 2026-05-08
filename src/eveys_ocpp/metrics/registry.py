"""All Prometheus metric definitions for the gateway, in one place.

The metrics module is imported once at process start (transitively from
`__main__.py`); every metric is instantiated against the global
`prometheus_client.REGISTRY` at import time. Two reasons to do this
boot-eagerly rather than lazily on first emit:

1. The first /metrics scrape returns a stable schema with every series
   present at zero. Prometheus' `rate()` / `increase()` otherwise treat
   "metric did not exist on the previous scrape" as a counter reset.
2. Duplicate-registration is a runtime error in prometheus-client.
   Boot-time registration means a single import path; nobody can
   accidentally call `Counter("eveys_ocpp_foo", ...)` twice from two
   different modules and crash startup.

Every metric carries the `eveys_ocpp_` prefix per Prometheus naming
conventions. Counters end in `_total`, histograms in `_seconds` (or
`_bytes` where the unit is non-time). Labels are bounded — none of
the metrics here carry `cp_id` because that would be unbounded for a
10k-charger fleet. Use traces (E4-3) for per-charger drill-down.

Two bucket sets partition the histograms:

- `INPROC_BUCKETS` — work that happens inside the asyncio event loop:
  OCPP handler dispatch, gRPC RPCs, DB writes, Redis ops. Median 5 ms,
  p99 budget 200 ms (the OCPP 30 s outer timeout's tight inner slice).
- `OUTBOUND_BUCKETS` — outbound HTTP that talks to the world: backend
  client, webhook delivery. Range from a 10 ms cache hit through the
  30 s OCPP outer timeout.

Series count: 55 metrics. Cardinality stays bounded — see comments at
high-cardinality sites (`measurand`, `vendor_id`, `route`).
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ---- Bucket sets ----------------------------------------------------------

# In-process work (asyncio event loop). 10 buckets, geometric below 50 ms
# then coarse to 1 s to catch outliers without bloating cardinality.
INPROC_BUCKETS: tuple[float, ...] = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
)

# OCPP handler dispatch shares the same SLO as in-process work.
OCPP_HANDLER_BUCKETS: tuple[float, ...] = INPROC_BUCKETS

# Outbound HTTP (backend client, webhook delivery). 11 buckets covering
# OCPP's 30 s outer timeout. Wider distribution than INPROC because
# tail latency depends on network + remote service responsiveness.
OUTBOUND_BUCKETS: tuple[float, ...] = (
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
)

# Kafka publish: faster than outbound HTTP (broker is local-network) but
# slower than purely in-process work. Reuse INPROC; broker-side tail
# spikes show up clearly in the 0.5-1 s buckets without needing extras.
KAFKA_PUBLISH_BUCKETS: tuple[float, ...] = INPROC_BUCKETS


# ---- Build / process info -------------------------------------------------

BUILD_INFO: Gauge = Gauge(
    "eveys_ocpp_build_info",
    "Static build metadata. Set once at boot; the value is always 1 and "
    "the labels carry the version + pod id so a Grafana table panel can "
    "render the running fleet without an extra info_total counter.",
    labelnames=("version", "pod_id"),
)


# ---- WS transport ---------------------------------------------------------

WS_CONNECTIONS_ACTIVE: Gauge = Gauge(
    "eveys_ocpp_ws_connections_active",
    "Charger WebSockets currently held by this pod.",
)
WS_CONNECTS_TOTAL: Counter = Counter(
    "eveys_ocpp_ws_connects_total",
    "Charger WebSocket connect events accepted by this pod.",
)
WS_DISCONNECTS_TOTAL: Counter = Counter(
    "eveys_ocpp_ws_disconnects_total",
    "Charger WebSocket disconnects observed by this pod.",
    labelnames=("reason",),
)
WS_HANDSHAKE_FAILURES_TOTAL: Counter = Counter(
    "eveys_ocpp_ws_handshake_failures_total",
    "WebSocket handshakes the gateway rejected before producing a CP id.",
    labelnames=("reason",),
)
WS_MESSAGES_IN_TOTAL: Counter = Counter(
    "eveys_ocpp_ws_messages_in_total",
    "OCPP CALLs received from chargers, grouped by action.",
    labelnames=("action",),
)
WS_MESSAGES_OUT_TOTAL: Counter = Counter(
    "eveys_ocpp_ws_messages_out_total",
    "OCPP CALLs sent to chargers (CSMS-initiated), grouped by action.",
    labelnames=("action",),
)


# ---- OCPP handlers (cross-cutting) ----------------------------------------

OCPP_HANDLER_LATENCY_SECONDS: Histogram = Histogram(
    "eveys_ocpp_handler_latency_seconds",
    "Inbound OCPP CALL handler wall-clock latency, sliced by action and "
    "outcome. Outcome is one of {ok, replay, fallback, error} so an "
    "alert can fire on a sustained `error` rate without re-summing the "
    "raw histogram.",
    labelnames=("action", "outcome"),
    buckets=OCPP_HANDLER_BUCKETS,
)
OCPP_HANDLER_ERRORS_TOTAL: Counter = Counter(
    "eveys_ocpp_handler_errors_total",
    "Handler invocations that raised, grouped by action and the "
    "exception class name (bounded — handlers raise from a small set "
    "of typed exceptions, never bare `Exception`).",
    labelnames=("action", "error_type"),
)


# ---- Per-handler counters -------------------------------------------------

BOOT_NOTIFICATIONS_TOTAL: Counter = Counter(
    "eveys_ocpp_boot_notifications_total",
    "BootNotifications handled, grouped by the gateway's decision.",
    labelnames=("decision",),  # Accepted / Pending / Rejected
)
BOOT_REPLAYS_TOTAL: Counter = Counter(
    "eveys_ocpp_boot_replays_total",
    "BootNotifications served from the idempotency cache (a charger "
    "retried because our previous response didn't reach it).",
)

AUTHORIZE_TOTAL: Counter = Counter(
    "eveys_ocpp_authorize_total",
    "Authorize decisions, sliced by outcome and where the answer came "
    "from. `source=cache` covers the per-pod authorize cache (E3-4); "
    "`source=backend` is a live backend call; `source=offline` is the "
    "fallback policy when the backend's circuit is open.",
    labelnames=("decision", "source"),
)
AUTHORIZE_CACHE_HITS_TOTAL: Counter = Counter(
    "eveys_ocpp_authorize_cache_hits_total",
    "Authorize requests served from the per-pod cache.",
)
AUTHORIZE_CACHE_MISSES_TOTAL: Counter = Counter(
    "eveys_ocpp_authorize_cache_misses_total",
    "Authorize requests not served from cache (forced a backend call).",
)

START_TRANSACTIONS_TOTAL: Counter = Counter(
    "eveys_ocpp_start_transactions_total",
    "StartTransaction outcomes, grouped by the IdTagInfo decision the "
    "gateway returned to the charger.",
    labelnames=("decision",),
)

STOP_TRANSACTIONS_TOTAL: Counter = Counter(
    "eveys_ocpp_stop_transactions_total",
    "StopTransaction events, grouped by reported reason. Bounded — "
    "OCPP 1.6 § 6.10 fixes the `Reason` enum.",
    labelnames=("reason",),
)
STOP_TRANSACTION_REPLAYS_TOTAL: Counter = Counter(
    "eveys_ocpp_stop_transaction_replays_total",
    "StopTransactions served from the idempotency cache (E2-11). A non-"
    "zero rate is normal under flaky charger networks.",
)

HEARTBEATS_TOTAL: Counter = Counter(
    "eveys_ocpp_heartbeats_total",
    "Heartbeat CALLs handled.",
)
HEARTBEAT_REGISTRY_RECLAIMS_TOTAL: Counter = Counter(
    "eveys_ocpp_heartbeat_registry_reclaims_total",
    "Heartbeats that re-asserted a charger as online after it was "
    "marked offline (e.g. brief WS drop without a clean disconnect).",
)

STATUS_NOTIFICATIONS_TOTAL: Counter = Counter(
    "eveys_ocpp_status_notifications_total",
    "StatusNotification CALLs, sliced by the OCPP-spec status string "
    "and error_code (both bounded by the spec). High-cardinality "
    "vendor extensions are not emitted as labels here — they go into "
    "the Kafka `cp.status` envelope only.",
    labelnames=("status", "error_code"),
)

METER_VALUES_TOTAL: Counter = Counter(
    "eveys_ocpp_meter_values_total",
    "MeterValues CALLs handled (one event per charger report, regardless "
    "of how many sampled values it carries).",
)
METER_VALUE_SAMPLES_TOTAL: Counter = Counter(
    "eveys_ocpp_meter_value_samples_total",
    "Individual sampled values flattened across MeterValues reports, "
    "grouped by measurand. Bounded — OCPP § 7.20 fixes the measurand "
    "enum.",
    labelnames=("measurand",),
)
METER_VALUE_QUARANTINED_TOTAL: Counter = Counter(
    "eveys_ocpp_meter_value_quarantined_total",
    "Sampled values dropped by the E5-4 sanity validator. `reason` is a "
    "closed enum (out_of_range, unparseable, not_finite); cardinality "
    "stays bounded. A non-zero rate here indicates a buggy charger, a "
    "sensor wedge, or an attacker probing the system — alert on it.",
    labelnames=("measurand", "reason"),
)
RATE_LIMIT_THROTTLED_TOTAL: Counter = Counter(
    "eveys_ocpp_rate_limit_throttled_total",
    "Inbound OCPP CALLs dropped by the per-charger rate limiter (E5-3). "
    "`action` is the OCPP action name (bounded by the OCPP enum). A "
    "non-zero rate identifies misbehaving or compromised chargers; the "
    "matching cp_id is in the structured log line, not a label, to "
    "keep cardinality bounded.",
    labelnames=("action",),
)
WS_BASIC_AUTH_TOTAL: Counter = Counter(
    "eveys_ocpp_ws_basic_auth_total",
    "WS-edge Basic Auth verification outcomes (E5-6). `outcome` is a "
    "closed enum: ok, no_header, malformed, username_mismatch, "
    "no_credential, bad_password. Sustained non-`ok` rate at a stable "
    "cp_id (visible in the structured log line, not a label) is a "
    "credential-rotation incident or an active probe.",
    labelnames=("outcome",),
)

DATA_TRANSFERS_TOTAL: Counter = Counter(
    "eveys_ocpp_data_transfers_total",
    "DataTransfer CALLs grouped by vendor_id (bounded — handful of "
    "OCPP vendors with vendor extensions) and our response status.",
    labelnames=("vendor_id", "status"),
)

FIRMWARE_STATUS_TOTAL: Counter = Counter(
    "eveys_ocpp_firmware_status_total",
    "FirmwareStatusNotification CALLs grouped by the OCPP-spec status "
    "(Idle / Downloading / Downloaded / Installing / Installed / "
    "DownloadFailed / InstallationFailed).",
    labelnames=("status",),
)
DIAGNOSTICS_STATUS_TOTAL: Counter = Counter(
    "eveys_ocpp_diagnostics_status_total",
    "DiagnosticsStatusNotification CALLs grouped by status "
    "(Idle / Uploading / Uploaded / UploadFailed).",
    labelnames=("status",),
)
SECURITY_EVENTS_TOTAL: Counter = Counter(
    "eveys_ocpp_security_events_total",
    "SecurityEventNotification CALLs grouped by event type "
    "(OCPP 1.6 Security Whitepaper §4: FirmwareUpdated, "
    "InvalidFirmwareSignature, InvalidSecurityEventCertificate, "
    "etc.). Operators use this for SIEM-style alerting; the "
    "type label is bounded by the spec's 18-value enum + any "
    "vendor extensions, so cardinality stays small.",
    labelnames=("event_type",),
)


# ---- gRPC server ----------------------------------------------------------

GRPC_REQUESTS_TOTAL: Counter = Counter(
    "eveys_ocpp_grpc_requests_total",
    "Inbound gRPC RPCs from the backend, grouped by RPC name and the "
    "OCPP-side response code (`Accepted` / `Rejected` / etc.).",
    labelnames=("rpc", "code"),
)
GRPC_REQUEST_LATENCY_SECONDS: Histogram = Histogram(
    "eveys_ocpp_grpc_request_latency_seconds",
    "Wall-clock latency for an inbound gRPC RPC, including the OCPP CALL roundtrip to the charger.",
    labelnames=("rpc", "code"),
    buckets=OUTBOUND_BUCKETS,  # includes charger response time, can be slow
)
GRPC_DISPATCH_ROUTE_TOTAL: Counter = Counter(
    "eveys_ocpp_grpc_dispatch_route_total",
    "Where each gRPC request was dispatched: `local` (this pod owned "
    "the WS), `bus` (another pod owned it; we forwarded over Redis), "
    "`offline` (no pod owned it; rejected).",
    labelnames=("rpc", "route"),
)
GRPC_CHARGER_OFFLINE_TOTAL: Counter = Counter(
    "eveys_ocpp_grpc_charger_offline_total",
    "gRPC requests that hit a charger no pod claims; the gateway "
    "returns the canonical CHARGER_OFFLINE response.",
    labelnames=("rpc",),
)


# ---- Cross-pod command bus (ADR-0016) -------------------------------------

BUS_REQUESTS_TOTAL: Counter = Counter(
    "eveys_ocpp_bus_requests_total",
    "Cross-pod bus dispatches, grouped by RPC and outcome (`ok` / `timeout` / `error`).",
    labelnames=("rpc", "outcome"),
)
BUS_REQUEST_LATENCY_SECONDS: Histogram = Histogram(
    "eveys_ocpp_bus_request_latency_seconds",
    "End-to-end latency of a bus-routed request: this pod publishes "
    "to Redis pub/sub, the owning pod runs the OCPP CALL, response "
    "comes back. Includes the charger roundtrip.",
    labelnames=("rpc",),
    buckets=OUTBOUND_BUCKETS,
)
BUS_INFLIGHT: Gauge = Gauge(
    "eveys_ocpp_bus_inflight",
    "Bus requests currently awaiting a reply on this pod.",
)


# ---- Kafka producer (ADR-0019) --------------------------------------------

KAFKA_PUBLISH_TOTAL: Counter = Counter(
    "eveys_ocpp_kafka_publish_total",
    "Envelopes published to Kafka, grouped by topic and outcome.",
    labelnames=("topic", "outcome"),
)
KAFKA_PUBLISH_LATENCY_SECONDS: Histogram = Histogram(
    "eveys_ocpp_kafka_publish_latency_seconds",
    "Time from `produce()` call to broker ack, per topic.",
    labelnames=("topic",),
    buckets=KAFKA_PUBLISH_BUCKETS,
)
KAFKA_PUBLISH_BYTES_TOTAL: Counter = Counter(
    "eveys_ocpp_kafka_publish_bytes_total",
    "Bytes successfully written to Kafka, per topic. Useful as a "
    "sanity check against the broker's own ingestion metrics.",
    labelnames=("topic",),
)


# ---- Webhooks (E3-9) ------------------------------------------------------

WEBHOOK_DELIVERIES_TOTAL: Counter = Counter(
    "eveys_ocpp_webhook_deliveries_total",
    "Webhook delivery final outcomes (one count per envelope after "
    "all retries are exhausted), grouped by event_type and outcome "
    "(`delivered` / `rejected` / `failed`). `rejected` means a 4xx "
    "from the backend; `failed` means the retry budget was exhausted.",
    labelnames=("event_type", "outcome"),
)
WEBHOOK_DELIVERY_LATENCY_SECONDS: Histogram = Histogram(
    "eveys_ocpp_webhook_delivery_latency_seconds",
    "Per-attempt webhook HTTP latency, per event_type.",
    labelnames=("event_type",),
    buckets=OUTBOUND_BUCKETS,
)
WEBHOOK_ATTEMPTS_TOTAL: Counter = Counter(
    "eveys_ocpp_webhook_attempts_total",
    "Total per-attempt webhook calls (counts retries individually). "
    "Compare with `_deliveries_total` to compute average attempts per "
    "envelope.",
    labelnames=("event_type",),
)
WEBHOOK_CONSUMER_LAG_MESSAGES: Gauge = Gauge(
    "eveys_ocpp_webhook_consumer_lag_messages",
    "Per-partition Kafka lag for the webhook dispatcher's consumer "
    "group. Expensive to track precisely; sampled on every poll loop.",
    labelnames=("topic", "partition"),
)


# ---- Backend HTTP client (ADR-0023) ---------------------------------------

BACKEND_REQUESTS_TOTAL: Counter = Counter(
    "eveys_ocpp_backend_requests_total",
    "Outbound HTTP requests to the backend, grouped by endpoint name "
    "(authorize / sessions_open / sessions_close / charge_points_register "
    "/ health) and outcome.",
    labelnames=("endpoint", "outcome"),
)
BACKEND_REQUEST_LATENCY_SECONDS: Histogram = Histogram(
    "eveys_ocpp_backend_request_latency_seconds",
    "Outbound backend HTTP latency.",
    labelnames=("endpoint", "outcome"),
    buckets=OUTBOUND_BUCKETS,
)
BACKEND_RETRIES_TOTAL: Counter = Counter(
    "eveys_ocpp_backend_retries_total",
    "Per-endpoint retry counter. Each retry attempt that's not the "
    "first call increments; helps spot a flaky backend without "
    "deriving it from `_requests_total - _deliveries_total`.",
    labelnames=("endpoint",),
)
BACKEND_CIRCUIT_STATE: Gauge = Gauge(
    "eveys_ocpp_backend_circuit_state",
    "Current circuit-breaker state per breaker: 0=closed, 1=half_open, "
    "2=open. Set on every transition; the value always reflects the "
    "live state.",
    labelnames=("name",),
)
BACKEND_CIRCUIT_TRANSITIONS_TOTAL: Counter = Counter(
    "eveys_ocpp_backend_circuit_transitions_total",
    "Circuit-breaker state transitions, grouped by breaker name and "
    "destination state. Differentiates a flapping breaker from a "
    "long-stuck-open one.",
    labelnames=("name", "to_state"),
)


# ---- Idempotency cache (E2-11) --------------------------------------------

IDEMPOTENCY_LOOKUPS_TOTAL: Counter = Counter(
    "eveys_ocpp_idempotency_lookups_total",
    "Idempotency-cache reads grouped by outcome (`miss` / `replay` / `error`).",
    labelnames=("outcome",),
)


# ---- Postgres -------------------------------------------------------------

DB_QUERY_LATENCY_SECONDS: Histogram = Histogram(
    "eveys_ocpp_db_query_latency_seconds",
    "Per-operation Postgres query latency. `op` is one of "
    "{select, insert, update, upsert} — coarse-grained because "
    "per-statement labels would explode cardinality.",
    labelnames=("op",),
    buckets=INPROC_BUCKETS,
)
DB_POOL_IN_USE: Gauge = Gauge(
    "eveys_ocpp_db_pool_in_use",
    "Sessions checked out from the SQLAlchemy async pool right now.",
)
DB_POOL_OVERFLOW: Gauge = Gauge(
    "eveys_ocpp_db_pool_overflow",
    "Sessions allocated above the pool's configured size (overflow "
    "slots in use). Sustained non-zero indicates the pool is "
    "undersized for the workload.",
)


# ---- Redis (online registry + bus) ----------------------------------------

REDIS_COMMAND_LATENCY_SECONDS: Histogram = Histogram(
    "eveys_ocpp_redis_command_latency_seconds",
    "Redis command latency, grouped by op (`get` / `set` / `del` / "
    "`expire` / `publish` / `subscribe`).",
    labelnames=("op",),
    buckets=INPROC_BUCKETS,
)


# ---- Online registry (ADR-0016) -------------------------------------------

REGISTRY_ONLINE_CHARGERS: Gauge = Gauge(
    "eveys_ocpp_registry_online_chargers",
    "Chargers this pod currently owns according to the local view of "
    "the online registry. Per-pod-correct, not authoritative across "
    "the cluster (Redis owns the cluster-wide truth). Useful for "
    "fleet-wide rollups via `sum()` across pods.",
)


# ---- REST server (ADR-0026) -----------------------------------------------

REST_REQUESTS_TOTAL: Counter = Counter(
    "eveys_ocpp_rest_requests_total",
    "Inbound REST requests grouped by HTTP method, FastAPI route "
    "template, and response code. The route label is the route "
    "TEMPLATE (e.g. `/api/v1/charge-points/{cp_id}`), never the "
    "literal path — that would make cp_id-keyed traffic blow up "
    "label cardinality.",
    labelnames=("method", "route", "code"),
)
REST_REQUEST_LATENCY_SECONDS: Histogram = Histogram(
    "eveys_ocpp_rest_request_latency_seconds",
    "Inbound REST request latency, by route template.",
    labelnames=("method", "route"),
    buckets=INPROC_BUCKETS,
)


__all__ = [
    "AUTHORIZE_CACHE_HITS_TOTAL",
    "AUTHORIZE_CACHE_MISSES_TOTAL",
    "AUTHORIZE_TOTAL",
    "BACKEND_CIRCUIT_STATE",
    "BACKEND_CIRCUIT_TRANSITIONS_TOTAL",
    "BACKEND_REQUESTS_TOTAL",
    "BACKEND_REQUEST_LATENCY_SECONDS",
    "BACKEND_RETRIES_TOTAL",
    "BOOT_NOTIFICATIONS_TOTAL",
    "BOOT_REPLAYS_TOTAL",
    "BUILD_INFO",
    "BUS_INFLIGHT",
    "BUS_REQUESTS_TOTAL",
    "BUS_REQUEST_LATENCY_SECONDS",
    "DATA_TRANSFERS_TOTAL",
    "DB_POOL_IN_USE",
    "DB_POOL_OVERFLOW",
    "DB_QUERY_LATENCY_SECONDS",
    "DIAGNOSTICS_STATUS_TOTAL",
    "FIRMWARE_STATUS_TOTAL",
    "GRPC_CHARGER_OFFLINE_TOTAL",
    "GRPC_DISPATCH_ROUTE_TOTAL",
    "GRPC_REQUESTS_TOTAL",
    "GRPC_REQUEST_LATENCY_SECONDS",
    "HEARTBEATS_TOTAL",
    "HEARTBEAT_REGISTRY_RECLAIMS_TOTAL",
    "IDEMPOTENCY_LOOKUPS_TOTAL",
    "INPROC_BUCKETS",
    "KAFKA_PUBLISH_BUCKETS",
    "KAFKA_PUBLISH_BYTES_TOTAL",
    "KAFKA_PUBLISH_LATENCY_SECONDS",
    "KAFKA_PUBLISH_TOTAL",
    "METER_VALUES_TOTAL",
    "METER_VALUE_QUARANTINED_TOTAL",
    "METER_VALUE_SAMPLES_TOTAL",
    "OCPP_HANDLER_BUCKETS",
    "OCPP_HANDLER_ERRORS_TOTAL",
    "OCPP_HANDLER_LATENCY_SECONDS",
    "OUTBOUND_BUCKETS",
    "RATE_LIMIT_THROTTLED_TOTAL",
    "REDIS_COMMAND_LATENCY_SECONDS",
    "REGISTRY_ONLINE_CHARGERS",
    "REST_REQUESTS_TOTAL",
    "REST_REQUEST_LATENCY_SECONDS",
    "START_TRANSACTIONS_TOTAL",
    "STATUS_NOTIFICATIONS_TOTAL",
    "STOP_TRANSACTIONS_TOTAL",
    "STOP_TRANSACTION_REPLAYS_TOTAL",
    "WEBHOOK_ATTEMPTS_TOTAL",
    "WEBHOOK_CONSUMER_LAG_MESSAGES",
    "WEBHOOK_DELIVERIES_TOTAL",
    "WEBHOOK_DELIVERY_LATENCY_SECONDS",
    "WS_BASIC_AUTH_TOTAL",
    "WS_CONNECTIONS_ACTIVE",
    "WS_CONNECTS_TOTAL",
    "WS_DISCONNECTS_TOTAL",
    "WS_HANDSHAKE_FAILURES_TOTAL",
    "WS_MESSAGES_IN_TOTAL",
    "WS_MESSAGES_OUT_TOTAL",
]
