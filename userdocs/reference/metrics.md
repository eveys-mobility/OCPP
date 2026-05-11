# Metrics reference

**Use this if you** are building dashboards, writing alerts, or investigating latency or error spikes.

**Audience.** SREs and observability engineers.

**What this answers.** Every Prometheus series the gateway exposes, what its labels mean, what "normal" looks like, and what a sudden change implies.

> The metrics surface is `/metrics` on port `9100`. Scrape it with Prometheus, build dashboards with Grafana. Sample dashboards are shipped under `deploy/grafana/dashboards/` in the repo.

---

## Naming

All gateway metrics are prefixed `eveys_ocpp_`. Histograms produce three series per name (`_bucket`, `_count`, `_sum`); counters produce `<name>_total`; gauges produce `<name>` directly.

---

## The signals that matter most

If you build only four panels, build these.

### `eveys_ocpp_ws_connections_active` (gauge)

How many charger WebSocket sockets are open on this pod right now.

- **Labels**: none.
- **Sum it across pods** for fleet total.
- **Watch for**: cliffs (network event), sudden growth (reconnect storm — usually after the gateway itself was unhealthy).

### `eveys_ocpp_ocpp_handler_latency_seconds` (histogram)

Inbound OCPP CALL handling latency, per action.

- **Labels**: `action` (`BootNotification`, `Authorize`, `StartTransaction`, ...).
- **p99 spikes**: usually Postgres slowness. Check `eveys_ocpp_db_query_latency_seconds` next.
- **p50 + p99 widening**: handler doing more work — recent change?

### `eveys_ocpp_grpc_request_latency_seconds` (histogram)

Outbound dispatch latency. The full round-trip from "your REST call landed" to "the charger answered".

- **Labels**: `rpc` (`RemoteStart`, etc.).
- **p99 dominated by**: the WS round-trip + the charger's own latency. Cross-pod hops add a few milliseconds.

### `eveys_ocpp_ocpp_handler_errors_total` (counter)

Per-action error count.

- **Labels**: `action`.
- **Trend matters more than absolute.** A handful per day is normal at fleet scale; rate climbing is the signal.

---

## Full catalogue (by area)

### WebSocket

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `eveys_ocpp_ws_connections_active` | gauge | — | Sockets open right now. |
| `eveys_ocpp_ws_connects_total` | counter | — | Cumulative connect count. |
| `eveys_ocpp_ws_disconnects_total` | counter | `reason` | Cumulative disconnect count. |
| `eveys_ocpp_ws_handshake_failures_total` | counter | `reason` | TLS / subprotocol / auth handshake failures. |
| `eveys_ocpp_ws_messages_in_total` | counter | `action` | OCPP CALLs received from chargers. |
| `eveys_ocpp_ws_messages_out_total` | counter | `action` | OCPP CALLs sent to chargers. |
| `eveys_ocpp_ws_basic_auth_total` | counter | `outcome` | Basic Auth pass/fail counts. |

### OCPP handlers (inbound)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `eveys_ocpp_ocpp_handler_latency_seconds` | histogram | `action` | End-to-end handler time. |
| `eveys_ocpp_ocpp_handler_errors_total` | counter | `action`, `exception` | Handler raised. |
| `eveys_ocpp_boot_notifications_total` | counter | `status` | BootNotification by reply status (Accepted / Pending / Rejected). |
| `eveys_ocpp_boot_replays_total` | counter | — | Replays of a prior BootNotification deduped by the idempotency cache. |
| `eveys_ocpp_authorize_total` | counter | `outcome` | Authorize results (Accepted/Blocked/Expired/Invalid/ConcurrentTx). |
| `eveys_ocpp_authorize_cache_hits_total` | counter | — | Redis Authorize cache hits. |
| `eveys_ocpp_authorize_cache_misses_total` | counter | — | Cache misses (fell through to backend). |
| `eveys_ocpp_start_transactions_total` | counter | — | StartTransaction persisted. |
| `eveys_ocpp_stop_transactions_received_total` | counter | — | StopTransaction received (denominator for SLO 4). |
| `eveys_ocpp_stop_transactions_total` | counter | — | StopTransaction persisted (numerator). |
| `eveys_ocpp_stop_transaction_replays_total` | counter | — | Deduped replays. |
| `eveys_ocpp_heartbeats_total` | counter | — | Heartbeats. |
| `eveys_ocpp_heartbeat_registry_reclaims_total` | counter | — | Charger heartbeat seen on a different pod than the registry said. |
| `eveys_ocpp_status_notifications_total` | counter | `status`, `error_code` | Charger state changes. |
| `eveys_ocpp_meter_values_total` | counter | — | MeterValues messages received. |
| `eveys_ocpp_meter_value_samples_total` | counter | `measurand` | Individual samples within messages. |
| `eveys_ocpp_meter_value_quarantined_total` | counter | `reason` | Samples rejected by sanity checks. |
| `eveys_ocpp_rate_limit_throttled_total` | counter | `surface` | Throttled requests (WS / REST). |
| `eveys_ocpp_data_transfers_total` | counter | `vendor_id`, `status` | DataTransfer counts. |
| `eveys_ocpp_firmware_status_total` | counter | `status` | FirmwareStatusNotification. |
| `eveys_ocpp_diagnostics_status_total` | counter | `status` | DiagnosticsStatusNotification. |
| `eveys_ocpp_sign_certificate_received_total` | counter | `outcome` | SignCertificate (CSRs from chargers). |
| `eveys_ocpp_security_events_total` | counter | `type` | SecurityEventNotification. |
| `eveys_ocpp_log_status_total` | counter | `status` | LogStatusNotification. |

### Outbound dispatch (gRPC + REST → charger)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `eveys_ocpp_grpc_requests_total` | counter | `rpc`, `outcome` | gRPC request count. |
| `eveys_ocpp_grpc_request_latency_seconds` | histogram | `rpc` | gRPC end-to-end latency. |
| `eveys_ocpp_grpc_dispatch_route_total` | counter | `route` | Same-pod vs cross-pod dispatch counts. |
| `eveys_ocpp_grpc_charger_offline_total` | counter | `rpc` | Dispatches that found the charger offline. |
| `eveys_ocpp_bus_requests_total` | counter | `outcome` | Cross-pod bus requests. |
| `eveys_ocpp_bus_request_latency_seconds` | histogram | — | Cross-pod hop latency. |
| `eveys_ocpp_bus_inflight` | gauge | — | Currently waiting on bus responses. |

### Kafka producer

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `eveys_ocpp_kafka_publish_total` | counter | `topic`, `outcome` | Publish attempts. |
| `eveys_ocpp_kafka_publish_latency_seconds` | histogram | `topic` | Publish latency. |
| `eveys_ocpp_kafka_publish_bytes_total` | counter | `topic` | Bytes published. |

### Webhooks

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `eveys_ocpp_webhook_deliveries_total` | counter | `event`, `outcome` | Final delivery outcomes (`success`, `failed`). |
| `eveys_ocpp_webhook_delivery_latency_seconds` | histogram | `event` | End-to-end delivery latency. |
| `eveys_ocpp_webhook_attempts_total` | counter | `event`, `outcome` | All delivery attempts including retries. |
| `eveys_ocpp_webhook_consumer_lag_messages` | gauge | `topic` | Backlog in the internal webhook consumer. |

### Backend integration

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `eveys_ocpp_backend_requests_total` | counter | `endpoint`, `outcome` | Calls to your backend. |
| `eveys_ocpp_backend_request_latency_seconds` | histogram | `endpoint` | Backend latency from the gateway's view. |
| `eveys_ocpp_backend_retries_total` | counter | `endpoint` | Backend retries. |
| `eveys_ocpp_backend_circuit_state` | gauge | `endpoint` | Circuit breaker state (0=closed, 1=half-open, 2=open). |
| `eveys_ocpp_backend_circuit_transitions_total` | counter | `endpoint`, `from`, `to` | State transitions. |

### Persistence

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `eveys_ocpp_db_query_latency_seconds` | histogram | `query` | Postgres query time. |
| `eveys_ocpp_db_pool_in_use` | gauge | — | Connections currently checked out. |
| `eveys_ocpp_db_pool_overflow` | gauge | — | Overflow connections in use. |
| `eveys_ocpp_redis_command_latency_seconds` | histogram | `command` | Redis command latency. |
| `eveys_ocpp_registry_online_chargers` | gauge | — | Chargers the gateway thinks are online. |

### Idempotency

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `eveys_ocpp_idempotency_lookups_total` | counter | `outcome` | `hit` (deduped) vs `miss` (new). |

### REST surface

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `eveys_ocpp_rest_requests_total` | counter | `path`, `method`, `status` | Inbound REST requests. |
| `eveys_ocpp_rest_request_latency_seconds` | histogram | `path`, `method` | Inbound REST latency. |

### Build info

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `eveys_ocpp_build_info` | gauge (value=1) | `version`, `commit`, `python_version` | Released version metadata. |

---

## Reading histograms

Every histogram exposes a default bucket layout sized to the surface it covers (sub-millisecond to multi-second). To get a percentile in PromQL:

```promql
histogram_quantile(0.99,
  sum by (le, rpc) (
    rate(eveys_ocpp_grpc_request_latency_seconds_bucket[5m])
  )
)
```

p99 is the most actionable percentile for the gateway — p50 hides the long tail that matters when chargers think a command failed.

---

## SLOs the project tracks internally

The dashboards in `deploy/grafana/dashboards/06-slos.json` include the SLOs the gateway is engineered against. The four you'll care about:

| SLO | Definition | What breaks it |
|---|---|---|
| **1. WS availability** | (connects + maintained) / (connects + maintained + failed handshakes) | TLS, auth, capacity issues. |
| **2. Command success** | `RemoteStart` Accepted / total RemoteStart attempts | Charger offline, charger rejecting valid commands. |
| **3. Event delivery** | Successful Kafka publish / total publish attempts | Broker outage, partition leader churn. |
| **4. Transaction durability** | `StopTransaction` persisted / received | Postgres outage or handler bug. |

---

## Alert ideas (no specific thresholds; these are starting points)

- 5xx rate on REST sustained > 1% for 5 minutes.
- `eveys_ocpp_ws_connections_active` drops > 20% over 1 minute (sudden disconnect).
- `eveys_ocpp_handler_errors_total{action="StopTransaction"}` rate > 0 for 1 minute.
- p99 of `eveys_ocpp_ocpp_handler_latency_seconds{action="MeterValues"}` > 500 ms.
- `eveys_ocpp_db_pool_in_use / db_pool_max` > 0.9 for 5 minutes.
- `eveys_ocpp_backend_circuit_state > 0` for any backend endpoint.

Tune to your fleet's baseline — the right thresholds depend on traffic shape.

---

## Where to go from here

- Operating from these metrics: [`../guides/operate.md`](../guides/operate.md).
- The events these metrics count: [`events.md`](./events.md).
- Configuration that affects what gets emitted: [`configuration.md`](./configuration.md).
