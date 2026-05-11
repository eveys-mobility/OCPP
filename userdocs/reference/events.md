# Events reference

**Use this if you** are subscribing to charger events over Kafka, webhooks, or both.

**Audience.** A backend or data developer wiring up consumers.

**What this answers.** Every event type, its envelope shape, partitioning, idempotency guarantees, and which transports it lands on.

> Integration patterns (when to pick Kafka vs webhooks, how to verify HMAC signatures, how to be idempotent) are in [`../guides/consume-events.md`](../guides/consume-events.md). This page is the catalogue.

---

## The envelope

Every event — over Kafka or webhooks — uses the same protobuf envelope. The proto file lives at `proto/events/v1/events.proto`:

```protobuf
message EventEnvelope {
  string event_id      = 1;   // UUID, unique per event
  string occurred_at   = 2;   // ISO-8601 server-receive timestamp (UTC)
  string cp_id         = 3;   // charger the event is about
  string schema_version = 4;  // "v1"
  string trace_id      = 5;   // optional, for distributed tracing

  oneof payload {
    CpConnected             cp_connected             = 100;
    CpBoot                  cp_boot                  = 101;
    CpStatus                cp_status                = 102;
    CpMeter                 cp_meter                 = 103;
    TxStarted               tx_started               = 104;
    CpSecurityEvent         cp_security_event        = 105;
    TxStopped               tx_stopped               = 106;
    CpDisconnected          cp_disconnected          = 107;
    CpFirmwareStatusChanged cp_firmware_status_changed = 108;
    CpDiagnosticsStatusChanged cp_diagnostics_status_changed = 109;
    CpCsrSubmitted          cp_csr_submitted         = 110;
  }
}
```

Generate consumer bindings from the proto file in your language. The `WhichOneof("payload")` discriminator tells you which payload variant is set.

---

## Topic / event catalogue

| Topic (default) | Payload | Direction | Volume | Webhook URL setting |
|---|---|---|---|---|
| `cp.connected` | `CpConnected` | WS established | per connect | `EVEYS_OCPP_WEBHOOK_URL_CP_ONLINE` |
| `cp.disconnected` | `CpDisconnected` | WS ended | per disconnect | `EVEYS_OCPP_WEBHOOK_URL_CP_OFFLINE` |
| `cp.boot` | `CpBoot` | `BootNotification` arrived | rare; on charger boot | `EVEYS_OCPP_WEBHOOK_URL_CP_BOOT` |
| `cp.status` | `CpStatus` | `StatusNotification` arrived | bursts on state change | `EVEYS_OCPP_WEBHOOK_URL_CP_STATUS` |
| `cp.meter` | `CpMeter` | `MeterValues` arrived | high; per sample per active session | `EVEYS_OCPP_WEBHOOK_URL_CP_METER` (disabled by default; use Kafka) |
| `cp.firmware_status` | `CpFirmwareStatusChanged` | `FirmwareStatusNotification` arrived | low; firmware lifecycle | `EVEYS_OCPP_WEBHOOK_URL_CP_FIRMWARE_STATUS` |
| `cp.diagnostics_status` | `CpDiagnosticsStatusChanged` | `DiagnosticsStatusNotification` arrived | low; diagnostics lifecycle | `EVEYS_OCPP_WEBHOOK_URL_CP_DIAGNOSTICS_STATUS` |
| `cp.security_event` | `CpSecurityEvent` | `SecurityEventNotification` arrived | sparse; audit-grade | (Kafka only; SIEM pipeline) |
| `cp.csr_submitted` | `CpCsrSubmitted` | `SignCertificate` arrived | sparse; cert-rotation cycles | (Kafka only; operator queue) |
| `tx.started` | `TxStarted` | `StartTransaction` persisted | one per session start | `EVEYS_OCPP_WEBHOOK_URL_TX_STARTED` |
| `tx.stopped` | `TxStopped` | `StopTransaction` persisted | one per session end | `EVEYS_OCPP_WEBHOOK_URL_TX_STOPPED` |
| `cp.credential_rotated` | `CpCredentialRotated` | operator changed a charger credential (TC_073) | sparse; audit-grade | (Kafka only; SIEM pipeline) |

Topic names are configurable; the table shows defaults. Rename only when your platform's naming convention demands it — every consumer detaches.

---

## Per-payload reference

### `CpConnected` — WebSocket opened

```protobuf
message CpConnected {
  string subprotocol = 1;   // negotiated: "ocpp1.6" or "ocpp2.0.1"
  string pod_id      = 2;   // gateway pod that accepted the connection
}
```

Published right after the gateway marks the charger online in the registry. **Webhook-equivalent: `cp.online`** (the historical name).

### `CpDisconnected` — WebSocket closed

```protobuf
message CpDisconnected {
  string pod_id = 1;
  string reason = 2;   // "clean" or "error"
}
```

Published only when the registry's compare-and-delete confirms this pod still owned the key — a reconnect-to-different-pod race does **not** produce a spurious offline event.

### `CpBoot` — `BootNotification` arrived

```protobuf
message CpBoot {
  string vendor           = 1;
  string model            = 2;
  string firmware_version = 3;
  string serial_number    = 4;
  CpBootStatus status     = 5;   // ACCEPTED / PENDING / REJECTED
}
```

### `CpStatus` — `StatusNotification` arrived

```protobuf
message CpStatus {
  int32  connector_id        = 1;
  string status              = 2;   // OCPP-spec string: Available, Charging, ...
  string error_code          = 3;   // OCPP-spec string: NoError, ConnectorLockFailure, ...
  string info                = 4;
  string vendor_id           = 5;
  string vendor_error_code   = 6;
  string charger_reported_at = 7;   // charger's own clock; untrusted
}
```

### `CpMeter` — `MeterValues` arrived

```protobuf
message CpMeter {
  int32  connector_id        = 1;
  int64  transaction_id      = 2;
  repeated SampledValue sampled_values = 3;
  string charger_reported_at = 4;
}

message SampledValue {
  string    value     = 1;
  Context   context   = 2;   // enum
  Format    format    = 3;   // enum
  Measurand measurand = 4;   // enum
  Phase     phase     = 5;   // enum
  Location  location  = 6;   // enum
  Unit      unit      = 7;   // enum
}
```

The enums encode the OCPP-spec wire strings (`MEASURAND_ENERGY_ACTIVE_IMPORT_REGISTER`, `MEASURAND_SOC`, etc.). High volume — every active session emits these on the sampling interval.

### `TxStarted` — `StartTransaction` persisted

```protobuf
message TxStarted {
  int64  transaction_id      = 1;
  int32  connector_id        = 2;
  string id_tag              = 3;
  int64  meter_start_wh      = 4;
  string charger_reported_at = 5;
}
```

Published **after** the gateway commits to Postgres. Replays of the same `StartTransaction` (which OCPP allows) do not re-emit.

### `TxStopped` — `StopTransaction` persisted

```protobuf
message TxStopped {
  int64  transaction_id      = 1;
  string id_tag              = 2;
  int64  meter_stop_wh       = 3;
  int64  consumed_wh         = 4;   // pre-computed: meter_stop - meter_start
  string stop_reason         = 5;
  string charger_reported_at = 6;
}
```

`consumed_wh` is computed from the matching `TxStarted` so consumers don't have to join. Replays don't re-emit.

### `CpSecurityEvent` — `SecurityEventNotification` arrived

```protobuf
message CpSecurityEvent {
  string type                = 1;   // OCPP type: InvalidSignature, TamperDetectionActivated, ...
  string charger_reported_at = 2;
  string tech_info           = 3;
}
```

Audit-grade. SIEM pipelines tail this.

### `CpFirmwareStatusChanged` / `CpDiagnosticsStatusChanged`

```protobuf
message CpFirmwareStatusChanged   { string status = 1; }
message CpDiagnosticsStatusChanged { string status = 1; }
```

Status is the charger-reported state-machine string — `Downloading`, `Installed`, `UploadFailed`, etc. Kept as a string for forward-compat with vendor extensions.

### `CpCredentialRotated` — operator rotated a charger's Basic Auth (TC_073)

```protobuf
message CpCredentialRotated {
  string action = 1;   // "set" or "removed"
  string actor  = 2;   // opaque operator id; may be empty
}
```

Emitted by `PUT` / `DELETE /api/v1/charge-points/{cp_id}/credentials`. Audit-grade; the password itself is never carried.

### `CpCsrSubmitted` — charger asked us to sign a CSR

```protobuf
message CpCsrSubmitted {
  string csr        = 1;   // PEM-encoded CSR
  int64  pending_id = 2;   // row id in pending_certificate_signings
}
```

The operator queue uses this to route a freshly-arrived CSR into a review UI.

---

## Kafka delivery semantics

- **Producer config.** `acks=all` + `enable.idempotence=true`. The gateway will not silently drop messages under broker churn.
- **Partition key.** `cp_id` on every record. Per-charger ordering is preserved.
- **Schema evolution.** Adding fields is allowed; renumbering or removing fields is forbidden until v2 of the envelope. Consumers **must** ignore unknown fields.
- **At-least-once.** Consumers should idempotent-handle on `event_id`.

---

## Webhook delivery semantics

- **Payload.** The same envelope, JSON-encoded. The `payload` `oneof` becomes a JSON object with a single key naming the variant (e.g. `{"payload": {"tx_started": {...}}}`).
- **Headers.**
  - `Content-Type: application/json`
  - `X-Eveys-Signature: sha256=<lowercase-hex-hmac>` — HMAC-SHA256 of the raw body using `EVEYS_OCPP_WEBHOOK_SECRET`.
  - `X-Eveys-Event-Type: cp.boot` — for routing.
  - `X-Eveys-Event-Id: <uuid>` — same value as the envelope's `event_id`.
  - `X-Eveys-Delivery-Attempt: <n>` — incremented on retries.
- **Success.** Any 2xx. The gateway gives up retrying on 4xx (the body is malformed in your view); retries non-2xx with exponential backoff up to a configurable cap.
- **At-least-once.** Be idempotent on `event_id`.

---

## What's *not* an event

- **Heartbeats.** Absorbed by the online registry; surfaced via REST (`last_heartbeat_at`), not as a Kafka topic.
- **Authorize.** Synchronous; the gateway forwards to your backend's hot-path REST endpoint.
- **Outbound command results** (RemoteStart's reply, Reset's reply). Returned synchronously to the REST/gRPC caller; not re-published.

---

## Version compatibility

`schema_version` is the load-bearing version field on the envelope. `v1` is the current schema; future schema breaks will increment this. Consumers should check the version on every message and refuse to process unknown versions rather than silently mis-interpret.

---

## Where to go from here

- Picking Kafka vs webhooks, verifying HMAC, designing idempotent consumers: [`../guides/consume-events.md`](../guides/consume-events.md).
- Why some events sometimes arrive twice: [`../concepts/idempotency-and-replay.md`](../concepts/idempotency-and-replay.md).
- The synchronous side of the same flow: [`rest-api.md`](./rest-api.md), [`grpc-api.md`](./grpc-api.md).
