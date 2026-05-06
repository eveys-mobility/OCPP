# 03 — Webhooks (gateway → backend)

> **Audience**: backend developer wiring the backend to receive event pushes from the OCPP gateway.

These are HTTP POST deliveries the **gateway** sends to **backend-configured URLs** when something happens. Async, fire-and-forget from the OCPP hot path; the gateway retries with exponential backoff on delivery failure.

The same events also flow on Kafka (the `cp.boot` / `cp.status` / `cp.meter` / `tx.started` topics — see ADR-0015 + ADR-0020). If the backend can subscribe to Kafka directly, prefer Kafka — it's higher-throughput, replayable, and durable. Webhooks are the alternative for backends that don't speak Kafka.

Read [README.md](./README.md) first for envelope, auth, signature, correlation conventions.

---

## Configuration

The gateway reads webhook URLs from environment variables. Each event has its own URL so the backend can route them differently (or disable individually):

```bash
EVEYS_OCPP_WEBHOOK_BASE_URL=<eveys_backend_base_url>/api/eveys/webhooks
EVEYS_OCPP_WEBHOOK_SECRET=<shared HMAC secret>

# Per-event URL overrides (optional; default to <base>/<event-name>)
EVEYS_OCPP_WEBHOOK_URL_CP_BOOT=<base>/charge-point-boot
EVEYS_OCPP_WEBHOOK_URL_CP_PRESENCE=<base>/charge-point-presence
EVEYS_OCPP_WEBHOOK_URL_CP_STATUS=<base>/charge-point-status
EVEYS_OCPP_WEBHOOK_URL_CP_METER=<base>/meter-values        # default OFF
EVEYS_OCPP_WEBHOOK_URL_FIRMWARE_STATUS=<base>/firmware-status
EVEYS_OCPP_WEBHOOK_URL_DIAGNOSTICS_STATUS=<base>/diagnostics-status
EVEYS_OCPP_WEBHOOK_URL_TX_STARTED=<base>/transaction-started
EVEYS_OCPP_WEBHOOK_URL_TX_STOPPED=<base>/transaction-stopped

# Per-event toggles (omit / "1" → enabled, "0" → disabled)
EVEYS_OCPP_WEBHOOK_ENABLE_CP_METER=0    # off by default — high volume
EVEYS_OCPP_WEBHOOK_ENABLE_TX_STARTED=1
# ...
```

`cp.meter` is **off by default** because per-charger MeterValues are 1 sample/30s × N chargers — at 10k chargers that's 333 webhooks/s. Either subscribe to Kafka, or query the gateway's [`GET /api/v1/charge-points/{cp_id}/meter-values`](./02-gateway-rest-api.md) endpoint on demand.

---

## Delivery semantics

- **At-least-once.** A delivery may be retried; the backend MUST be idempotent on `X-Eveys-Event-Id`.
- **Retries**: 5 attempts with exponential backoff: 1 s, 5 s, 30 s, 2 min, 10 min. After 5 failures the event is logged as `webhook.delivery_failed` and dropped from the gateway's retry queue (operators can replay via Kafka if subscribed).
- **Per-charger ordering not guaranteed.** Two events from the same charger may arrive out of order. The backend should use `occurred_at` for ordering, not arrival time. Same caveat as Kafka — `cp_id` is the partition key on Kafka, but webhooks are unordered.
- **HTTP status interpretation**:
  - `2xx` — delivered. Gateway considers the event acknowledged.
  - `4xx` (except `429`) — backend rejected (malformed signature, schema mismatch, etc.). Gateway logs and **does not retry** — the backend's bug, not a transient failure.
  - `429` / `5xx` / network timeout → retried per the backoff schedule.

The backend SHOULD respond `200 OK` with an empty body or the standard envelope:

```json
{ "success": true, "data": null, "message": "ok" }
```

---

## Authentication

Every delivery carries an HMAC-SHA-256 signature over the raw request body:

```
X-Eveys-Signature: sha256=<lowercase hex>
```

The shared secret is `EVEYS_OCPP_WEBHOOK_SECRET` (gateway side) — the backend stores the same string in its secret store.

**Verification (Python example)**:

```python
import hmac, hashlib

def verify(body_bytes: bytes, signature_header: str, secret: str) -> bool:
    if not signature_header.startswith("sha256="):
        return False
    expected = hmac.new(
        secret.encode("utf-8"),
        body_bytes,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header[len("sha256=") :])
```

**Verify before parsing.** A request that fails signature verification must be rejected with `401`; the gateway will retry and surface the failure in operator alerts after 5 attempts.

---

## Common headers

Every webhook delivery carries:

```
Content-Type: application/json
X-Eveys-Signature: sha256=<hex>
X-Eveys-Event-Id: <uuid v4>          // idempotency key — dedup on this
X-Eveys-Event-Type: cp.status_changed
X-Eveys-Delivered-At: 2026-05-05T14:32:11.847+00:00
X-Eveys-Attempt: 1                   // 1..5 for the retry sequence
```

The body always uses the same envelope as everything else:

```json
{
  "success": true,
  "data": { "<event-specific>": "..." },
  "message": "<event_type>"
}
```

`event_id` is also duplicated inside `data.event_id` for convenience. The backend can use either — they're identical.

---

## Event catalog

### `cp.boot`

Fired when a charger sends `BootNotification.req`.

```json
{
  "success": true,
  "data": {
    "event_id": "evt-...-uuid",
    "event_type": "cp.boot",
    "occurred_at": "2026-05-05T14:00:00.000+00:00",
    "cp_id": "CP_ACME_42",
    "vendor": "ACME",
    "model": "ChargeMaster X1",
    "firmware_version": "1.4.2",
    "serial_number": "SN-001-2026",
    "registration_status": "Accepted"
  },
  "message": "cp.boot"
}
```

### `cp.online` / `cp.offline`

Fired when a charger's WebSocket connects or disconnects (gateway-side observation; the charger itself is unaware these events exist).

```json
{
  "success": true,
  "data": {
    "event_id": "evt-...",
    "event_type": "cp.online",
    "occurred_at": "2026-05-05T14:00:00.847+00:00",
    "cp_id": "CP_ACME_42",
    "pod_id": "ocpp-gw-7b3fc9d-x4z8q"
  },
  "message": "cp.online"
}
```

`cp.offline` carries the same shape with `event_type: "cp.offline"`.

### `cp.status_changed`

Fired on every inbound `StatusNotification.req` (one per connector per state change).

```json
{
  "success": true,
  "data": {
    "event_id": "evt-...",
    "event_type": "cp.status_changed",
    "occurred_at": "2026-05-05T14:32:11.847+00:00",
    "charger_reported_at": "2026-05-05T14:32:11.000+00:00",
    "cp_id": "CP_ACME_42",
    "connector_id": 1,
    "status": "Charging",
    "error_code": "NoError",
    "info": "",
    "vendor_id": "",
    "vendor_error_code": ""
  },
  "message": "cp.status_changed"
}
```

`status` is the OCPP-spec string: `Available` / `Preparing` / `Charging` / `SuspendedEV` / `SuspendedEVSE` / `Finishing` / `Reserved` / `Unavailable` / `Faulted`.

### `cp.firmware_status_changed`

Fired on inbound `FirmwareStatusNotification.req`. Backend uses this to track multi-step firmware-rollout state machines:

```json
{
  "success": true,
  "data": {
    "event_id": "evt-...",
    "event_type": "cp.firmware_status_changed",
    "occurred_at": "2026-05-05T14:00:00.000+00:00",
    "cp_id": "CP_ACME_42",
    "status": "Downloading"
  },
  "message": "cp.firmware_status_changed"
}
```

`status`: `Idle` / `Downloading` / `Downloaded` / `DownloadFailed` / `Installing` / `Installed` / `InstallationFailed` (+ Phase-5 Security profile additions).

### `cp.diagnostics_status_changed`

Same shape, for `DiagnosticsStatusNotification.req`. `status`: `Idle` / `Uploading` / `Uploaded` / `UploadFailed`.

### `cp.meter_values` (default OFF)

Fired on every inbound `MeterValues.req`. **High volume** — disabled by default. Subscribe via Kafka `cp.meter` topic instead at scale.

```json
{
  "success": true,
  "data": {
    "event_id": "evt-...",
    "event_type": "cp.meter_values",
    "occurred_at": "2026-05-05T14:32:11.847+00:00",
    "charger_reported_at": "2026-05-05T14:32:11.000+00:00",
    "cp_id": "CP_ACME_42",
    "connector_id": 1,
    "transaction_id": 12345,
    "sampled_values": [
      {
        "value": "23500",
        "measurand": "Energy.Active.Import.Register",
        "unit": "Wh",
        "context": "Sample.Periodic",
        "format": "Raw",
        "phase": "",
        "location": "Outlet"
      }
    ]
  },
  "message": "cp.meter_values"
}
```

### `tx.started` / `tx.stopped`

Fired immediately after a successful `/api/eveys/sessions/open` or `/sessions/close`. Acts as the **belt-and-braces** signal alongside the synchronous call — if the synchronous call timed out or was retried, the webhook gives the backend a second chance to reconcile.

```json
{
  "success": true,
  "data": {
    "event_id": "evt-...",
    "event_type": "tx.started",
    "occurred_at": "2026-05-05T14:32:11.847+00:00",
    "charger_reported_at": "2026-05-05T14:32:11.000+00:00",
    "cp_id": "CP_ACME_42",
    "connector_id": 1,
    "transaction_id": 12345,
    "id_tag": "RFID_ABCDEF12",
    "meter_start_wh": 4500000,
    "reservation_id": null
  },
  "message": "tx.started"
}
```

`tx.stopped` carries the closure shape:

```json
{
  "success": true,
  "data": {
    "event_id": "evt-...",
    "event_type": "tx.stopped",
    "occurred_at": "2026-05-05T15:14:30.012+00:00",
    "charger_reported_at": "2026-05-05T15:14:30.000+00:00",
    "cp_id": "CP_ACME_42",
    "transaction_id": 12345,
    "id_tag": "RFID_ABCDEF12",
    "meter_stop_wh": 4523500,
    "consumed_wh": 23500,
    "stop_reason": "Local"
  },
  "message": "tx.stopped"
}
```

---

## Reconciliation

Webhook delivery is **at-least-once**, **best-effort**. The backend should never rely solely on webhooks for billing-critical state. Pair every webhook subscription with one of:

- **Kafka subscription** to `cp.boot` / `cp.status` / `cp.meter` / `tx.started` for replayable durable events. Same `event_id` as the webhook — dedup is straightforward.
- **Periodic reconciliation** via [`GET /api/v1/charge-points/...`](./02-gateway-rest-api.md) and `/transactions/...` to catch anything dropped after the 5-attempt budget.

The gateway's authoritative source of truth is its Postgres + ClickHouse; webhooks are a notification, not a contract.

---

## Test deliveries

For development, the gateway exposes:

```http
POST /api/v1/admin/webhooks/test HTTP/1.1
Authorization: Bearer <admin token>
Content-Type: application/json

{
  "event_type": "cp.boot",
  "cp_id": "CP_ACME_42"
}
```

The gateway constructs a sample event, signs it with the configured secret, and POSTs to the configured URL for that event type. Useful for end-to-end smoke testing the backend's signature verification + handler.
