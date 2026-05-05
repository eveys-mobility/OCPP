# 01 — Backend REST contract

> **Audience**: backend developer implementing the endpoints `eveys/ocpp` calls into.

These are the endpoints **the backend** exposes for **the gateway to call**. Every endpoint below lives at `<eveys_backend_base_url>/api/eveys/...`.

The endpoints divide into two tiers:

- **Hot-path** (Authorize, sessions/open, sessions/close) — called synchronously from inside an OCPP handler. P99 latency budget < 200 ms; the charger has a hard 30 s OCPP timeout and we want to spend most of that budget on the WebSocket leg, not on this REST hop.
- **Cold-path** (charge-points/register, health) — called when the cost of latency is amortised across multiple OCPP messages.

Read [README.md](./README.md) first for envelope, auth, correlation, idempotency, and timestamp conventions.

---

## `POST /api/eveys/authorize`

**Tier**: hot-path. Called once per inbound OCPP `Authorize.req` from a charger.

**Purpose**: "Is the user identified by `id_tag` allowed to charge at `cp_id` right now?" The backend returns the OCPP `IdTagInfo` shape (status / parent / expiry) which the gateway forwards to the charger verbatim.

**Request:**

```http
POST /api/eveys/authorize HTTP/1.1
Host: <eveys_backend_base_url>
Authorization: Bearer <token>
Content-Type: application/json
X-Request-ID: 8a3f2c40-3b8e-4d0a-9b62-7a3d5d1e9fa1
Idempotency-Key: ocpp-auth-CP_ACME_42-RFID_ABCDEF
```

```json
{
  "id_tag": "RFID_ABCDEF12",
  "cp_id": "CP_ACME_42"
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `id_tag` | string (≤ 20 chars) | yes | OCPP-spec id_tag the charger reported. Treat as opaque. |
| `cp_id` | string (≤ 64 chars) | yes | Charger that asked. Lets the backend enforce site/operator policies. |

**Response — accepted:**

```http
HTTP/1.1 200 OK
Content-Type: application/json
X-Request-ID: 8a3f2c40-3b8e-4d0a-9b62-7a3d5d1e9fa1
```

```json
{
  "success": true,
  "data": {
    "id_tag": "RFID_ABCDEF12",
    "request_id": "8a3f2c40-3b8e-4d0a-9b62-7a3d5d1e9fa1",
    "id_tag_info": {
      "status": "Accepted",
      "parent_id_tag": "FAMILY_007",
      "expiry_date": "2026-12-31T23:59:59+00:00"
    }
  },
  "message": "Authorization granted"
}
```

| Field | Type | Notes |
|---|---|---|
| `id_tag_info.status` | enum | One of `Accepted`, `Blocked`, `Expired`, `Invalid`, `ConcurrentTx`. Gateway forwards verbatim to the charger as the OCPP `Authorize.conf.idTagInfo.status`. |
| `id_tag_info.parent_id_tag` | string \| null | Optional family-card group; charger uses for "any tag in this group can stop the transaction the parent started". |
| `id_tag_info.expiry_date` | ISO-8601 UTC \| null | When this authorization expires (charger caches it locally per OCPP § 4.2 *Authorization Cache*). |

**Response — business rejection:**

The user exists but isn't allowed to charge. **Still HTTP 200**; the rejection is conveyed via `id_tag_info.status`:

```json
{
  "success": true,
  "data": {
    "id_tag": "RFID_BLOCKED",
    "request_id": "8a3f2c40-3b8e-4d0a-9b62-7a3d5d1e9fa1",
    "id_tag_info": {
      "status": "Blocked",
      "parent_id_tag": null,
      "expiry_date": null
    }
  },
  "message": "id_tag is blocked"
}
```

`success: true` here means "the system understood the request"; the OCPP-level "no" is encoded in `id_tag_info.status`. The gateway forwards the `Blocked` / `Expired` / `Invalid` / `ConcurrentTx` status to the charger; charger refuses to start the transaction.

**Response — transport error (5xx, retried):**

```json
{
  "success": false,
  "data": null,
  "message": "internal_database_error",
  "error_code": "DB_UNAVAILABLE"
}
```

The gateway treats anything non-200 as transient. **Fallback policy** when the backend is unreachable for > 3 attempts (configurable):

- `EVEYS_OCPP_AUTHORIZE_FALLBACK=reject` (default) — return `Invalid` to the charger; charger refuses transaction. Loud, safe.
- `EVEYS_OCPP_AUTHORIZE_FALLBACK=accept_offline` — return `Accepted` with a 5-minute `expiry_date`; charger may proceed. Use only when the operator accepts the risk.

The gateway's circuit breaker trips after sustained failure; the operator dashboard surfaces "backend authorize degraded" loudly.

**Latency / retry budget:**

- **Timeout**: 5 s per call. Configurable via `EVEYS_OCPP_BACKEND_TIMEOUT_AUTHORIZE_SECONDS`.
- **Retries**: 1 retry on `5xx` or network error, with 250 ms backoff. The 30 s OCPP outer timeout absorbs the budget.
- **Idempotency-Key**: the gateway uses `ocpp-auth-{cp_id}-{id_tag}-{message_id}`; the backend must treat replays as no-ops.

---

## `POST /api/eveys/sessions/open`

**Tier**: hot-path. Called once per inbound OCPP `StartTransaction.req`.

**Purpose**: tell the backend "a charging session just started — record it and let me know if it's still authorized." The OCPP `transaction_id` is **gateway-assigned** (it's our Postgres surrogate key for the `transactions` table); the backend records it for billing.

**Request:**

```http
POST /api/eveys/sessions/open HTTP/1.1
Authorization: Bearer <token>
X-Request-ID: <uuid>
Idempotency-Key: ocpp-session-open-12345
```

```json
{
  "transaction_id": 12345,
  "cp_id": "CP_ACME_42",
  "connector_id": 1,
  "id_tag": "RFID_ABCDEF12",
  "meter_start_wh": 4500000,
  "started_reported_at": "2026-05-05T14:32:11.847+00:00",
  "reservation_id": null
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `transaction_id` | int64 | yes | Gateway-assigned. Stable across the lifetime of the session. |
| `cp_id` | string | yes | |
| `connector_id` | int32 | yes | 1-based; specific connector that started. |
| `id_tag` | string | yes | The user's id_tag (already authorized via `/authorize`). |
| `meter_start_wh` | int64 | yes | Wh reading the charger reported at session start. |
| `started_reported_at` | ISO-8601 | yes | Charger's clock at session start. **Untrusted** per OCPP — backend should also record `received_at` on its side. |
| `reservation_id` | int64 \| null | no | If the session consumed a reservation (E2-1C), the gateway includes its assigned reservation_id. |

**Response — session opened:**

```json
{
  "success": true,
  "data": {
    "transaction_id": 12345,
    "request_id": "<uuid>",
    "command_id": 8842,
    "id_tag_info": {
      "status": "Accepted",
      "parent_id_tag": null,
      "expiry_date": null
    }
  },
  "message": "Session opened"
}
```

| Field | Notes |
|---|---|
| `command_id` | Backend-assigned record id for the session in the backend's data model. The gateway logs it for ops correlation but does not depend on it. |
| `id_tag_info.status` | OCPP `StartTransaction.conf.idTagInfo.status`. The backend can re-validate authorization at session-open time and reject (`Blocked` / `Expired` / `Invalid` / `ConcurrentTx`) — the charger will then close the session immediately. |

**Response — session rejected at open:**

Backend can refuse to open a session even after authorize said `Accepted` (e.g. user's account went into arrears between the Authorize and StartTransaction):

```json
{
  "success": true,
  "data": {
    "transaction_id": 12345,
    "request_id": "<uuid>",
    "command_id": 8842,
    "id_tag_info": {
      "status": "Blocked",
      "parent_id_tag": null,
      "expiry_date": null
    }
  },
  "message": "Account in arrears; session refused"
}
```

**Latency / retry budget:**

- **Timeout**: 8 s. The gateway has already written to its own `transactions` table by the time it calls; if the backend times out, the gateway logs and proceeds — `Accepted` is the safe default for billing recovery (the session row is in the gateway's Postgres; the backend can reconcile via `GET /api/v1/transactions/{id}` or via the `tx.started` webhook).
- **Retries**: 2 retries with 500 ms / 1 s backoff.
- **Fallback policy** if backend is unreachable: `EVEYS_OCPP_SESSION_OPEN_FALLBACK=accept` (default) — return `Accepted` to the charger and rely on the webhook + reconciliation. The gateway's `transactions` row is the audit trail.

---

## `POST /api/eveys/sessions/close`

**Tier**: hot-path. Called once per inbound OCPP `StopTransaction.req`.

**Purpose**: tell the backend "the session is over, here's the final meter reading and reason."

**Request:**

```json
{
  "transaction_id": 12345,
  "cp_id": "CP_ACME_42",
  "id_tag": "RFID_ABCDEF12",
  "meter_stop_wh": 4523500,
  "stopped_reported_at": "2026-05-05T15:14:30.012+00:00",
  "stop_reason": "Local",
  "transaction_data": [
    {
      "timestamp": "2026-05-05T15:14:30.000+00:00",
      "sampled_value": [
        { "value": "23500", "measurand": "Energy.Active.Import.Register", "unit": "Wh" }
      ]
    }
  ]
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `transaction_id` | int64 | yes | The session's id (matching `/sessions/open`). |
| `meter_stop_wh` | int64 | yes | Final Wh reading. Backend computes consumed = stop − start. |
| `stop_reason` | enum string | no | One of OCPP's stop reasons: `EmergencyStop`, `EVDisconnected`, `HardReset`, `Local`, `Other`, `PowerLoss`, `Reboot`, `Remote`, `SoftReset`, `UnlockCommand`, `DeAuthorized`. Empty string when charger didn't supply one. |
| `transaction_data` | array \| null | no | Optional final snapshot of MeterValues at stop time (OCPP § 6.10 *transactionData*). Mirrors the `cp.meter` Kafka envelope; backend may ignore if it consumes the topic. |

**Response:**

```json
{
  "success": true,
  "data": {
    "transaction_id": 12345,
    "request_id": "<uuid>",
    "command_id": 8842,
    "id_tag_info": {
      "status": "Accepted",
      "parent_id_tag": null,
      "expiry_date": null
    }
  },
  "message": "Session closed"
}
```

`id_tag_info` is forwarded to the charger as `StopTransaction.conf.idTagInfo`. Backend can return `Blocked` here too — typically for billing-fraud cases — and the charger will refuse the same id_tag on subsequent transactions until the cache expires.

**Latency / retry budget:**

- **Timeout**: 10 s. StopTransaction is already idempotent on the gateway side (E2-11 idempotency cache) — the backend must also be.
- **Retries**: 3 retries; this is the billing-critical path.
- **Idempotency-Key**: `ocpp-session-close-{transaction_id}-{message_id}`.

---

## `POST /api/eveys/charge-points/register`

**Tier**: cold-path. Called once per OCPP `BootNotification.req`. Best-effort.

**Purpose**: tell the backend a charger came online (potentially for the first time). Gateway already creates / upserts the `charge_points` row in its own Postgres; this lets the backend mirror the metadata.

**Request:**

```json
{
  "cp_id": "CP_ACME_42",
  "vendor": "ACME",
  "model": "ChargeMaster X1",
  "firmware_version": "1.4.2",
  "serial_number": "SN-001-2026",
  "boot_at": "2026-05-05T14:00:00.000+00:00"
}
```

**Response:**

```json
{
  "success": true,
  "data": {
    "cp_id": "CP_ACME_42",
    "request_id": "<uuid>",
    "command_id": 4421,
    "registration_status": "Accepted",
    "heartbeat_interval_seconds": 60
  },
  "message": "Charge point registered"
}
```

| Field | Notes |
|---|---|
| `registration_status` | OCPP `BootNotification.conf.status`: `Accepted` / `Pending` / `Rejected`. Backend can reject (`Rejected`) a charger that's not in the operator's allowlist; charger will then re-attempt boot per spec. The gateway forwards verbatim. |
| `heartbeat_interval_seconds` | The charger's heartbeat cadence. If absent, the gateway uses the configured default (60 s). |

**Latency / retry budget:**

- **Timeout**: 5 s.
- **Retries**: 1.
- **Fallback** when backend is unreachable: gateway returns `Accepted` with the configured default heartbeat interval and proceeds. Backend reconciles via the `cp.boot` webhook on next delivery.

---

## `GET /api/eveys/health`

**Tier**: probe.

**Purpose**: gateway can verify the backend is up before going into full operation, and (optionally) for circuit-breaker probing.

**Response:**

```json
{
  "success": true,
  "data": {
    "status": "ok",
    "version": "<backend_version>",
    "request_id": "<uuid>"
  },
  "message": "ok"
}
```

---

## Error envelope (canonical)

Every endpoint above can return this on failure. `error_code` is a stable string the gateway can map to its own retry / circuit-breaker logic:

```json
{
  "success": false,
  "data": null,
  "message": "human-readable error",
  "error_code": "STABLE_CODE"
}
```

| `error_code` | When | Retryable? |
|---|---|---|
| `BAD_REQUEST` | Malformed body | No |
| `UNAUTHORIZED` | Bearer token missing / invalid / expired | No (caller fixes auth) |
| `UNKNOWN_ID_TAG` | id_tag not recognised by backend | No |
| `UNKNOWN_CP_ID` | cp_id not in backend's records | No |
| `IDEMPOTENCY_CONFLICT` | Replay with same key but different body | No (caller's bug) |
| `RATE_LIMITED` | Too many requests | Yes (honor `Retry-After`) |
| `DB_UNAVAILABLE` | Backend's DB is down | Yes |
| `INTERNAL_ERROR` | Unhandled exception | Yes |

The gateway falls into its per-endpoint fallback policy after the configured retry budget; the operator dashboard surfaces "backend degraded" with the offending `error_code`.

---

## JSON field naming

`snake_case` and `camelCase` are both acceptable; pick one and stick with it. The gateway translates at the HTTP boundary regardless of choice.

---

## Simulated end-to-end for a single transaction

For implementers' sanity, the full happy-path sequence:

1. Charger boots → gateway calls `POST /api/eveys/charge-points/register` → backend `Accepted`.
2. User taps RFID → charger calls OCPP `Authorize.req` → gateway calls `POST /api/eveys/authorize` → backend `Accepted`.
3. Charger calls OCPP `StartTransaction.req` → gateway assigns `transaction_id=12345` and calls `POST /api/eveys/sessions/open` → backend `Accepted`. Charger starts charging.
4. (Async, throughout the session) Charger emits `MeterValues.req` every 30 s → gateway publishes to Kafka `cp.meter` → ClickHouse via the ingestor (E2-14). Backend reads the time-series via the gateway's [`GET /api/v1/charge-points/{cp_id}/meter-values`](./02-gateway-rest-api.md) endpoint when it needs a chart.
5. User unplugs → charger calls OCPP `StopTransaction.req` → gateway calls `POST /api/eveys/sessions/close` → backend `Accepted` and closes the billing record.
6. (Async) Webhook `tx.stopped` fires from gateway to backend, carrying the same data — the backend can use this to confirm or re-derive billing if `/sessions/close` ever timed out.
