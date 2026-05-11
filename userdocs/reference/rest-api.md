# REST API reference

**Use this if you** are calling the gateway over HTTP from a backend, dashboard, or scripts.

**Audience.** A developer who already knows REST and wants the exact endpoint surface.

**What this answers.** Every path, request body, response body, error code, and pagination rule.

> Integration patterns (auth, errors, retries, cursors) are explained in [`../guides/use-the-rest-api.md`](../guides/use-the-rest-api.md). This page is the catalogue.

---

## Pagination

Every list endpoint supports **two** pagination modes. Pick one per call — sending both `cursor` and `page` returns `400 BAD_REQUEST`.

### Cursor mode (streaming)

Query params: `cursor` (opaque string from the previous page), `limit` (page size hint).

Response shape:

```json
{ "<resource>": [ ... ], "next_cursor": "eyJpZCI6MTIzfQ", "request_id": "..." }
```

`next_cursor` is `null` on the last page. The cursor's internal shape is opaque; do not decode it.

Use cursor mode for backfills, sync jobs, and anything that intends to read every row eventually.

### Page mode (offset)

Query params: `page` (1-indexed integer ≥ 1), `page_size` (rows per page, capped by `rest_max_page_size`).

Response shape:

```json
{
  "<resource>": [ ... ],
  "pagination": {
    "page":        2,
    "page_size":   100,
    "total":       4523,
    "total_pages": 46,
    "has_next":    true,
    "has_prev":    true
  },
  "request_id": "..."
}
```

Use page mode for operator UIs that need to know how many rows exist and let users jump to arbitrary pages.

### Pick one per call

Cursor mode is faster on deep tables (O(log N) per page) but doesn't tell you how many rows exist in total. Page mode runs `SELECT COUNT(*)` for the `total` field — a few milliseconds on millions of rows, but it's there.

---

## Conventions in this page

- Every path is prefixed with `/api/v1`.
- Every request carries `Authorization: Bearer <token>` unless explicitly noted.
- Success responses are raw JSON (no envelope). Field names are `snake_case`.
- Error responses match the envelope in [§Error envelope](#error-envelope).
- `cp_id` is always a path parameter; `transaction_id`, `reservation_id`, etc. are integers.
- The complete OpenAPI schema is at `GET /api/v1/openapi.json`; Swagger UI at `GET /api/v1/docs`.

---

## Probes

### `GET /health` — liveness

Auth-exempt. Returns `200 OK` whenever the process is up. The body's `status` field reports downstream component health (`ok` / `degraded` / `unavailable`); HTTP status itself does not change.

```json
{
  "status": "ok",
  "components": { "postgres": "ok", "redis": "ok" },
  "version": "0.1.0",
  "build": { ... }
}
```

### `GET /ready` — readiness / drain signal

Auth-exempt. Returns `200 OK` normally. Returns `503 Service Unavailable` once the pod is draining (after `SIGTERM`). Load balancers and Envoy probe this; do not use it for application logic.

---

## Charge points

### `GET /charge-points` — list the fleet

Cursor- or page-paginated. See [Pagination](#pagination) below for the two modes.

Query params:

| Param | Type | Default | Notes |
|---|---|---|---|
| `cursor` | string | — | Opaque; pass `next_cursor` from the previous page. Cursor mode. |
| `page` | int (>=1) | — | 1-indexed page number. Page mode. |
| `page_size` | int (>=1) | `rest_default_page_size` | Page mode size. Capped by `rest_max_page_size`. |
| `limit` | int | `rest_default_page_size` | Cursor mode size. Capped by `rest_max_page_size`. |
| `vendor` | string | — | Filter by `BootNotification`-reported vendor. |
| `model` | string | — | Filter by model. |
| `firmware_version` | string | — | Filter by firmware version (exact). |
| `online` | bool | — | Filter by registry presence (`true` or `false`). |
| `last_status` | string | — | Filter by latest connector status (e.g. `Charging`). |
| `last_firmware_status` | string | — | Filter by latest firmware-update status. |
| `last_diagnostics_status` | string | — | Filter by latest diagnostics-upload status. |
| `last_log_status` | string | — | Filter by latest security-log upload status. |
| `last_boot_after` / `last_boot_before` | ISO-8601 | — | Window on `last_boot_at`. |
| `last_heartbeat_after` / `last_heartbeat_before` | ISO-8601 | — | "Hasn't checked in since X". |
| `created_after` / `created_before` | ISO-8601 | — | When the charger was first seen. |
| `cp_id_prefix` | string | — | LIKE-style prefix (`CP_ACME_*`). `%` and `_` are escaped. |

Response:

```json
{
  "charge_points": [
    {
      "cp_id": "CP_ACME_42",
      "vendor": "ACME",
      "model": "CV2",
      "firmware_version": "1.4.2",
      "serial_number": "SN-001",
      "last_boot_at": "2026-05-11T08:00:00Z",
      "last_heartbeat_at": "2026-05-11T10:00:00Z",
      "last_status": "Available",
      "connectors": [ { "connector_id": 1, "status": "Available" } ]
    }
  ],
  "next_cursor": "eyJpZCI6MTIzfQ",
  "request_id": "..."
}
```

### `GET /charge-points/{cp_id}` — single charger detail

Inlines active reservations and active charging profiles.

```json
{
  "cp_id": "CP_ACME_42",
  "vendor": "ACME",
  "model": "CV2",
  "firmware_version": "1.4.2",
  "serial_number": "SN-001",
  "last_boot_at": "...",
  "last_heartbeat_at": "...",
  "last_firmware_status": "Installed",
  "last_diagnostics_status": null,
  "last_log_status": null,
  "connectors": [ ... ],
  "active_reservations": [ ... ],
  "active_charging_profiles": [ ... ],
  "request_id": "..."
}
```

Errors: `404 UNKNOWN_CP_ID`.

---

## Transactions

### `GET /transactions` — global transaction list

Cursor- or page-paginated. Query params:

| Param | Type | Notes |
|---|---|---|
| `cursor` / `limit` | | Cursor pagination. |
| `page` / `page_size` | int | Offset pagination (1-indexed). |
| `cp_id` | string | Narrow to one charger. |
| `id_tag` | string | Narrow to one user identifier. |
| `active` | bool | `true` = currently charging; `false` = stopped. |
| `from` / `to` | ISO-8601 | Window on `started_reported_at`. |
| `connector_id` | int | Specific connector. |
| `stop_reason` | string | `Local`, `Remote`, `EmergencyStop`, `EVDisconnected`, etc. |
| `stopped_after` / `stopped_before` | ISO-8601 | Window on `stopped_reported_at` (end time). |
| `min_consumed_wh` / `max_consumed_wh` | int (≥0) | Energy band; open transactions excluded by NULL semantics. |

### `GET /charge-points/{cp_id}/transactions` — per-charger list

Same query params (minus `cp_id`); `cp_id` comes from the path.

### `GET /transactions/{transaction_id}` — single transaction with telemetry

Response carries a bounded `telemetry` block (state-of-charge + per-phase voltage/current/power snapshot when reported by the charger).

```json
{
  "transaction_id": 12345,
  "cp_id": "CP_ACME_42",
  "connector_id": 1,
  "id_tag": "USER_RFID_123",
  "meter_start_wh": 12000,
  "meter_stop_wh": 18500,
  "started_reported_at": "...",
  "started_received_at": "...",
  "stopped_reported_at": "...",
  "stopped_received_at": "...",
  "stop_reason": "Local",
  "telemetry": {
    "soc": { "start_pct": 32, "last_pct": 78, "last_at": "..." },
    "phases": {
      "L1": { "voltage_v": 230, "current_a": 16, "power_w": 3680 },
      "L2": { ... },
      "L3": { ... }
    }
  },
  "request_id": "..."
}
```

Errors: `404 UNKNOWN_TRANSACTION_ID`.

---

## Reservations and charging profiles

### `GET /charge-points/{cp_id}/reservations`

Cursor-paginated. Query params include `status` and `active=true|false` (active = `status=Active AND expiry_date > now()`).

### `GET /charge-points/{cp_id}/charging-profiles`

Cursor-paginated. Lists every profile currently installed on the charger from the gateway's mirror.

---

## Time-series

### `GET /charge-points/{cp_id}/meter-values`

Query params:

| Param | Notes |
|---|---|
| `transaction_id` | Narrow to one session. |
| `from`, `to` | ISO-8601 window bounds. |
| `measurand` | `Energy.Active.Import.Register`, `Voltage`, etc. |
| `cursor`, `limit` | Standard. |

Errors: `400 WINDOW_TOO_LARGE` when the window is too wide.

### `GET /charge-points/{cp_id}/status-history`

Connector state transitions. Same query shape as `meter-values`.

---

## Commands (POST … under `/charge-points/{cp_id}/commands/`)

Every command dispatches an OCPP CALL to the charger and returns its reply. Status mappings come straight from the OCPP enums.

| Path | Body | Response `status` values |
|---|---|---|
| `/remote-start` | `{"id_tag":"...","connector_id":1,"charging_profile":{...?}}` | `Accepted` / `Rejected` |
| `/remote-stop` | `{"transaction_id":N}` | `Accepted` / `Rejected` |
| `/reset` | `{"type":"Soft"\|"Hard"}` | `Accepted` / `Rejected` |
| `/change-configuration` | `{"key":"...","value":"..."}` | `Accepted` / `Rejected` / `RebootRequired` / `NotSupported` |
| `/get-configuration` | `{"keys":["..."]}` (empty/absent = all) | `{"configuration_key":[{key, readonly, value}], "unknown_key":["..."]}` |
| `/clear-cache` | `{}` | `Accepted` / `Rejected` |
| `/trigger-message` | `{"requested_message":"BootNotification","connector_id":0}` | `Accepted` / `Rejected` / `NotImplemented` |
| `/extended-trigger-message` | `{"requested_message":"LogStatusNotification"\|"SignChargePointCertificate"\|...Core6}` | `Accepted` / `Rejected` / `NotImplemented` |
| `/unlock-connector` | `{"connector_id":1}` | `Unlocked` / `UnlockFailed` / `NotSupported` |
| `/change-availability` | `{"connector_id":1,"type":"Operative"\|"Inoperative"}` (`connector_id:0` = whole charger) | `Accepted` / `Rejected` / `Scheduled` |
| `/data-transfer` | `{"vendor_id":"...","message_id":"...","data":"..."}` | `{"status":"Accepted"\|...,"data":"..."}` |
| `/get-local-list-version` | `{}` | `{"list_version":N}` (`-1` if no list) |
| `/send-local-list` | `{"list_version":N,"update_type":"Full"\|"Differential","local_authorization_list":[...]}` | `Accepted` / `Failed` / `NotSupported` / `VersionMismatch` |
| `/reserve-now` | `{"connector_id":1,"id_tag":"...","expiry_date":"ISO-8601","parent_id_tag":"...?","reservation_id":N?}` | `Accepted` / `Faulted` / `Occupied` / `Rejected` / `Unavailable` |
| `/cancel-reservation` | `{"reservation_id":N}` | `Accepted` / `Rejected` |
| `/get-diagnostics` | `{"location":"ftp://...","retries":N?,"retry_interval":N?,"start_time":"...?","stop_time":"...?"}` | `{"file_name":"..."}` |
| `/get-log` | `{"log_type":"DiagnosticsLog"\|"SecurityLog","request_id":N,"log":{"remote_location":"...","oldest_timestamp":"...?","latest_timestamp":"...?"},"retries":N?,"retry_interval":N?}` | `{"status":"Accepted"\|"Rejected"\|"AcceptedCanceled","filename":"...?"}` |
| `/install-certificate` | `{"certificate_type":"CentralSystemRootCertificate"\|"ManufacturerRootCertificate","certificate":"<PEM>"}` | `{"status":"Accepted"\|"Failed"\|"Rejected","sha256_hash":"..."}` |
| `/delete-certificate` | `{"sha256_hash":"..."}` | `Accepted` / `Failed` / `NotFound` |
| `/get-installed-certificate-ids` | `{"certificate_type":"CentralSystemRootCertificate"\|"ManufacturerRootCertificate"}` | `{"status":"Accepted"\|"NotFound","certificate_hash_data":[{hash_algorithm,issuer_name_hash,issuer_key_hash,serial_number}]}` |
| `/certificate-signed` | `{"certificate_chain":"-----BEGIN CERTIFICATE-----\\n..."}` | `Accepted` / `Rejected` |
| `/update-firmware` | `{"location":"ftp://...","retrieve_date":"...","retries":N?,"retry_interval":N?}` | `{}` (charger acks; status follows via `FirmwareStatusNotification`) |
| `/signed-update-firmware` | `{"request_id":N,"retries":N?,"retry_interval":N?,"firmware":{"location":"...","retrieve_date_time":"...","signing_certificate":"<PEM>","signature":"<base64>","install_date_time":"...?"}}` | `Accepted` / `Rejected` / `AcceptedCanceled` / `InvalidCertificate` / `RevokedCertificate` |
| `/set-charging-profile` | `{"connector_id":N,"cs_charging_profiles":{...}}` | `Accepted` / `Rejected` / `NotSupported` |
| `/clear-charging-profile` | `{"id":N?,"connector_id":N?,"charging_profile_purpose":"...?","stack_level":N?}` | `Accepted` / `Unknown` |
| `/get-composite-schedule` | `{"connector_id":N,"duration":N,"charging_rate_unit":"A"\|"W"?}` | `{"status":"Accepted"\|"Rejected","connector_id":N?,"schedule_start":"...?","charging_schedule":{...?}}` |

All responses include `request_id`. Common errors: `404 UNKNOWN_CP_ID`, `409 CHARGER_OFFLINE`, `504 CHARGER_TIMEOUT`.

### `GET /charge-points/{cp_id}/commands/get-charger-status` — cached state, no OCPP round-trip

Returns the gateway's cached view of the charger (`last_boot`, `last_status`, online flag). Use for dashboards where a sub-second answer matters more than absolute freshness.

---

## Pending certificate signings (operator queue)

Operator review surface for charger-initiated CSRs.

### `GET /charge-points/{cp_id}/pending-certificate-signings`

Cursor-paginated. Query param `status` ∈ `pending` / `signed` / `rejected`. Each row carries `id`, `cp_id`, `csr` (PEM), `received_at`, `status`, `signed_at`, `approved_by`, `rejected_at`, `rejected_reason`.

### `GET /charge-points/{cp_id}/pending-certificate-signings/{id}`

Single row, same shape.

### `POST /charge-points/{cp_id}/pending-certificate-signings/{id}/approve`

Body:

```json
{ "signed_chain": "-----BEGIN CERTIFICATE-----\n...", "approved_by": "ops@example.com" }
```

Marks the row `signed`, dispatches `CertificateSigned.req` to the charger, returns the charger's reply:

```json
{ "id": 42, "cp_id": "CP_X", "status": "signed", "charger_status": "Accepted", "request_id": "..." }
```

Concurrent approvals collapse to one dispatch + one 404 (the SQL row-state guard makes this atomic).

### `POST /charge-points/{cp_id}/pending-certificate-signings/{id}/reject`

Body:

```json
{ "reason": "CN does not match charger serial" }
```

Marks the row `rejected`; no charger interaction.

---

## Charger credentials (TC_073)

Operator surface for managing per-charger Basic Auth passwords. The gateway bcrypts the plaintext server-side; the password is not logged and never reaches a SQL statement.

### `PUT /charge-points/{cp_id}/credentials`

Body:

```json
{ "password": "a-long-enough-password", "actor": "ops@example.com" }
```

`actor` is optional. Password must be 12–72 bytes.

Returns `{ "cp_id": "...", "status": "provisioned", "request_id": "..." }`. Idempotent — calling with the same password is safe.

Errors: `404 UNKNOWN_CP_ID`, `400 BAD_REQUEST` (password too short or too long).

### `DELETE /charge-points/{cp_id}/credentials`

Optional query param `?actor=...`. No body.

Returns `{ "cp_id": "...", "status": "unprovisioned", "request_id": "..." }`. Idempotent — deleting a missing credential still returns 200.

Errors: `404 UNKNOWN_CP_ID` (the charger itself doesn't exist).

Both endpoints emit a `cp.credential_rotated` Kafka event for audit consumers.

---

## Admin (runtime config)

### `GET /admin/config`

Returns the live `Settings` (with secrets redacted) and any operator overrides currently in place.

### `PATCH /admin/config`

Apply a runtime override to an allow-listed setting. Body:

```json
{ "key": "EVEYS_OCPP_LOG_LEVEL", "value": "DEBUG" }
```

Only fields explicitly flagged as runtime-overridable are accepted; others return `400 BAD_REQUEST`.

### `DELETE /admin/config/overrides/{key}`

Remove a runtime override; the field reverts to its environment/defaults.

### `GET /sys/config/schema`

Returns the JSON schema of the runtime `Settings` model — useful for building admin UIs.

---

## OpenAPI / Swagger

- `GET /openapi.json` — full schema.
- `GET /docs` — Swagger UI.
- `GET /redoc` — ReDoc.

All three are auth-exempt. Disable via `EVEYS_OCPP_REST_OPENAPI_ENABLED=false` for internet-exposed deployments.

---

## Error envelope

Every non-2xx response has the shape:

```json
{
  "error":      "human-readable description",
  "error_code": "STABLE_CODE",
  "request_id": "..."
}
```

| HTTP | `error_code` | Meaning |
|---|---|---|
| 400 | `BAD_REQUEST` | Malformed payload, missing/invalid field. |
| 400 | `WINDOW_TOO_LARGE` | Time-series query window exceeded the cap. |
| 401 | `UNAUTHORIZED` | No bearer token. |
| 403 | `FORBIDDEN` | Wrong bearer token. |
| 404 | `UNKNOWN_CP_ID` | No charger with this id (or no matching subresource). |
| 404 | `UNKNOWN_TRANSACTION_ID` | No transaction with this id. |
| 404 | `UNKNOWN_RESERVATION_ID` | No reservation with this id. |
| 409 | `CHARGER_OFFLINE` | Charger isn't connected; nothing to dispatch to. |
| 429 | `RATE_LIMITED` | Exceeded per-token rate. |
| 504 | `CHARGER_TIMEOUT` | Charger didn't reply within the 30-second OCPP ceiling. |
| 500 | `INTERNAL_ERROR` | Unexpected. File against the gateway. |

`error_code` is a stable enum-like surface. `error` is human prose and may change between releases.

---

## Where to go from here

- Patterns for using these endpoints from a backend: [`../guides/use-the-rest-api.md`](../guides/use-the-rest-api.md).
- gRPC equivalent of every command: [`grpc-api.md`](./grpc-api.md).
- Event consumption: [`events.md`](./events.md).
