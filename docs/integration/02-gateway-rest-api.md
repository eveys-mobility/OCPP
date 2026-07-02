# 02 — Gateway REST API

> **Audience**: backend developer consuming the read + command endpoints `eveys/ocpp` exposes.

These are the endpoints `eveys/ocpp` (the gateway) **exposes** for the backend to call. Every endpoint below lives at `<eveys_ocpp_base_url>/api/v1/...`.

The endpoints fall into three groups:

- **Read** — query gateway-known state (presence, transactions, MeterValues time-series, status history, reservations, profiles). Pure GETs, idempotent.
- **Commands** — issue OCPP CSMS-initiated actions (RemoteStart, Reset, etc.). These are HTTP wrappers around the corresponding gRPC RPCs; same dispatch path, same charger round-trip, same status outcomes.
- **Probe** — health.

Read [README.md](./README.md) first for auth, correlation, and timestamp conventions.

> **Response shape** — gateway responses are **raw JSON**, not enveloped. The top-level object *is* the resource. Errors use a consistent error shape (see "Error responses" at the bottom of this doc). This is intentionally asymmetric with the backend-side spec [01](./01-backend-rest-contract.md), which uses `{ "success", "data", "message" }`. Outbound (gateway → backend): envelope. Inbound (backend → gateway): raw.

---

## Read endpoints

### `GET /api/v1/charge-points`

List chargers known to the gateway. Cursor-paginated.

**Query parameters**:

| Param | Type | Notes |
|---|---|---|
| `cursor` | string | Opaque cursor from a prior response. Omit on first page. |
| `limit` | int | 1–500. Default 100. |
| `page` / `page_size` | int | Offset paging (1-indexed) as an alternative to `cursor`. |
| `online` | bool | Filter by registry presence. Omit for "all". |
| `vendor` | string | Filter (exact match). |
| `model` | string | Filter (exact match). |
| `firmware_version` | string | Filter (exact match). |
| `last_status` | string | Latest connector status (e.g. `Charging`). |
| `last_firmware_status` | string | Latest firmware-update state. |
| `last_diagnostics_status` | string | Latest diagnostics-upload state. |
| `last_log_status` | string | Latest security-log upload state. |
| `last_boot_after` / `last_boot_before` | ISO-8601 | Window on `last_boot_at`. |
| `last_heartbeat_after` / `last_heartbeat_before` | ISO-8601 | "Hasn't checked in since X". |
| `created_after` / `created_before` | ISO-8601 | When the charger was first seen. |
| `cp_id_prefix` | string | LIKE-style prefix (`CP_ACME_*`). `%` and `_` escaped to literals. |

**Response**:

```json
{
  "charge_points": [
    {
      "cp_id": "CP_ACME_42",
      "online": true,
      "pod_id": "ocpp-gw-7b3fc9d-x4z8q",
      "vendor": "ACME",
      "model": "ChargeMaster X1",
      "firmware_version": "1.4.2",
      "serial_number": "SN-001-2026",
      "last_boot_at": "2026-05-05T14:00:00.000+00:00",
      "last_heartbeat_at": "2026-05-05T15:14:30.000+00:00",
      "last_status": "Available",
      "last_diagnostics_status": null,
      "last_firmware_status": "Installed",
      "connectors": [
        {
          "connector_id": 1,
          "status": "Charging",
          "error_code": "NoError",
          "last_changed_at": "2026-05-05T14:30:00.000+00:00"
        },
        {
          "connector_id": 2,
          "status": "Available",
          "error_code": "NoError",
          "last_changed_at": "2026-05-05T14:25:00.000+00:00"
        }
      ],
      "last_offline_seconds": 247,
      "last_offline_ended_at": "2026-05-11T09:30:00.000+00:00"
    }
  ],
  "next_cursor": "eyJpZCI6MTAwfQ==",
  "request_id": "<uuid>"
}
```

`last_offline_seconds` is the gap, in seconds, between the prior WS
disconnect and the reconnect that closed it. `last_offline_ended_at` is
the server-receive time of that reconnect. Both are `null` for chargers
the gateway has never observed reconnecting (first boot, history
older than the feature). For the full reconnect history use
[`GET /charge-points/{cp_id}/offline-history`](#get-apiv1charge-pointscp_idoffline-history).

`next_cursor` is `null` on the last page.

> **Two pagination modes.** This response shape (with `next_cursor`) is **cursor pagination** — pass `cursor=<value>` and the server streams the next page. As an alternative, every list endpoint also accepts `page=N` + `page_size=M` (offset pagination). When you pass `page`, the response carries a `pagination` block instead of `next_cursor`:
>
> ```json
> {
>   "charge_points": [ ... ],
>   "pagination": {
>     "page":        2,
>     "page_size":   100,
>     "total":       4523,
>     "total_pages": 46,
>     "has_next":    true,
>     "has_prev":    true
>   },
>   "request_id": "<uuid>"
> }
> ```
>
> Sending both `cursor` and `page` returns `400 BAD_REQUEST` ("pick one"). Cursor mode is best for streaming through a deep list; offset mode is best for "show me page 7" operator UIs. Pick one per call.

#### `connectors[]` vs `last_status`

`connectors[]` is the source of truth for **per-connector state** — one
entry per connector with the most recent `StatusNotification` data
sourced from ClickHouse `cp_status`. Empty array (`[]`) when the
charger has never sent a `StatusNotification`, when the gateway is
running without a ClickHouse client wired (tests, dev workstations), or
when ClickHouse is briefly unavailable — the route degrades gracefully
rather than 500.

`last_status` is a **scalar convenience field** carrying the most
recent status across **any** connector. For single-connector chargers
(the common case) it matches the device's current state. For
multi-connector chargers it is **last-write-wins** across connectors
and should not be read as "the device is currently X" — read
`connectors[]` instead.

### `GET /api/v1/charge-points/{cp_id}`

Single charger detail. Same per-charger object as the list endpoint, plus active reservations and profiles inline.

**Response**:

```json
{
  "cp_id": "CP_ACME_42",
  "online": true,
  "pod_id": "ocpp-gw-7b3fc9d-x4z8q",
  "vendor": "ACME",
  "model": "ChargeMaster X1",
  "firmware_version": "1.4.2",
  "serial_number": "SN-001-2026",
  "last_boot_at": "2026-05-05T14:00:00.000+00:00",
  "last_heartbeat_at": "2026-05-05T15:14:30.000+00:00",
  "last_status": "Charging",
  "last_diagnostics_status": null,
  "last_firmware_status": "Installed",
  "connectors": [
    {
      "connector_id": 1,
      "status": "Charging",
      "error_code": "NoError",
      "last_changed_at": "2026-05-05T14:30:00.000+00:00"
    },
    {
      "connector_id": 2,
      "status": "Available",
      "error_code": "NoError",
      "last_changed_at": "2026-05-05T14:25:00.000+00:00"
    }
  ],
  "last_offline_seconds": 247,
  "last_offline_ended_at": "2026-05-11T09:30:00.000+00:00",
  "active_reservations": [
    {
      "reservation_id": 8842,
      "connector_id": 1,
      "id_tag": "RFID_FAMILY_007",
      "expiry_date": "2026-05-05T16:00:00.000+00:00",
      "status": "Active"
    }
  ],
  "active_charging_profiles": [
    {
      "charging_profile_id": 42,
      "connector_id": 1,
      "stack_level": 0,
      "purpose": "TxDefaultProfile",
      "kind": "Recurring"
    }
  ],
  "active_sessions": [
    {
      "transaction_id": 9001,
      "connector_id": 1,
      "id_tag": "RFID_FAMILY_007",
      "started_at": "2026-05-05T14:30:00.000+00:00",
      "meter_start_wh": 1500000,
      "energy_consumed_wh": 4200,
      "last_meter_at": "2026-05-05T15:14:30.000+00:00",
      "soc_pct": 78.0,
      "power_w": 11040.0
    }
  ],
  "latest_meter": {
    "connector_id": 1,
    "energy_wh": 1504200.0,
    "occurred_at": "2026-05-05T15:14:30.000+00:00"
  },
  "request_id": "<uuid>"
}
```

#### `active_sessions[]`

One entry per currently-running transaction (Postgres `transactions`
rows with no `stopped_received_at`). Up to 10 rows; a charger with
more concurrent sessions than that is a misconfiguration worth
investigating. Empty when the charger is idle.

| Field | Source | When `null` |
|---|---|---|
| `transaction_id`, `connector_id`, `id_tag`, `started_at`, `meter_start_wh` | Postgres `transactions` | never |
| `energy_consumed_wh` | latest `Energy.Active.Import.Register` on the session's connector − `meter_start_wh` | no MeterValues have arrived since the StartTransaction (charger booting, network gap) |
| `last_meter_at` | server-receive time of that latest sample | same as above |
| `soc_pct` | `argMax` of `SoC` measurand on the transaction | charger never reports SoC (most AC chargers don't) |
| `power_w` | sum of per-phase `Power.Active.Import` snapshots | charger never reports power-active-import (some DC chargers, or charger that only reports current/voltage) |

#### `latest_meter`

Most recent `Energy.Active.Import.Register` reading regardless of
session, picking the connector with the highest `occurred_at`. Useful
for spotting metering gaps on idle chargers (no active session +
`latest_meter` going stale = the charger has stopped reporting).
`null` when the charger has never sent a MeterValues.

`active_sessions[]` and `latest_meter` are best-effort: a ClickHouse
hiccup degrades them to `null` fields (sessions metadata still
surfaces from Postgres) rather than 500ing the detail path.

`404` with `error_code: UNKNOWN_CP_ID` if the charger has never sent a BootNotification.

---

### `GET /api/v1/charge-points/{cp_id}/meter-values`

MeterValues time-series for a charger. **ClickHouse-backed**.

**Query parameters**:

| Param | Type | Required | Notes |
|---|---|---|---|
| `from` | ISO-8601 | yes | Start of window (inclusive). |
| `to` | ISO-8601 | yes | End of window (exclusive). Cap of 7 days per request. |
| `connector_id` | int | no | Filter by connector. |
| `transaction_id` | int64 | no | Filter by transaction. |
| `measurand` | string | no | OCPP measurand filter (e.g. `Energy.Active.Import.Register`, `Voltage`, `Current.Import`, `Power.Active.Import`). Repeatable. |
| `cursor` | string | no | Opaque cursor for paging (large windows). |
| `limit` | int | no | 1–10000. Default 1000. |

**Response**:

```json
{
  "samples": [
    {
      "event_id": "evt-...-uuid",
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
        },
        {
          "value": "230.5",
          "measurand": "Voltage",
          "unit": "V",
          "phase": "L1"
        }
      ]
    }
  ],
  "next_cursor": null,
  "request_id": "<uuid>"
}
```

The `sampled_values` shape mirrors the OCPP `MeterValues.req.sampledValue` (and the `cp_meter` ClickHouse table from ADR-0020). Empty-string fields (`phase`, `format`, etc.) are spec-allowed defaults.

---

### `GET /api/v1/charge-points/{cp_id}/status-history`

StatusNotification history. **ClickHouse-backed**.

**Query parameters** mirror `meter-values`: `from`, `to`, `connector_id`, `cursor`, `limit`. No `measurand` filter.

**Response**:

```json
{
  "transitions": [
    {
      "event_id": "evt-...",
      "occurred_at": "2026-05-05T14:32:11.847+00:00",
      "connector_id": 1,
      "status": "Charging",
      "error_code": "NoError",
      "info": "",
      "vendor_id": "",
      "vendor_error_code": "",
      "charger_reported_at": "2026-05-05T14:32:11.000+00:00"
    }
  ],
  "next_cursor": null,
  "request_id": "<uuid>"
}
```

---

### `GET /api/v1/charge-points/{cp_id}/offline-history`

Reconnect-by-reconnect history of WS outages observed for one
charger. **ClickHouse-backed**. One row per outage, anchored on the
reconnect that closed it.

**Query parameters**:

| Param | Type | Required | Notes |
|---|---|---|---|
| `since` | ISO-8601 | no | Earliest `came_online_at` (inclusive). Omit for "from the beginning". |
| `until` | ISO-8601 | no | Latest `came_online_at` (inclusive). Omit for "up to now". |
| `cursor` | string | no | Opaque cursor for streaming through a deep list. |
| `limit` | int | no | 1–10000. Default 1000. |
| `page` / `page_size` | int | no | Offset-mode pagination (mutually exclusive with `cursor`). |

`since > until` returns `400 BAD_REQUEST`. Pagination follows the
same dual-mode contract as `/charge-points` — cursor or
page+page_size, never both.

**Response**:

```json
{
  "cp_id": "CP_ACME_42",
  "offline_windows": [
    {
      "event_id": "evt-...",
      "occurred_at": "2026-05-11T09:30:00.000+00:00",
      "went_offline_at": "2026-05-11T09:25:53.000+00:00",
      "came_online_at": "2026-05-11T09:30:00.000+00:00",
      "offline_seconds": 247,
      "prior_pod_id": "ocpp-gw-7b3fc9d-x4z8q",
      "prior_reason": "clean"
    }
  ],
  "next_cursor": null,
  "request_id": "<uuid>"
}
```

Offline rows are emitted only when the reconnect closes a window
whose opening disconnect this gateway actually observed and held
(per the cross-pod ownership rules in ADR-0026). A pod crash that
skipped writing the disconnect marker means that particular outage
isn't represented here — the gateway prefers under-reporting to
inventing a duration it cannot prove.

`prior_reason` is `"clean"` for a 1000-Normal-Closure disconnect or
`"error"` for any other terminal exception out of the connection
task. Empty string for outages recorded before this field existed
(pre-feature history).

---

### `GET /api/v1/charge-points/{cp_id}/events`

Server-Sent Events stream of per-CP lifecycle events. Open the
stream once per detail page; the gateway pushes a framed SSE event
each time a relevant Kafka envelope keyed on this `cp_id` arrives.
Replaces polling on the detail page.

**Off by default**. The endpoint is mounted only when the pod runs
with `EVEYS_OCPP_SSE_ENABLED=true`; a pod that doesn't serve
operator UIs need not open the per-pod Kafka consumer the bus uses.

**Auth**. Same bearer-token allowlist as every other `/api/v1/*`
endpoint, but this one route additionally accepts a query parameter
`?access_token=<bearer>` because browsers' native `EventSource`
cannot set custom request headers. The query-param fallback is
scoped to this exact path only — passing `?access_token=` on any
other endpoint is ignored (defense-in-depth; URL tokens leak into
proxy logs, browser history, and `Referer` headers).

**Response**:

```
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
X-Accel-Buffering: no

: connected

event: tx_started
data: {"event_id":"evt-...","occurred_at":"2026-05-12T14:00:00+00:00","cp_id":"CP_ACME_42","schema_version":"v1","transaction_id":42,"connector_id":1,"id_tag":"RFID_FAMILY_007","meter_start_wh":1500000,"charger_reported_at":"2026-05-12T13:59:58Z"}

event: meter
data: {"event_id":"evt-...","occurred_at":"2026-05-12T14:00:30+00:00","cp_id":"CP_ACME_42","schema_version":"v1","connector_id":1,"transaction_id":42,"charger_reported_at":"2026-05-12T14:00:29Z","sampled_values":[{"value":"1504200","measurand":"MEASURAND_ENERGY_ACTIVE_IMPORT_REGISTER","unit":"UNIT_WH","context":"CONTEXT_SAMPLE_PERIODIC","format":null,"phase":null,"location":"LOCATION_OUTLET"}]}

: heartbeat

event: tx_stopped
data: {"event_id":"evt-...","occurred_at":"2026-05-12T14:30:00+00:00","cp_id":"CP_ACME_42","schema_version":"v1","transaction_id":42,"id_tag":"RFID_FAMILY_007","meter_stop_wh":1600000,"consumed_wh":100000,"stop_reason":"Local","charger_reported_at":"2026-05-12T14:29:58Z"}
```

Event types (the SSE `event:` field):

| Type | When it fires | Payload shape |
|---|---|---|
| `connected` | WS handshake completed | `{subprotocol, pod_id}` |
| `disconnected` | WS dropped (gateway-side observation) | `{pod_id, reason}` |
| `offline_duration` | Reconnect closed an outage; carries the gap | `{went_offline_at, came_online_at, offline_seconds, prior_pod_id, prior_reason}` |
| `boot` | BootNotification accepted | `{vendor, model, firmware_version, serial_number, status}` |
| `status` | StatusNotification | `{connector_id, status, error_code, info, vendor_id, vendor_error_code, charger_reported_at}` |
| `meter` | MeterValues | `{connector_id, transaction_id, sampled_values[], charger_reported_at}` |
| `firmware_status_changed` | FirmwareStatusNotification | `{status}` |
| `diagnostics_status_changed` | DiagnosticsStatusNotification | `{status}` |
| `tx_started` | StartTransaction persisted | `{transaction_id, connector_id, id_tag, meter_start_wh, charger_reported_at}` |
| `tx_stopped` | StopTransaction persisted | `{transaction_id, id_tag, meter_stop_wh, consumed_wh, stop_reason, charger_reported_at}` |
| `security_event` | SecurityEventNotification | `{type, tech_info, charger_reported_at}` |

Every payload also carries the envelope's common fields:
`event_id`, `occurred_at`, `cp_id`, `schema_version` — same as the
Kafka envelope, so a Console session can correlate an SSE event with
its Kafka record.

**Operational notes**:

- The endpoint sends a comment-line heartbeat (`: heartbeat\n\n`)
  every `EVEYS_OCPP_SSE_HEARTBEAT_SECONDS` (20s by default) so
  intermediate proxies don't close an idle stream.
- A terminal `event: error` with `data: {"reason": "<why>"}` closes
  the stream gracefully. `reason` is `slow_consumer` when the
  per-subscriber bounded queue overflowed (the client should reduce
  the work it does per event before reconnecting) or `server_closed`
  when the pod is shutting down (the client should reconnect to
  another pod).
- One stream per CP. Open a second stream for a second CP. There is
  no multiplexed shape on v1.
- Strictly tail-from-now. The endpoint does not replay history; use
  `/transactions`, `/meter-values`, or `/offline-history` for that.
- `404 UNKNOWN_CP_ID` for typo'd `cp_id` — same shape as the
  `/meter-values` 404, returned before the stream opens.

---

### `GET /api/v1/charge-points/{cp_id}/transactions`

Transactions for a charger. **Postgres-backed**.

**Query parameters**:

| Param | Type | Notes |
|---|---|---|
| `from` | ISO-8601 | Filter by `started_received_at >= from`. |
| `to` | ISO-8601 | Filter by `started_received_at < to`. |
| `id_tag` | string | Exact-match. |
| `open` | bool | `true` → only sessions where `meter_stop_wh IS NULL`. |
| `connector_id` | int | Specific connector on this charger. |
| `stop_reason` | string | Exact match (`Local`, `Remote`, `EmergencyStop`, etc.). |
| `stopped_after` / `stopped_before` | ISO-8601 | Window on `stopped_reported_at`. |
| `min_consumed_wh` / `max_consumed_wh` | int (≥ 0) | Energy band. Open transactions are excluded by NULL semantics. |
| `cursor` / `limit` | | Cursor paging. |
| `page` / `page_size` | int | Offset paging (1-indexed). |

**Response**:

```json
{
  "transactions": [
    {
      "transaction_id": 12345,
      "cp_id": "CP_ACME_42",
      "connector_id": 1,
      "id_tag": "RFID_ABCDEF12",
      "meter_start_wh": 4500000,
      "meter_stop_wh": 4523500,
      "consumed_wh": 23500,
      "started_reported_at": "2026-05-05T14:32:11.000+00:00",
      "started_received_at": "2026-05-05T14:32:11.847+00:00",
      "stopped_reported_at": "2026-05-05T15:14:30.000+00:00",
      "stopped_received_at": "2026-05-05T15:14:30.012+00:00",
      "stop_reason": "Local"
    }
  ],
  "next_cursor": null,
  "request_id": "<uuid>"
}
```

`consumed_wh` is `null` on open sessions (no stop yet).

---

### `GET /api/v1/transactions`

Global cursor-paginated list of transactions across all chargers. Same row
shape as the per-cp variant; each row already includes `cp_id` so the
caller doesn't need a second lookup.

**Query parameters**:

| Param | Type | Notes |
|---|---|---|
| `cursor` | string | Opaque cursor from a prior response. Omit on first page. |
| `limit` | int | 1–10000. Default from `Settings.rest_default_page_size`. |
| `page` / `page_size` | int | Offset paging (1-indexed) as an alternative to `cursor`. |
| `cp_id` | string | Exact match. Omit for "all chargers". |
| `id_tag` | string | Exact match. |
| `active` | bool | `true` keeps txns with no stop event yet; `false` keeps only stopped txns; omitted returns both. |
| `from` | ISO 8601 | Lower bound on `started_reported_at`. |
| `to` | ISO 8601 | Upper bound on `started_reported_at`. |
| `connector_id` | int | Specific connector. |
| `stop_reason` | string | Exact match (`Local`, `Remote`, `EmergencyStop`, etc.). |
| `stopped_after` / `stopped_before` | ISO-8601 | Window on `stopped_reported_at`. |
| `min_consumed_wh` / `max_consumed_wh` | int (≥ 0) | Energy band. Open transactions are excluded. |

Use this endpoint for "what's charging across the fleet right now?"
(`active=true`) without N+1 fan-out across the per-cp endpoint.

`400` with `error_code: BAD_REQUEST` for malformed cursor or
unparseable `from`/`to`.

---

### `GET /api/v1/transactions/{transaction_id}`

Single transaction. Same shape as the list-row above plus a `telemetry`
block carrying a bounded snapshot derived from the time-series store.

```json
{
  "transaction_id": 12345,
  "cp_id": "CP_ACME_42",
  "connector_id": 1,
  "id_tag": "RFID_ABCDEF12",
  "meter_start_wh": 4500000,
  "meter_stop_wh": 4523500,
  "consumed_wh": 23500,
  "started_reported_at": "2026-05-05T14:32:11.000+00:00",
  "started_received_at": "2026-05-05T14:32:11.847+00:00",
  "stopped_reported_at": "2026-05-05T15:14:30.000+00:00",
  "stopped_received_at": "2026-05-05T15:14:30.012+00:00",
  "stop_reason": "Local",
  "telemetry": {
    "soc": {
      "start_pct": 38.0,
      "last_pct": 81.0,
      "last_at": "2026-05-05T15:14:29.500+00:00"
    },
    "phases": {
      "L1": { "voltage_v": 231.4, "current_a": 14.8, "power_w": 3424.7, "last_at": "..." },
      "L2": { "voltage_v": 231.8, "current_a": 14.9, "power_w": 3454.7, "last_at": "..." },
      "L3": { "voltage_v": 232.2, "current_a": 15.0, "power_w": 3484.7, "last_at": "..." }
    }
  },
  "request_id": "<uuid>"
}
```

`telemetry.soc.start_pct` is the earliest SoC sample inside the
transaction's window; `last_pct` is the most recent. For a stopped
transaction `last_pct` is effectively the SoC at stop. Any SoC field
is `null` when the charger never reported SoC.

`telemetry.phases` is keyed by OCPP 1.6 phase name (`L1`, `L2`, `L3`).
Each value is `argMax(value, occurred_at)` per measurand on that phase
— for a stopped transaction that's the value at stop; for an open
transaction it's the most recent sample. Phases the charger never
reported are absent from the map. Single-phase AC populates one key.
DC chargers without per-phase metering populate none.

The list endpoints (`GET /api/v1/transactions` and
`GET /api/v1/charge-points/{cp_id}/transactions`) deliberately **do
not** include `telemetry` — surfacing it on every cursor row would
fan out one ClickHouse query per row. Callers wanting telemetry on a
specific transaction hit this detail endpoint per id, or use
`/meter-values?transaction_id=…` for the full curve.

`telemetry: null` (whole block) when the gateway has no ClickHouse
read client wired in (compose-smoke / some test envs).

`404` with `error_code: UNKNOWN_TRANSACTION_ID` if not found.

---

### `GET /api/v1/charge-points/{cp_id}/reservations`

Active and recently-cancelled reservations. **Postgres-backed**.

**Query parameters**:

| Param | Notes |
|---|---|
| `status` | `Active` (default) / `Cancelled` / `Pending` / `all` |
| `connector_id` | Filter |
| `cursor` / `limit` | Standard |

**Response**:

```json
{
  "reservations": [
    {
      "reservation_id": 8842,
      "cp_id": "CP_ACME_42",
      "connector_id": 1,
      "id_tag": "RFID_FAMILY_007",
      "parent_id_tag": "FAMILY_PARENT",
      "expiry_date": "2026-05-05T16:00:00.000+00:00",
      "status": "Active",
      "created_at": "2026-05-05T14:00:00.000+00:00"
    }
  ],
  "next_cursor": null,
  "request_id": "<uuid>"
}
```

**Note on `expiry_date`** (per ADR-0021): the gateway stores the wall-clock expiry the operator supplied; effective expiry is `now() > expiry_date` regardless of `status`. The backend should treat any reservation with `expiry_date < now()` as expired even if `status: Active`.

---

### `GET /api/v1/charge-points/{cp_id}/charging-profiles`

Charging profiles known to the gateway (mirror of charger-Accepted `SetChargingProfile` payloads per ADR-0022). **Postgres-backed.**

**Query parameters**: `status` (`Active` default / `Cleared` / `all`), `connector_id`, `purpose`, `cursor`, `limit`.

**Response**:

```json
{
  "charging_profiles": [
    {
      "charging_profile_id": 42,
      "cp_id": "CP_ACME_42",
      "connector_id": 1,
      "stack_level": 0,
      "purpose": "TxDefaultProfile",
      "kind": "Recurring",
      "recurrency_kind": "Daily",
      "valid_from": null,
      "valid_to": null,
      "transaction_id": null,
      "schedule": {
        "duration": 86400,
        "charging_rate_unit": "W",
        "min_charging_rate": null,
        "start_schedule": null,
        "periods": [
          { "start_period": 0, "limit": 11000.0, "number_phases": 3 },
          { "start_period": 28800, "limit": 22000.0, "number_phases": 3 }
        ]
      },
      "status": "Active",
      "created_at": "2026-05-05T14:00:00.000+00:00"
    }
  ],
  "next_cursor": null,
  "request_id": "<uuid>"
}
```

**Note** (per ADR-0022): this is the *input* the operator pushed; the *resolved* composite schedule lives only on the charger. To read the resolved schedule, issue `POST /api/v1/charge-points/{cp_id}/commands/get-composite-schedule`.

---

## Command endpoints

Each command endpoint is a thin HTTP wrapper around a gRPC RPC. Same charger round-trip, same status outcomes, same Idempotency-Key semantics. The set below covers all 19 OCPP CSMS-initiated commands.

The general pattern:

```text
POST /api/v1/charge-points/{cp_id}/commands/<command-name> HTTP/1.1
Authorization: Bearer <token>
X-Request-ID: <uuid>
Idempotency-Key: <opaque>
Content-Type: application/json

{ <command-specific body> }
```

Response (canonical, raw):

```json
{
  "status": "Accepted",
  "command_id": 1172,
  "request_id": "<uuid>"
}
```

Plus any command-specific fields (e.g. `reservation_id` on `reserve-now`, `file_name` on `get-diagnostics`, the resolved schedule on `get-composite-schedule`).

`status` is the OCPP-level outcome string (`Accepted` / `Rejected` / `RebootRequired` / `Occupied` / etc., depending on the command).

`command_id` is gateway-assigned for ops correlation. The gateway's structured logs link it to the OCPP message id and the gRPC trace.

When the charger is offline → `404 UNKNOWN_CP_ID` if never seen, or `503` with `error_code: CHARGER_OFFLINE` if the registry shows the charger but no pod owns it.

The full set:

| Endpoint | Body | Response extras |
|---|---|---|
| `POST .../commands/remote-start` | `{ "id_tag": "...", "connector_id": 1, "charging_profile": {...} }` | `status` ∈ `Accepted`/`Rejected` |
| `POST .../commands/remote-stop` | `{ "transaction_id": 12345 }` | `status` |
| `POST .../commands/reset` | `{ "type": "Soft" \| "Hard" }` | `status` |
| `POST .../commands/change-configuration` | `{ "key": "...", "value": "..." }` | `status` ∈ `Accepted`/`Rejected`/`RebootRequired`/`NotSupported` |
| `POST .../commands/get-configuration` | `{ "keys": [ "..." ] }` (empty/absent → all) | `{ "configuration_key": [ {key, readonly, value} ], "unknown_key": [ "..." ] }` |
| `POST .../commands/clear-cache` | `{}` | `status` |
| `POST .../commands/trigger-message` | `{ "requested_message": "BootNotification", "connector_id": 0 }` | `status` |
| `POST .../commands/extended-trigger-message` | `{ "requested_message": "LogStatusNotification" \| "SignChargePointCertificate" \| ...Core 6 }` (`connector_id` optional) | `status` ∈ `Accepted`/`Rejected`/`NotImplemented` |
| `POST .../commands/get-installed-certificate-ids` | `{ "certificate_type": "CentralSystemRootCertificate" \| "ManufacturerRootCertificate" }` | `{ "status": "Accepted" \| "NotFound", "certificate_hash_data": [{ "hash_algorithm": "SHA256", "issuer_name_hash": "...", "issuer_key_hash": "...", "serial_number": "..." }] }` (empty list on `NotFound`; `hash_algorithm` is `null` for vendor extensions outside SHA-2 family) |
| `POST .../commands/unlock-connector` | `{ "connector_id": 1 }` | `status` ∈ `Unlocked`/`UnlockFailed`/`NotSupported` |
| `POST .../commands/change-availability` | `{ "connector_id": 1, "type": "Operative" \| "Inoperative" }` (`connector_id: 0` targets the whole charger) | `status` ∈ `Accepted`/`Rejected`/`Scheduled` |
| `POST .../commands/data-transfer` | `{ "vendor_id": "...", "message_id": "...", "data": "..." }` | `{ "status": "...", "data": "..." }` |
| `POST .../commands/get-local-list-version` | `{}` | `{ "list_version": 11 }` (`-1` if charger has no list) |
| `POST .../commands/send-local-list` | `{ "list_version": 12, "update_type": "Full" \| "Differential", "local_authorization_list": [...] }` | `status` ∈ `Accepted`/`Failed`/`NotSupported`/`VersionMismatch` |
| `POST .../commands/reserve-now` | `{ "connector_id": 1, "expiry_date": "...", "id_tag": "...", "parent_id_tag": "..." }` | `{ "status": "Accepted", "reservation_id": 8842 }` (charger refuses → status ∈ `Occupied`/`Faulted`/`Unavailable`/`Rejected`) |
| `POST .../commands/cancel-reservation` | `{ "reservation_id": 8842 }` | `status` |
| `POST .../commands/get-diagnostics` | `{ "location": "https://...", "retries": 3, "retry_interval": 60, "start_time": "...", "stop_time": "..." }` | `{ "file_name": "diag-...tar.gz" }` |
| `POST .../commands/update-firmware` | `{ "location": "https://...", "retrieve_date": "...", "retries": 3, "retry_interval": 60 }` | empty |
| `POST .../commands/set-charging-profile` | `{ "connector_id": 1, "charging_profile": {...} }` | `status` ∈ `Accepted`/`Rejected`/`NotSupported` |
| `POST .../commands/clear-charging-profile` | `{ "charging_profile_id": 42, "connector_id": 1, "purpose": "TxProfile", "stack_level": 0 }` (all optional; all-empty → wipe everything) | `status` ∈ `Accepted`/`Unknown` |
| `POST .../commands/get-composite-schedule` | `{ "connector_id": 1, "duration": 7200, "charging_rate_unit": "W" }` | `{ "status": "Accepted", "connector_id": 1, "schedule_start": "...", "charging_schedule": { "duration": 7200, "charging_rate_unit": "W", "periods": [...] } }` |

The full request / response body shape for each is the same as the corresponding gRPC message in `proto/ocpp_gw/v1/gateway.proto` — JSON-mapped via `MessageToJson` if you want to mechanically check a payload. For non-trivial commands (`set-charging-profile`, `send-local-list`) the body uses the same nested objects as the proto.

### Read-only command (no OCPP round-trip)

| Endpoint | Body | Notes |
|---|---|---|
| `GET .../commands/get-charger-status` | `(none)` | Returns the cached state — equivalent to `GET /api/v1/charge-points/{cp_id}` projected to the OCPP-relevant subset. Doesn't round-trip the WebSocket. |

---

## Pending certificate signings (operator queue)

Operator review surface for charger-initiated CSRs (OCPP 1.6 Security Whitepaper §4.13 SignCertificate). The charger sends a CSR; the gateway persists it as a `pending` row. An operator inspects the row, signs the CSR offline against whatever CA they choose, and posts the resulting chain back to `/approve` — at which point the gateway dispatches `CertificateSigned.req` to the charger and surfaces the charger's reply.

| Endpoint | Body | Returns |
|---|---|---|
| `GET /api/v1/charge-points/{cp_id}/pending-certificate-signings` | `(none)` (query: `status ∈ pending\|signed\|rejected`, `cursor`, `limit`) | `{ "pending_certificate_signings": [...], "next_cursor": "..." }` — each row carries `id`, `cp_id`, `csr` (PEM), `received_at`, `status`, `signed_at`, `approved_by`, `rejected_at`, `rejected_reason`. |
| `GET .../pending-certificate-signings/{id}` | `(none)` | Single row, same shape. `404` when the row or charger doesn't exist. |
| `POST .../pending-certificate-signings/{id}/approve` | `{ "signed_chain": "-----BEGIN CERTIFICATE-----\n...\n-----BEGIN CERTIFICATE-----\n...", "approved_by": "..." }` (`approved_by` optional) | `{ "id", "cp_id", "status": "signed", "charger_status": "Accepted" \| "Rejected" }`. The DB transition happens before the dispatch — a Rejected charger reply still leaves the row `signed`; operators reading the row later can tell from the response that the chain didn't take. |
| `POST .../pending-certificate-signings/{id}/reject` | `{ "reason": "..." }` | `{ "id", "cp_id", "status": "rejected", "rejected_reason": "..." }`. No charger interaction — per spec, the charger re-submits if it cares, producing a fresh row. |

Both action endpoints return `404` with `error_code=UNKNOWN_CP_ID` when the row is missing OR no longer `pending` (i.e. another operator already approved/rejected it). The transition is guarded at the SQL row-state level so concurrent calls collapse to one dispatch + one 404.

---

## Charger credential rotation (TC_073)

Operator surface for managing per-charger Basic Auth credentials. The plaintext password is supplied in the request body; the gateway bcrypts it server-side. The plaintext never reaches a SQL statement, log line, or audit event.

| Endpoint | Body | Returns |
|---|---|---|
| `PUT /api/v1/charge-points/{cp_id}/credentials` | `{ "password": "...", "actor": "ops@example.com" }` (`actor` optional). Password must be 12–72 bytes — the bcrypt input limit. | `{ "cp_id": "...", "status": "provisioned" }`. Idempotent. `404 UNKNOWN_CP_ID` when the charger doesn't exist. |
| `DELETE /api/v1/charge-points/{cp_id}/credentials` (optional query `?actor=...`) | empty | `{ "cp_id": "...", "status": "unprovisioned" }`. Idempotent — calling on a charger with no credential row still returns 200. `404 UNKNOWN_CP_ID` when the charger itself doesn't exist. |

Every successful change emits a `cp.credential_rotated` Kafka envelope (`action ∈ set|removed`, plus the operator-supplied `actor`). The password is never carried.

---

## `GET /api/v1/health`

Probe.

```json
{
  "status": "ok",
  "version": "<gateway_version>",
  "components": {
    "postgres": "ok",
    "redis": "ok",
    "kafka": "ok",
    "clickhouse": "ok"
  }
}
```

When a downstream component is degraded, `components.<name>` becomes `degraded` or `unavailable` and the top-level `status` becomes `degraded`. HTTP status remains `200`; the backend's monitoring should alert on `status != "ok"`.

---

## `GET /api/v1/ready`

Readiness probe — distinct from `/health`. Returns `200` when the pod is accepting new connections, `503` once the pod has begun draining for shutdown.

```json
{ "status": "ready", "request_id": "<uuid>" }
```

When draining (SIGTERM received, drain mechanism engaged):

```json
{ "status": "draining", "request_id": "<uuid>", "draining_for_seconds": 4.2 }
```

Auth-exempt — the load balancer's readiness probe doesn't carry a bearer token. Used by Kubernetes / Envoy / any LB that respects HTTP readiness probes: a 503 here removes the pod from the rotation pool before the process actually exits, so chargers don't get connection refusals during rolling deploys.

`/health` reports component liveness (Postgres, Redis); `/ready` reports willingness to accept new connections. Monitor both.

---

## Admin runtime config — `GET / PATCH / DELETE /api/v1/admin/config`

Per-pod runtime overrides for a tightly-scoped allowlist of `Settings` fields. The gateway's `Settings` is frozen (env vars are the source of truth and most fields bind into a SQLAlchemy engine / TCP socket / Kafka producer at boot), but a small set is read fresh on every use and can be flipped without a rolling deploy. This surface is the operator UX for those.

**Per-pod scope.** Hitting these endpoints on pod A doesn't affect pod B. Cluster-wide propagation via Redis pub/sub is a future enhancement; for fleet-wide changes, deploy a new env value and roll out.

### `GET /api/v1/admin/config`

Returns the current effective `Settings` dump (secrets auto-redact via E5-7) plus the in-process overrides currently in effect plus the allowlist for `PATCH`.

```json
{
  "settings": {
    "log_level": "INFO",
    "ws_rate_limit_enabled": true,
    "rest_inbound_tokens": "**********",
    "db_url": "**********",
    "...": "..."
  },
  "overrides": { "log_level": "DEBUG" },
  "allowlist": {
    "log_level": "stdlib logging level applied to every emit.",
    "ws_rate_limit_enabled": "Per-charger CALL rate limiter (E5-3) kill-switch.",
    "backend_authorize_cache_enabled": "Per-pod Authorize cache (E3-4) kill-switch."
  },
  "scope": "per-pod",
  "request_id": "<uuid>"
}
```

### `PATCH /api/v1/admin/config`

Body: `{"updates": {"<field>": <value>, ...}}`. Each field must be in the allowlist. Non-allowlisted fields → `400 BAD_REQUEST` with the allowed list in the message.

Tolerant value coercion: bools accept `true` / `"true"` / `1`; `log_level` is validated against the same `Literal[...]` as `Settings.log_level`. Atomicity is **not** promised — if a PATCH carries one allowed and one rejected field, the allowed one stays in effect and the response surfaces both halves so the operator can revert deliberately.

`log_level` updates take effect immediately via `logging.getLogger().setLevel(...)`. The other fields' read sites consult overrides per-call, so the next message / next Authorize / next CALL picks up the new value.

```json
{
  "applied": { "log_level": "DEBUG" },
  "overrides": { "log_level": "DEBUG" },
  "scope": "per-pod",
  "request_id": "<uuid>"
}
```

### `DELETE /api/v1/admin/config/overrides/{key}`

Clears one override. Subsequent reads fall back to the boot-time `Settings` value. Idempotent: clearing a key that was never set returns `cleared: false` rather than 404.

```json
{
  "cleared": true,
  "key": "log_level",
  "overrides": {},
  "scope": "per-pod",
  "request_id": "<uuid>"
}
```

### Allowlist criteria

A field gets allowlisted when:
1. The runtime read site reads it fresh on every use (not cached at boot).
2. The value is operator-meaningful at runtime (not `db_url` / `ws_port` / `kafka_brokers` — those bind to a socket / producer at boot and changing them via PATCH would either be a no-op or a bug).

Adding a new allowlisted field is a deliberate `runtime_overrides.py` edit plus a matching `get_override(...)` call site.

---

## Error responses

Errors return a consistent shape (not the success-shape envelope):

```json
{
  "error": "human-readable description",
  "error_code": "STABLE_CODE",
  "request_id": "<uuid>"
}
```

Stable `error_code` values the gateway emits:

| `error_code` | Meaning | HTTP |
|---|---|---|
| `BAD_REQUEST` | Malformed body / missing required field | 400 |
| `UNAUTHORIZED` | Bearer token missing / invalid | 401 |
| `FORBIDDEN` | Token valid but lacks scope | 403 |
| `UNKNOWN_CP_ID` | cp_id has never sent a BootNotification | 404 |
| `UNKNOWN_TRANSACTION_ID` | transaction_id not found | 404 |
| `UNKNOWN_RESERVATION_ID` | reservation_id not found | 404 |
| `CHARGER_OFFLINE` | Charger known but no pod owns the WS right now | 503 |
| `CHARGER_TIMEOUT` | Charger online but didn't reply within 30 s | 504 |
| `WINDOW_TOO_LARGE` | `from`/`to` span > 7 days on time-series endpoints | 400 |
| `RATE_LIMITED` | Too many requests | 429 |
| `INTERNAL_ERROR` | Unhandled exception | 500 |

---

## JSON field naming

`snake_case` and `camelCase` are both acceptable; pick one per surface and stick with it. The gateway translates at the HTTP boundary regardless of choice.

---

## gRPC alternative

The gateway also exposes the same 19 commands as gRPC on port 50051. Use REST as documented here for the standard integration; gRPC is available for callers that want a lower-overhead binary protocol (also used internally for cross-pod command routing).
