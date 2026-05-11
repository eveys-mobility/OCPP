# Use the REST API

**Audience.** A backend developer driving the gateway from their service.

**What this answers.** Auth, errors, pagination, request IDs, cursors. The patterns you'll repeat across every endpoint, with worked examples for the calls a backend hits most.

> If you want the per-endpoint payload reference, that's [`../reference/rest-api.md`](../reference/rest-api.md). This page is the integration *layer* — how to talk to the API, not what every endpoint looks like.

---

## 1. The shape of every call

Every REST call has the same skeleton:

```http
<METHOD> /api/v1/<path> HTTP/1.1
Host: <gateway-host>
Authorization: Bearer <token>
Content-Type: application/json
X-Request-ID: <uuid>           # optional but recommended

<json body, when applicable>
```

A successful response carries raw JSON — no envelope:

```json
{
  "field": "value",
  "request_id": "8a3f2c40-3b8e-4d0a-9b62-7a3d5d1e9fa1"
}
```

A failed response carries a small, stable envelope:

```json
{
  "error": "human-readable description",
  "error_code": "STABLE_CODE_FOR_PROGRAMMATIC_USE",
  "request_id": "8a3f2c40-3b8e-4d0a-9b62-7a3d5d1e9fa1"
}
```

The asymmetry is deliberate. Success bodies are domain data; the envelope would be noise. Error bodies are predictable; the envelope is essential.

---

## 2. Authentication

Bearer-token auth. The gateway accepts a CSV of valid tokens in `EVEYS_OCPP_REST_INBOUND_TOKENS`. Any one of them works.

```bash
curl -s http://<gateway>/api/v1/charge-points \
  -H "Authorization: Bearer <token>"
```

Missing header → `401 UNAUTHORIZED`. Wrong token → `403 FORBIDDEN`. The mapping is intentional — the gateway tells callers whether they forgot to send credentials or sent the wrong ones, because the remediation is different.

Token rotation is "issue a new one, deploy it everywhere, remove the old one from the CSV, deploy again". Two tokens can be valid simultaneously which makes overlapping rollouts safe.

---

## 3. Errors — the canonical codes

When the gateway answers an HTTP non-2xx, the `error_code` is one of:

| HTTP | `error_code` | What it means | What you do |
|---|---|---|---|
| 400 | `BAD_REQUEST` | Malformed payload, missing required field, invalid value. | Fix the client. |
| 401 | `UNAUTHORIZED` | No `Authorization` header (or empty). | Send a token. |
| 403 | `FORBIDDEN` | Token didn't match. | Refresh / re-issue. |
| 404 | `UNKNOWN_CP_ID` | No charger with this `cp_id`. | Re-check or register the charger. |
| 404 | `UNKNOWN_TRANSACTION_ID` | No transaction with this id. | Re-check or accept it's gone. |
| 404 | `UNKNOWN_RESERVATION_ID` | No reservation with this id. | Same. |
| 409 | `CHARGER_OFFLINE` | Charger isn't currently connected; nothing to dispatch to. | Wait / retry; surface to operator. |
| 429 | `RATE_LIMITED` | You exceeded the inbound rate limit. | Back off; retry with jitter. |
| 504 | `CHARGER_TIMEOUT` | Charger took longer than the 30 s OCPP ceiling to reply. | Retry, idempotent commands are safe. |
| 400 | `WINDOW_TOO_LARGE` | A range query asked for too many points. | Narrow the window. |
| 500 | `INTERNAL_ERROR` | Unexpected — a bug or a downstream went sick. | File against the gateway. |

These codes are **stable**. Treat them as a programmatic enum. The `error` string is human prose and may change without notice.

---

## 4. Request IDs (for tracing)

Every response carries `request_id` — a UUID the gateway assigns to every request. If you send `X-Request-ID` in, the gateway adopts it; if you don't, it generates one.

Log the `request_id` on your side too. When something goes wrong, you and the gateway operator both grep the same string across all systems — your logs, the gateway's logs, your distributed traces.

```bash
REQ=$(uuidgen)
curl -s http://<gateway>/api/v1/charge-points/CP_X \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Request-ID: $REQ"
# ... and ...
echo "asked the gateway about CP_X, request_id=$REQ"
```

---

## 5. Pagination — two modes

Every list endpoint (`/charge-points`, `/transactions`, `/charge-points/{cp_id}/transactions`, `/charge-points/{cp_id}/reservations`, …) supports **two** pagination shapes. Pick the one that fits your caller.

### Cursor pagination — for streaming through every row

```bash
curl -s "http://<gateway>/api/v1/transactions?limit=100" \
  -H "Authorization: Bearer $TOKEN"
```

Response:

```json
{
  "transactions": [ ... up to 100 rows ... ],
  "next_cursor": "eyJpZCI6MTIzNDV9",
  "request_id": "..."
}
```

`next_cursor` is an opaque base64 string. Pass it back to get the next page:

```bash
curl -s "http://<gateway>/api/v1/transactions?limit=100&cursor=eyJpZCI6MTIzNDV9" \
  -H "Authorization: Bearer $TOKEN"
```

When you reach the end, `next_cursor` is `null`. Don't try to decode the cursor — its internal shape may change between releases. Treat it as a string the gateway gave you to hand back.

Use this mode for **backfills, integrations, and any caller that's going to read every row**. Performance stays constant regardless of how deep the table is — cursor lookup is O(log N).

### Page pagination — for operator UIs

```bash
curl -s "http://<gateway>/api/v1/transactions?page=7&page_size=50" \
  -H "Authorization: Bearer $TOKEN"
```

Response:

```json
{
  "transactions": [ ... up to 50 rows ... ],
  "pagination": {
    "page":        7,
    "page_size":   50,
    "total":       4523,
    "total_pages": 91,
    "has_next":    true,
    "has_prev":    true
  },
  "request_id": "..."
}
```

Use this mode for **operator dashboards** ("show me page 7 of 91"). The `total` lets you render a page selector; `has_next` / `has_prev` light up the prev/next buttons. The page numbers are 1-indexed.

### Pick one

- Sending both `cursor` and `page` returns `400 BAD_REQUEST`. You must commit to one mode per call.
- Sending neither defaults to cursor mode, first page, default size — i.e. the existing legacy behaviour.
- Both `limit` and `page_size` work as size hints; if you set `page` and only `limit`, `limit` is used. If you set `page_size`, it wins.

`limit` / `page_size` is bounded. The gateway clamps to a maximum (default 1000) and uses a sensible default if you don't pass one. The defaults are in [`../reference/configuration.md`](../reference/configuration.md) under `rest_default_page_size` / `rest_max_page_size`.

---

## 6. Idempotency (for the calls that need it)

Most operations on the gateway are naturally idempotent — list, fetch, query. The mutating ones split into two groups:

- **OCPP dispatch commands** (`RemoteStart`, `RemoteStop`, `Reset`, `UnlockConnector`, `ChangeAvailability`, the rest). These have OCPP-level semantics. `RemoteStart` twice in a row is allowed; the charger answers `Rejected` the second time. Safe to retry.
- **State mutations** (`SendLocalList`, `ReserveNow`, `SetChargingProfile`). These write Postgres after the charger acks. Retries are still safe — they go through the same charger-side check — but be aware they may re-emit Kafka envelopes.

The gateway does not require an `Idempotency-Key` header. On the OCPP side it dedupes replays of charger-initiated messages via the message ID; on your side, treat your call IDs (`X-Request-ID`) as the de-facto idempotency anchor for log correlation.

---

## 7. Worked examples — the calls you'll write most

### 7.1 "Is this charger connected?"

```bash
curl -s http://<gateway>/api/v1/charge-points/CP_X \
  -H "Authorization: Bearer $TOKEN"
```

Look at `last_heartbeat_at`. Recent (within a couple of intervals) → online. Stale → likely offline; check the registry's online flag in the same payload.

### 7.2 "Start a session"

```bash
curl -s -X POST http://<gateway>/api/v1/charge-points/CP_X/commands/remote-start \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id_tag": "USER_RFID_123", "connector_id": 1}'
```

Response: `{"status": "Accepted", ...}`. Reminder: **`Accepted` here means the charger acknowledged the command**, not that a session is running. Watch for the `tx.started` event to confirm a `StartTransaction` actually landed.

### 7.3 "Stop a session"

```bash
curl -s -X POST http://<gateway>/api/v1/charge-points/CP_X/commands/remote-stop \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"transaction_id": 12345}'
```

The `transaction_id` is the one you saw in `tx.started`. The charger replies; a subsequent `StopTransaction` arrives from the charger and produces the `tx.stopped` event.

### 7.4 "List recent transactions for one charger"

```bash
curl -s "http://<gateway>/api/v1/charge-points/CP_X/transactions?limit=20" \
  -H "Authorization: Bearer $TOKEN"
```

`page-through with next_cursor` for older rows. Filter by `id_tag` or `status` as query params; see the reference for the full set.

### 7.5 "Get meter-value time series for a session"

```bash
curl -s "http://<gateway>/api/v1/charge-points/CP_X/meter-values?transaction_id=12345" \
  -H "Authorization: Bearer $TOKEN"
```

Time-series come from ClickHouse. Bound your window or page the result with cursors — the gateway will refuse very wide queries with `WINDOW_TOO_LARGE`.

### 7.6 "Read fresh state for a UI"

For dashboards and operations screens, lean on these read endpoints (all cacheable for a few seconds without compromising freshness):

- `GET /charge-points` — fleet listing with pagination.
- `GET /charge-points/{cp_id}` — single charger detail with active reservations and profiles inlined.
- `GET /transactions` — global transaction list across the fleet.
- `GET /transactions/{transaction_id}` — single transaction including the per-phase + SoC telemetry block.
- `GET /charge-points/{cp_id}/status-history` — connector state transitions.

For *commands*, post to `/charge-points/{cp_id}/commands/<verb>`.

### 7.7 "Approve a pending CSR"

```bash
# List pending CSRs for one charger
curl -s "http://<gateway>/api/v1/charge-points/CP_X/pending-certificate-signings?status=pending" \
  -H "Authorization: Bearer $TOKEN"

# Approve one
curl -s -X POST http://<gateway>/api/v1/charge-points/CP_X/pending-certificate-signings/42/approve \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"signed_chain": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----", "approved_by": "ops@example.com"}'
```

Response includes the charger's reply (`charger_status`) so you can tell whether the chain landed.

---

## 8. OpenAPI / Swagger UI

The gateway publishes its full OpenAPI schema at `/api/v1/openapi.json` and a Swagger UI at `/api/v1/docs`. Useful both for browsing the API interactively and for generating client stubs in your language:

```bash
# Browse interactively
open http://<gateway>/api/v1/docs

# Generate a TypeScript client (example)
curl -s http://<gateway>/api/v1/openapi.json -o openapi.json
npx openapi-typescript openapi.json --output gateway-client.d.ts
```

The schema covers every endpoint, every request body, every response, every error envelope.

> The OpenAPI surface and Swagger UI ship enabled by default. For internet-exposed deployments, set `EVEYS_OCPP_REST_OPENAPI_ENABLED=false` so the schema isn't a free reconnaissance tool. Keep it enabled behind a VPN, in dev, and in staging.

---

## 9. Rate limiting and retries

The gateway has a per-token bucket. You'll see `429 RATE_LIMITED` if you exceed it. Back off with exponential jitter (e.g., 250 ms, 500 ms, 1 s, 2 s, capped).

For `CHARGER_TIMEOUT` (504) and `CHARGER_OFFLINE` (409), retry policy depends on the operation:

- Read operations — retry freely.
- `RemoteStart` / `RemoteStop` — retry only if you can verify the side-effect didn't already happen (consume `tx.started` / `tx.stopped` to confirm).
- `Reset`, `UnlockConnector` — physical state changes; retry only after you've confirmed with the charger's own state via `GET /charge-points/{cp_id}` or a fresh `StatusNotification`.

For all retries, attach the **same** `X-Request-ID` so logs collate cleanly across attempts.

---

## Where to go from here

- Full endpoint payloads: [`../reference/rest-api.md`](../reference/rest-api.md).
- Asynchronous events that follow REST commands: [`consume-events.md`](./consume-events.md).
- Why some calls feel weird (`Accepted ≠ done`): [`../concepts/how-ocpp-flows-work.md`](../concepts/how-ocpp-flows-work.md).
