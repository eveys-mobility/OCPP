# 12 — Connect a real charger and monitor activity

> Operator/integrator guide. End-to-end: install the stack, connect a real OCPP 1.6 charge point, see traffic flowing in logs, query the databases, and use the gateway REST API from `curl` or Postman.

This doc is the **single source of truth for first-charger-connection**. If a step here doesn't work, fix the doc — pass-by-Slack rots fast.

---

## 1. What you'll have at the end

After following this doc:

- The full data plane (Postgres, Redis, Kafka, ClickHouse) running locally via docker-compose.
- The `eveys/ocpp` gateway running and accepting OCPP 1.6 WebSocket connections.
- A real charge point connected to your gateway, sending BootNotification → Heartbeat → StatusNotification → MeterValues.
- Activity visible in three places:
  - **Gateway logs** — structured JSON events per CALL.
  - **Postgres** — relational state (chargers, transactions, reservations, charging profiles).
  - **ClickHouse** — time-series (meter values, status history, boots, transaction starts).
- The gateway's REST API reachable from `curl` and Postman, with auth, error envelope, and pagination working as documented.

---

## 2. Prerequisites

Use [`07-local-dev-setup.md`](./07-local-dev-setup.md) for the tool inventory (Python 3.13, Docker, `make`, `uv`, optional `kcat`/`pgcli`/`redis-cli`/`clickhouse-client`). Run `make doctor` from the repo root to verify everything is installed.

You also need:

- A real OCPP 1.6 charge point (or a software simulator — see §5.4).
- Network reachability from the charger to your machine on TCP **19000** (host port published by the gateway container; remaps to container port 9000).
- Optional: the Eveys backend reachable. Without it, the gateway falls back per ADR-0023 — see §4.3 for the dev mock.

---

## 3. Architecture in one picture

```
                                  +------------------------+
                                  |    Operator / Backend  |
                                  +-----------+------------+
                                              |
                                              | REST   (Authorization: Bearer ...)
                                              v
   ws://host:19000/<cp_id>            +-------+--------+
   subprotocol: ocpp1.6               |   eveys/ocpp   |
                                      |    gateway     |
   +----------+      WebSocket        |                |
   | Charge   +---------------------->+  WS  : 9000    |   (host 19000 → container 9000)
   | point    |                       |  REST: 8080    +---->  Postgres   (chargers, transactions, ...)
   +----------+      (CALLs/CALLRESULTs)  |  gRPC: 50051 |     Redis      (online registry, idempotency)
                                      +-------+--------+      Kafka      (cp.meter, cp.boot, cp.status, tx.started)
                                              |
                                              v
                                        ClickHouse (time-series via Kafka ingestor)
```

- **WS port (host 19000 → container 9000)**: the only port a charger talks to. Plain `ws://` only — TLS is terminated upstream by Envoy in production. The host port is `19000` because container port `9000` collides with ClickHouse's native protocol on the host. See §5.2.
- **REST port (8080)**: backend-facing read API. Bearer-token auth required. See §7.
- **gRPC port (50051)**: command channel from backend to gateway. Same surface is exposed over REST as `/api/v1/charge-points/{cp_id}/commands/*` (E3-8); see §7.3.
- **Metrics port (9100)**: Prometheus scrape endpoint (E4-1).

---

## 4. Install & first boot

### 4.1 Bring up the data plane

From the repo root:

```bash
make doctor          # verify prerequisites
make install         # create .venv, install runtime + dev deps
make build-image     # build eveys-ocpp:dev (the compose stack consumes this)
make compose-up      # start postgres + redis + kafka + clickhouse + ocpp
make compose-wait    # block until every container reports healthy
```

`make compose-wait` exits zero when all five services are healthy (default 120 s). If it times out, run `make compose-status` to see which one is unhealthy and `docker logs <container>` for diagnostics.

### 4.2 Apply schemas

The gateway image ships with the SQLAlchemy models, but Postgres needs the schema applied once and ClickHouse needs the DDL migrations:

```bash
# Postgres — Alembic migration (idempotent)
.venv/bin/alembic upgrade head

# ClickHouse — apply DDL migrations 0001..0005
make ch-migrate
```

You can verify the schemas are in place:

```bash
psql postgres://eveys:eveys@localhost:5432/eveys_ocpp -c "\dt"
# Expected: charge_points, transactions, reservations, charging_profiles,
#           charging_schedule_periods, local_auth_lists,
#           local_auth_list_entries, alembic_version

curl 'http://localhost:8124/?query=SHOW+TABLES+FROM+eveys_ocpp'
# Expected: cp.boot  cp.meter  cp.status  schema_migrations  tx.started
```

### 4.3 Start a mock backend (recommended for first connection)

Without a backend, **Authorize defaults to `Invalid`** (the safe default per [ADR-0023](./adr/0023-backend-rest-integration.md)) — every RFID swipe will fail. For lab testing, run the dev mock:

```bash
make mock-backend
# >> booting mock backend on http://localhost:9200 ...
```

Then in a different shell, configure the gateway to use it (the compose service reads its config from env; for local development you can run the gateway *outside* compose with these vars):

```bash
export EVEYS_OCPP_BACKEND_BASE_URL=http://localhost:9200
export EVEYS_OCPP_BACKEND_TOKEN=dev-token
export EVEYS_OCPP_BACKEND_AUTHORIZE_FALLBACK=accept_offline   # also-fine for lab use
```

If you'd rather not run a mock and just want every Authorize to succeed for first-connection testing, set:

```bash
export EVEYS_OCPP_BACKEND_AUTHORIZE_FALLBACK=accept_offline
```

> **Production warning.** `accept_offline` means the gateway accepts auth for 5 minutes when the backend is unreachable. It's an operator-opt-in for lab/disaster-recovery only. Never set it in production without a written incident plan.

---

## 5. Connect a real charger

### 5.1 Charger-side configuration

In the charger's CSMS configuration screen, set:

| Setting | Value | Notes |
|---|---|---|
| Central System URL | `ws://<your-host-or-IP>:19000/<cp_id>` | Host port is `19000` (compose remaps container 9000 → host 19000). `<cp_id>` is whatever ID you give the charger (e.g. `CP_001`); it goes in the URL **path**, not as a query string. |
| WebSocket subprotocol | `ocpp1.6` | The gateway rejects mismatched subprotocols with WS close code 1002. |
| Authentication | none | The gateway accepts every WS connection. Auth/IP-allowlist/rate-limit live at the edge (Envoy) in production. |
| Heartbeat interval | server-decided | The charger sends its desired interval in BootNotification; the gateway responds with `EVEYS_OCPP_HEARTBEAT_INTERVAL_SECONDS` (default 300). |

Example URL: `ws://192.168.1.42:19000/CP_LAB_001`.

> **Heads-up**: if you're running the gateway **outside compose** (`python -m eveys_ocpp` directly on your host), it binds to host port `9000` directly — no remapping. So in that mode the URL is `ws://host:9000/<cp_id>`. The `19000` mapping is purely a compose-stack quirk to dodge the ClickHouse collision.

### 5.2 No TLS at the gateway

The gateway speaks plain `ws://`. Production puts Envoy in front with TLS termination so chargers connect to `wss://`. For first-connection testing on the same LAN, `ws://` is fine.

**Local TLS via the in-tree Envoy** (E5-1, ADR-0007). Once you've generated dev certs, the compose stack runs Envoy on `wss://<host>:19443/<cp_id>` with the same routing your production deployment will use:

```bash
scripts/gen-dev-certs.sh    # one-shot, idempotent; certs are .gitignore'd
make compose-up             # brings up everything including envoy
```

Charger dials `wss://<host>:19443/<cp_id>`. The cert is self-signed; chargers that validate certs need `--insecure` or equivalent. The plain `ws://<host>:19000/<cp_id>` path stays available for tests and dev clients that don't want to deal with self-signed roots.

If your charger firmware refuses non-TLS *and* refuses self-signed certs, put a third-party reverse proxy with locally-trusted TLS in front:

```bash
# minimal Caddyfile
:9443 {
    tls internal
    reverse_proxy localhost:19000   # or :9000 if running the gateway outside compose
}
```

Then the charger dials `wss://<host>:9443/<cp_id>` — Caddy terminates TLS with a cert your OS trust store recognises and forwards plain `ws://` to the gateway.

### 5.3 Network reachability

- **Same LAN**: find your machine's LAN IP (`ipconfig getifaddr en0` on macOS, `hostname -I` on Linux) and put it in the charger's CSMS URL.
- **Charger off-site**: open an `ngrok` TCP tunnel: `ngrok tcp 19000` (or `9000` if running outside compose). The charger dials `ws://<random>.ngrok.io:<random>/<cp_id>`. Free tier rotates the URL per session.
- **macOS firewall** may prompt to allow the eveys/ocpp container to accept incoming connections. Approve it.

### 5.4 Sanity check with a charger simulator

Before plugging in a real charger, verify the stack works with the in-tree simulator. Two options (see [`10-testing-strategy.md`](./10-testing-strategy.md)):

```bash
make compose-smoke               # Tier-3, production-shaped image, full lifecycle
.venv/bin/pytest tests/e2e/ -v   # Tier-2, against the already-up compose stack
```

If either passes, the stack is good and any subsequent charger problems are charger-side.

---

## 6. Watch activity in real time

A connected charger generates activity in three places: **logs**, **Postgres**, and **ClickHouse** (via Kafka). Here's how to see each.

### 6.1 Gateway logs

Tail the gateway container:

```bash
docker logs eveys-ocpp -f
# or, if running outside compose:
.venv/bin/python -m eveys_ocpp 2>&1 | tee gateway.log
```

Logs are structured JSON (toggle via `EVEYS_OCPP_LOG_JSON`). Every line carries `cp_id`, `transaction_id` (when applicable), and an `event` name. The events you'll see during a normal connect-charge-disconnect cycle:

| Event | Meaning | Source |
|---|---|---|
| `ws.connected` | TCP connection accepted, subprotocol negotiated. `cp_id` is bound to the context for every subsequent log line on this connection. | `transport/ws_server.py` |
| `boot_notification.decided` | Charger registered. Decision is `Accepted` / `Pending` / `Rejected`; `interval` is the heartbeat interval the gateway told the charger. | `handlers/v16/boot_notification.py` |
| `boot_notification.replay_ignored` | Same BootNotification arrived twice (e.g. retry storm). Idempotency cache short-circuited the second one. | same |
| `heartbeat.tick` (DEBUG) | Periodic keep-alive; refreshes the Redis online registry TTL. | `handlers/v16/heartbeat.py` |
| `status_notification` | Connector state change (Available, Charging, Faulted, ...). | `handlers/v16/status_notification.py` |
| `authorize.decided` | RFID-card swipe outcome. `decision` is `Accepted` / `Invalid` / `Expired` / etc. | `handlers/v16/authorize.py` |
| `authorize.cache_hit` | Redis cache short-circuited the backend call. | same |
| `start_transaction.decided` | New billing session opened. `transaction_id` is the gateway-assigned ID echoed back to the charger. | `handlers/v16/start_transaction.py` |
| `meter_values` | Energy/power sample batch. `samples` count + how many were quarantined for value-sanity. | `handlers/v16/meter_values.py` |
| `stop_transaction.applied` | Session closed cleanly. | `handlers/v16/stop_transaction.py` |
| `stop_transaction.replay_ignored_db` / `_cache` | Duplicate StopTransaction (network retry). Dual-layer dedup absorbed it. | same |
| `ws.disconnected` | Connection closed. Normal at end of session or charger reboot. | `transport/ws_server.py` |

Filter for one charger:

```bash
docker logs eveys-ocpp -f 2>&1 | jq 'select(.cp_id == "CP_LAB_001")'
```

### 6.2 Postgres queries

Postgres holds **relational state** — one row per known charger, per session, per reservation, per charging profile. Fast small-cardinality queries.

```sql
-- Has the charger booted yet?
SELECT cp_id, vendor, model, firmware_version,
       last_boot_at, last_heartbeat_at, last_status
FROM charge_points
WHERE cp_id = 'CP_LAB_001';

-- Sessions for one charger, newest first.
SELECT t.transaction_id, t.id_tag, t.connector_id,
       t.meter_start_wh, t.meter_stop_wh,
       t.started_reported_at, t.stopped_reported_at, t.stop_reason
FROM transactions t
JOIN charge_points cp ON cp.id = t.charge_point_id
WHERE cp.cp_id = 'CP_LAB_001'
ORDER BY t.started_reported_at DESC
LIMIT 20;

-- Currently-open sessions across the fleet.
SELECT cp.cp_id, t.transaction_id, t.connector_id, t.id_tag,
       t.started_reported_at
FROM transactions t
JOIN charge_points cp ON cp.id = t.charge_point_id
WHERE t.stopped_received_at IS NULL
ORDER BY t.started_reported_at DESC;

-- Active reservations.
SELECT cp.cp_id, r.connector_id, r.id_tag, r.expiry_date, r.status
FROM reservations r
JOIN charge_points cp ON cp.id = r.charge_point_id
WHERE r.status = 'Active'
  AND r.expiry_date > NOW();

-- Charging profiles installed on a charger.
SELECT cp.cp_id, p.charging_profile_id, p.connector_id, p.stack_level,
       p.charging_profile_purpose, p.charging_profile_kind,
       p.valid_from, p.valid_to, p.status
FROM charging_profiles p
JOIN charge_points cp ON cp.id = p.charge_point_id
WHERE cp.cp_id = 'CP_LAB_001';
```

Reference: `src/eveys_ocpp/persistence/models.py` is the canonical schema definition.

### 6.3 ClickHouse queries

ClickHouse holds **time-series data** — every meter sample, every status change, every boot event. Optimized for high-volume scans across time windows.

```sql
-- Latest 100 meter samples for a charger.
SELECT cp_id, transaction_id, connector_id, occurred_at, sampled_values
FROM eveys_ocpp.`cp.meter`
WHERE cp_id = 'CP_LAB_001'
ORDER BY occurred_at DESC
LIMIT 100;

-- Energy delivered by a charger in the last 24 h (Wh, energy register).
SELECT
    cp_id,
    sum(arrayJoin(sampled_values).value) AS total_wh
FROM eveys_ocpp.`cp.meter`
ARRAY JOIN sampled_values AS sv
WHERE cp_id = 'CP_LAB_001'
  AND sv.measurand = 'Energy.Active.Import.Register'
  AND occurred_at > now() - INTERVAL 1 DAY
GROUP BY cp_id;

-- Status transitions for a connector in the last hour.
SELECT occurred_at, status, error_code, vendor_error_code
FROM eveys_ocpp.`cp.status`
WHERE cp_id = 'CP_LAB_001' AND connector_id = 1
  AND occurred_at > now() - INTERVAL 1 HOUR
ORDER BY occurred_at DESC;

-- Boot history (one row per BootNotification).
SELECT occurred_at, vendor, model, firmware_version, status
FROM eveys_ocpp.`cp.boot`
WHERE cp_id = 'CP_LAB_001'
ORDER BY occurred_at DESC;

-- Transaction-start firehose (financial event stream).
SELECT cp_id, transaction_id, connector_id, id_tag, meter_start_wh, occurred_at
FROM eveys_ocpp.`tx.started`
WHERE occurred_at > now() - INTERVAL 1 HOUR
ORDER BY occurred_at DESC;
```

Use the HTTP interface for one-offs:

```bash
curl 'http://localhost:8124/?query=SELECT+count()+FROM+eveys_ocpp.%60cp.meter%60'
```

DDL is in `src/eveys_ocpp/clickhouse/ddl/`.

### 6.4 Kafka topics (firehose)

Activity flows through Kafka *before* it reaches ClickHouse — useful for plumbing custom consumers (alerts, dashboards, sidecars):

| Topic | Event | Volume |
|---|---|---|
| `cp.meter` | MeterValues samples | Highest (one batch per ~15 s per active connector) |
| `cp.status` | StatusNotification (state changes) | Low |
| `cp.boot` | BootNotification accepted/pending/rejected | Very low (once per charger boot) |
| `tx.started` | StartTransaction (financial event) | Low |

Tail one with `kcat` (install via `brew install kcat` or `apt install kafkacat`):

```bash
kcat -C -b localhost:9092 -t cp.meter -c 5 -e -q | jq
kcat -C -b localhost:9092 -t cp.boot -c 1 -e -q | jq
kcat -C -b localhost:9092 -t tx.started -c 5 -e -q | jq
```

Each message is a JSON envelope: `{schema_version, event_id, occurred_at, cp_id, payload: {...}}`. The exact shapes are pinned in `src/eveys_ocpp/events/`.

---

## 7. REST API for operators

The gateway exposes a small read-only REST surface on port **8080** under `/api/v1/`. It's the same surface the Eveys backend consumes; you can use it from `curl` or Postman for ops queries.

### 7.1 Auth setup

The middleware enforces bearer tokens against an allowlist (CSV) per [ADR-0026](./adr/0026-gateway-rest-api.md). Three modes:

| `EVEYS_OCPP_REST_AUTH_DISABLED` | `EVEYS_OCPP_REST_INBOUND_TOKENS` | Behaviour |
|---|---|---|
| `false` (default) | `""` (empty) | **Reject every request with 401.** Production-safe default. |
| `false` | `tok-a,tok-b,tok-c` | Exact-match against the CSV allowlist. Multi-value supports rotation. |
| `true` | (any) | **Bypass auth entirely.** Logs `rest_auth.disabled=True` loudly at boot. **Dev / unit-test only.** |

Recommended dev setup:

```bash
export EVEYS_OCPP_REST_INBOUND_TOKENS=dev-token
# then in your client:
#   Authorization: Bearer dev-token
```

> **Never set `rest_auth_disabled=true` in production.** It bypasses every check; the only failure mode is "every operator can read everything." The boot log warns about it, but it's still your responsibility not to enable it by mistake.

`/api/v1/health` is **exempt from auth** so a load balancer can probe it without a token. Every other route requires the header.

### 7.2 Reaching the REST port

The REST server runs inside the gateway container on port **8080**. Note: as of today this port is **not published** in `deploy/compose/docker-compose.yml`. Three workarounds:

**Option A — run the gateway outside compose.** This is what most operators do during development:

```bash
# In one shell, keep the data plane running:
make compose-up

# In another, run the gateway against it. Settings default to localhost
# for postgres/redis/kafka, and the host has those ports published.
.venv/bin/python -m eveys_ocpp
```

REST is then reachable at `http://localhost:8080/`.

**Option B — add a port mapping.** Edit `deploy/compose/docker-compose.yml`, find the `ocpp:` service's `ports:` block (around line 139), and add:

```yaml
      - "8080:8080"      # REST (host 8080 → container 8080)
```

Then `make compose-down && make compose-up`.

**Option C — `docker compose exec`.** For one-off curl from inside the container:

```bash
docker compose -f deploy/compose/docker-compose.yml exec ocpp \
    curl -s http://localhost:8080/api/v1/health
```

The rest of this section assumes REST is reachable at `http://localhost:8080`.

### 7.3 Endpoints

#### Read endpoints (5)

| Method | Path | Purpose | Auth |
|---|---|---|---|
| GET | `/api/v1/health` | Liveness + Postgres/Redis component probe | exempt |
| GET | `/api/v1/charge-points` | List chargers; cursor-paginated; filters: `online`, `vendor` | required |
| GET | `/api/v1/charge-points/{cp_id}` | Single-charger detail with active reservations + charging profiles inlined | required |
| GET | `/api/v1/charge-points/{cp_id}/transactions` | Sessions for a charger; cursor-paginated; filters: `id_tag`, `open`, `from`, `to` (ISO-8601) | required |
| GET | `/api/v1/transactions/{transaction_id}` | Single transaction by OCPP-visible `transaction_id` (not the surrogate PK) | required |

#### Command endpoints (19, E3-8)

All under `/api/v1/charge-points/{cp_id}/commands/`. POST except `get-charger-status` (GET — read-only, no OCPP round-trip).

| Path | Body | Notes |
|---|---|---|
| `remote-start` | `{ "id_tag": "...", "connector_id": 1 }` | Start a session |
| `remote-stop` | `{ "transaction_id": 12345 }` | Stop a session |
| `reset` | `{ "type": "Soft" \| "Hard" }` | Reset the charger |
| `change-configuration` | `{ "key": "...", "value": "..." }` | Returns Accepted/Rejected/RebootRequired/NotSupported |
| `get-configuration` | `{ "keys": [ "..." ] }` (or empty for all) | Returns `{ configuration_key, unknown_key }` |
| `clear-cache` | `{}` | Wipe the charger's local Authorize cache |
| `trigger-message` | `{ "requested_message": "BootNotification", "connector_id": 0 }` | Force a message from the charger |
| `unlock-connector` | `{ "connector_id": 1 }` | Returns Unlocked/UnlockFailed/NotSupported |
| `data-transfer` | `{ "vendor_id": "...", "message_id": "...", "data": "..." }` | Vendor-specific; returns `{ status, data }` |
| `get-local-list-version` | `{}` | Returns `{ "list_version": 11 }` |
| `send-local-list` | `{ "list_version": 12, "update_type": "Full"\|"Differential", "local_authorization_list": [...] }` | Mirrors to Postgres on Accepted |
| `reserve-now` | `{ "connector_id": 1, "expiry_date": "...", "id_tag": "...", "parent_id_tag": "..." }` | Returns `{ status, reservation_id }`; gateway-assigned id |
| `cancel-reservation` | `{ "reservation_id": 8842 }` | Mirrors to Postgres on Accepted |
| `get-diagnostics` | `{ "location": "https://...", ... }` | Returns `{ "file_name": "..." }` |
| `update-firmware` | `{ "location": "https://...", "retrieve_date": "..." }` | Status arrives via `FirmwareStatusNotification` |
| `set-charging-profile` | `{ "connector_id": 1, "charging_profile": {...} }` | Mirrors to Postgres on Accepted |
| `clear-charging-profile` | `{ "charging_profile_id": 42, ... }` (all optional) | Mirrors to Postgres on Accepted |
| `get-composite-schedule` | `{ "connector_id": 1, "duration": 7200, "charging_rate_unit": "W" }` | Returns the resolved composite from the charger |
| `get-charger-status` (GET) | (none) | Cached state — no OCPP round-trip |

Examples:

```bash
TOKEN=dev-token

# Tell the charger to start a session for an RFID tag.
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id_tag":"RFID_X","connector_id":1}' \
  http://localhost:8080/api/v1/charge-points/CP_LAB_001/commands/remote-start | jq
# { "status": "Accepted", "request_id": "..." }

# Stop a running session by its OCPP transaction_id.
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":12345}' \
  http://localhost:8080/api/v1/charge-points/CP_LAB_001/commands/remote-stop | jq

# Reserve a connector.
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"connector_id":1,"id_tag":"RFID_X","expiry_date":"2026-05-06T16:00:00+00:00"}' \
  http://localhost:8080/api/v1/charge-points/CP_LAB_001/commands/reserve-now | jq
# { "status": "Accepted", "reservation_id": 8842, "request_id": "..." }
```

Full spec: `docs/integration/02-gateway-rest-api.md` § "Command endpoints". ADR-0026 records the design decisions.

#### Worked example — list, then drill in

```bash
TOKEN=dev-token

# 1. health (no auth)
curl -s http://localhost:8080/api/v1/health | jq
# {
#   "status": "ok",
#   "version": "...",
#   "components": {"postgres": "ok", "redis": "ok"},
#   "request_id": "..."
# }

# 2. list chargers
curl -s -H "Authorization: Bearer $TOKEN" \
     'http://localhost:8080/api/v1/charge-points?limit=10' | jq
# {
#   "charge_points": [
#     {
#       "cp_id": "CP_LAB_001",
#       "online": true,
#       "pod_id": "pod-7b3fc9d",
#       "vendor": "ACME",
#       "model": "X1",
#       "firmware_version": "1.0.0",
#       "serial_number": "SN-1",
#       "last_boot_at": "2026-05-06T14:00:00+00:00",
#       "last_heartbeat_at": "2026-05-06T14:05:00+00:00",
#       "last_status": "Available",
#       "last_diagnostics_status": null,
#       "last_firmware_status": "Installed"
#     }
#   ],
#   "next_cursor": null,
#   "request_id": "..."
# }

# 3. drill into one
curl -s -H "Authorization: Bearer $TOKEN" \
     http://localhost:8080/api/v1/charge-points/CP_LAB_001 | jq
# (same fields plus active_reservations[] and active_charging_profiles[])

# 4. transactions for that charger, last 24h, only currently-open
curl -s -H "Authorization: Bearer $TOKEN" \
     'http://localhost:8080/api/v1/charge-points/CP_LAB_001/transactions?open=true&from=2026-05-05T00:00:00%2B00:00' | jq

# 5. one transaction by OCPP id
curl -s -H "Authorization: Bearer $TOKEN" \
     http://localhost:8080/api/v1/transactions/12345 | jq
```

### 7.4 Postman setup

**Step 1 — environment variables.** In Postman: *Environments → Create Environment* → name it `eveys-ocpp-local`. Add:

| Variable | Initial value | Current value |
|---|---|---|
| `base_url` | `http://localhost:8080` | `http://localhost:8080` |
| `token` | `dev-token` | `dev-token` |

Select this environment in the top-right dropdown.

**Step 2 — collection auth.** *New Collection → name it `eveys-ocpp` → Authorization tab → Type: Bearer Token → Token: `{{token}}`*. Every request in the collection inherits this header.

**Step 3 — requests.** Add five requests:

| Name | Method | URL |
|---|---|---|
| Health | GET | `{{base_url}}/api/v1/health` |
| List charge points | GET | `{{base_url}}/api/v1/charge-points?limit=10` |
| Get charge point | GET | `{{base_url}}/api/v1/charge-points/CP_LAB_001` |
| List transactions for CP | GET | `{{base_url}}/api/v1/charge-points/CP_LAB_001/transactions?limit=20` |
| Get transaction | GET | `{{base_url}}/api/v1/transactions/12345` |

For *Health*, override Authorization → Type: *No Auth* (the endpoint is exempt and works with or without a token).

**Step 4 — import shortcut.** Skip the manual setup by importing this Collection v2.1 JSON. In Postman: *Import → Raw text → paste*:

```json
{
  "info": {
    "name": "eveys-ocpp",
    "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
  },
  "auth": {
    "type": "bearer",
    "bearer": [{"key": "token", "value": "{{token}}", "type": "string"}]
  },
  "variable": [
    {"key": "base_url", "value": "http://localhost:8080"},
    {"key": "token", "value": "dev-token"}
  ],
  "item": [
    {
      "name": "Health",
      "request": {
        "auth": {"type": "noauth"},
        "method": "GET",
        "url": "{{base_url}}/api/v1/health"
      }
    },
    {
      "name": "List charge points",
      "request": {
        "method": "GET",
        "url": "{{base_url}}/api/v1/charge-points?limit=10"
      }
    },
    {
      "name": "Get charge point",
      "request": {
        "method": "GET",
        "url": "{{base_url}}/api/v1/charge-points/CP_LAB_001"
      }
    },
    {
      "name": "List transactions for CP",
      "request": {
        "method": "GET",
        "url": "{{base_url}}/api/v1/charge-points/CP_LAB_001/transactions?limit=20"
      }
    },
    {
      "name": "Get transaction",
      "request": {
        "method": "GET",
        "url": "{{base_url}}/api/v1/transactions/12345"
      }
    }
  ]
}
```

After import, edit the collection's variables (`base_url`, `token`) to match your environment.

### 7.5 Pagination & cursors

List endpoints (`/charge-points`, `/charge-points/{cp_id}/transactions`) return a `next_cursor` field. To fetch the next page, pass it back as the `cursor` query parameter:

```bash
# page 1
curl -s -H "Authorization: Bearer $TOKEN" \
     'http://localhost:8080/api/v1/charge-points?limit=2' | jq
# { "charge_points": [...], "next_cursor": "eyJpZCI6Mn0", "request_id": "..." }

# page 2
curl -s -H "Authorization: Bearer $TOKEN" \
     'http://localhost:8080/api/v1/charge-points?limit=2&cursor=eyJpZCI6Mn0' | jq
```

Notes:

- `next_cursor` is `null` when there are no more rows.
- Cursors are opaque base64-JSON. Don't construct them by hand — pass through what the previous page returned.
- `limit` is a **hint**: pages may be shorter than `limit` after post-Postgres filtering (e.g. `online=true` filters against Redis after the SQL page is fetched). This is documented behaviour.
- A bad cursor (`?cursor=garbage`) returns 400 `BAD_REQUEST`.

### 7.6 Error envelope

Every error from the REST surface returns the same shape (per ADR-0026):

```json
{
  "error": "human-readable message",
  "error_code": "STABLE_CODE",
  "request_id": "uuid"
}
```

Stable error codes (`src/eveys_ocpp/api/_errors.py`):

| HTTP | `error_code` | When |
|---|---|---|
| 400 | `BAD_REQUEST` | Malformed cursor, unparseable `from`/`to`, invalid query string, validation failure |
| 401 | `UNAUTHORIZED` | Missing / wrong / malformed `Authorization` header |
| 403 | `FORBIDDEN` | Reserved (used by routes that exist but aren't allowed for this token — none today) |
| 404 | `UNKNOWN_CP_ID` | `cp_id` has never sent a BootNotification |
| 404 | `UNKNOWN_TRANSACTION_ID` | `transaction_id` not in the transactions table |
| 404 | `UNKNOWN_RESERVATION_ID` | Reserved (returned by reservation endpoints when E3-7 commit 3 lands) |
| 503 | `CHARGER_OFFLINE` | Charger known but no pod owns the WS right now (or registry shows a different pod and cross-pod bus is misconfigured) |
| 504 | `CHARGER_TIMEOUT` | Charger online but didn't reply within 30 s |
| 400 | `WINDOW_TOO_LARGE` | Reserved (timeseries surface in E3-7 commit 4) |
| 429 | `RATE_LIMITED` | Reserved (no rate limit at the gateway today; Envoy at the edge) |
| 500 | `INTERNAL_ERROR` | Anything that escapes the typed-error path. The traceback is logged; the response body never leaks it. |

Use `error_code` for programmatic dispatch; the human `error` string may change without notice.

`request_id` is the value of the inbound `X-Request-ID` header, or a freshly-generated UUID when missing. Pin it in your bug reports — operators search the gateway log by this ID.

---

## 8. Troubleshooting

### Charger connects then immediately disconnects

Almost always a subprotocol mismatch. The gateway requires the WS subprotocol header to be exactly `ocpp1.6` and closes with code 1002 if it isn't. Check the charger's WebSocket subprotocol setting (some firmwares use `ocpp16`, `ocpp1_6`, or omit it entirely).

In the logs, look for `ws.connected` immediately followed by `ws.disconnected` with `close_code=1002`.

### Authorize always returns Invalid

You're hitting the default fallback policy. Either:
- Run `make mock-backend` and point the gateway at it (see §4.3), or
- Set `EVEYS_OCPP_BACKEND_AUTHORIZE_FALLBACK=accept_offline` for first-connection testing.

In the logs, `authorize.decided` with `decision=Invalid` and an absent `backend_request_id` is the smoking gun — the gateway never reached the backend and fell back to `reject`.

### REST returns 401

Check (in order):
1. Is the `Authorization` header present? It must be `Authorization: Bearer <token>` exactly — no leading/trailing whitespace, no other scheme.
2. Is the token in the allowlist? `echo $EVEYS_OCPP_REST_INBOUND_TOKENS` must include your token in the CSV.
3. Did the gateway boot **after** the env var was set? Settings are read at boot only.
4. Is the allowlist empty? `EVEYS_OCPP_REST_INBOUND_TOKENS=""` + `rest_auth_disabled=false` is "reject everything" — set the allowlist.

### REST connection refused on :8080

Port not published in compose. See §7.2 for the three workarounds.

### Transactions not appearing in ClickHouse

Postgres has the row but ClickHouse doesn't. Sequence:
1. `kcat -C -b localhost:9092 -t tx.started -c 5 -e -q` — is the event in Kafka?
2. If no: gateway didn't publish. Look for `start_transaction.publish_failed` in the logs.
3. If yes: the ingestor isn't consuming. `docker logs eveys-ocpp-clickhouse-ingestor` for diagnostics.
4. Check ClickHouse migrations are applied: `clickhouse client --query "SHOW TABLES FROM eveys_ocpp"`.

### `make compose-up` hangs on Kafka

Stale KRaft cluster ID from a previous run. Wipe data:

```bash
make compose-down-volumes
make compose-up
```

### Postgres rejects with `password authentication failed`

Stale data volume from a previous compose run with different credentials:

```bash
docker volume rm ocpp_postgres_data
make compose-up
```

---

## 9. What's next

Once you have one charger connecting and you're comfortable with the read APIs, the next platform features in flight:

- **E3-7 commits 3+4** — reservations + charging-profiles list endpoints, ClickHouse-backed meter-values + status-history endpoints. See [`docs/02-tasks.md`](./02-tasks.md) and [`docs/01-roadmap.md`](./01-roadmap.md).
- **E3-9** — webhook delivery: gateway pushes signed events to a backend URL. Replaces backend-side polling.
- **Phase 4** — load test, observability dashboard, alerting runbook.

For the full backend-integration contract — request shapes, idempotency keys, retry policy — see [`docs/integration/README.md`](./integration/README.md).

---

## 10. Maintenance

This doc is **part of the contract** for first-charger-connection.

When you change something that affects this flow:

- New REST endpoint → add a row to §7.3 and to the Postman collection JSON in §7.4.
- New error code in `_errors.py` → add a row to §7.6.
- New Postgres or ClickHouse table → add a query to §6.2 or §6.3.
- Compose port published or unpublished → update §3, §7.2.
- Backend fallback policy default changed → update §4.3 and §8.

A drift between this doc and reality counts as a Sev-2 bug. File it in GitHub Issues with label `docs:drift`.
