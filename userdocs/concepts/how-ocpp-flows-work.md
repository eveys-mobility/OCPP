# How OCPP flows work

**Audience.** Anyone who wants to understand what's happening between a charger, this gateway, and the backend during a charging session.

**What this answers.** The full Authorize → StartTransaction → MeterValues → StopTransaction loop, walked end-to-end with all four surfaces in play. By the end you'll be able to read a log and predict what message comes next.

> Term not familiar? [`../04-glossary.md`](../04-glossary.md) has the one-line definition.

---

## The shape of every conversation

OCPP defines a small alphabet of message types and a finite set of rules about when each may appear. Every conversation between a charger and the gateway is a sequence of these messages over a single long-lived WebSocket. There are only two roles in the alphabet:

- **Inbound**: the charger initiates; the gateway answers.
- **Outbound**: the gateway initiates (usually because your backend asked); the charger answers.

Both directions use the same envelope:

```text
[2, <message_id>, "<Action>", { ...payload... }]                     # CALL
[3, <message_id>, { ...payload... }]                                 # CALLRESULT (reply)
[4, <message_id>, "<errorCode>", "<description>", { ... }]           # CALLERROR
```

The leading `2`/`3`/`4` is the message type ID. The `message_id` correlates a CALL with its eventual CALLRESULT — same id, same socket. The protocol has a **hard 30-second ceiling** on how long the answering side may take.

That's it. Everything below this point is just *which* CALL appears *when*.

---

## A whole session in one diagram

```
   Charger                  Gateway                  Backend                  Kafka / webhooks
   ───────                  ───────                  ───────                  ────────────────

   open WS upgrade ──────►  accept; auth check
                            mark online (Redis)
                                                                                  cp.connected ─►

   BootNotification ─────►  persist; reply Accepted
                                                                                  cp.boot ─►

   StatusNotification ───►  persist
                                                                                  cp.status ─►

   Heartbeat        ─────►  refresh registry
   ...                      (no event published; high volume)

                            ◄──────  POST /commands/remote-start (your code)
   ◄──── RemoteStartTransaction
   ──── Accepted ────────►
                            ──────►  (no event; synchronous response)

   Authorize        ─────►  forward to backend ──►  POST /api/eveys/authorize
                            ◄────── { status: Accepted, ... }
   ◄──── Accepted

   StartTransaction ────►   persist; assign transaction_id
   ◄──── { transactionId: 12345, idTagInfo: Accepted }
                                                                                  tx.started ─►

   MeterValues      ─────►  persist (Kafka → ClickHouse)
   MeterValues      ─────►  ...
   MeterValues      ─────►  ...
                                                                                  cp.meter ─► (each)

                            ◄──────  POST /commands/remote-stop (your code)
   ◄──── RemoteStopTransaction
   ──── Accepted ────────►

   StopTransaction  ─────►  persist
   ◄──── { idTagInfo: Accepted }
                                                                                  tx.stopped ─►

                            (charger remains connected, idle)

   StatusNotification ───►  Available
                                                                                  cp.status ─►
```

The rest of this page is a walkthrough of that diagram — what each step does, what your code can rely on, and where things can go sideways.

---

## 1. Connection: the WebSocket comes up

A charger boots; its firmware opens a WebSocket to `wss://<your-edge>/<cp_id>` with subprotocol `ocpp1.6`. Envoy ring-hashes on `cp_id` and forwards the upgrade to a gateway pod. That pod is now responsible for this charger until the socket drops.

What happens inside the gateway:

1. The TLS handshake completes (Envoy terminates).
2. The gateway validates the WebSocket upgrade headers — subprotocol matches; Basic Auth password verifies against the row in `charge_point_credentials`.
3. The pod marks the charger online in the Redis registry: `cp:online:<cp_id> → <pod_id>`. Other pods can now route commands to this one.
4. The gateway publishes a `cp.connected` envelope to Kafka.

If any of those steps fails, the upgrade returns a 4xx and no socket is opened. The charger retries.

**What your code can rely on.** Once `cp.connected` lands, the gateway has a usable socket and will route commands successfully. There's no separate "ready" signal — connection = ready.

---

## 2. Boot: the charger announces itself

Within ~1 second of the socket opening, the charger sends its first CALL:

```text
[2, "msg-001", "BootNotification", {
  "chargePointVendor": "ACME",
  "chargePointModel":  "Charger-V2",
  "chargePointSerialNumber": "SN-001",
  "firmwareVersion":   "1.4.2"
}]
```

The gateway:

1. Persists or upserts the row in `charge_points` (creates if first time).
2. Replies with `{ status: "Accepted", currentTime: "...", interval: 60 }`.
3. Publishes `cp.boot`.

`currentTime` is the clock the charger should sync to — chargers can have wildly wrong clocks until they hear this. `interval` is how often it should heartbeat (a default of 60s is gentle; tune via `EVEYS_OCPP_HEARTBEAT_INTERVAL_SECONDS`).

**Why `Accepted` by default.** Some platforms gatekeep registration — `Pending` while a human approves, `Rejected` for unknown chargers. The gateway is `Accepted`-by-default because the Basic Auth check at the WebSocket upgrade is the load-bearing gate. By the time you see a `BootNotification`, the charger is already authenticated.

---

## 3. Status: connectors come online

Once booted, the charger reports the state of each connector:

```text
[2, "msg-002", "StatusNotification", {
  "connectorId": 1,
  "status":      "Available",
  "errorCode":   "NoError"
}]
```

Connector `0` refers to "the whole charger" — chargers report a `connectorId=0` status to signal an overall fault.

Status transitions follow a well-defined state machine:

```
Available → Preparing → Charging → SuspendedEVSE → Finishing → Available
                    └→ SuspendedEV ─┘
                    └→ Faulted ─────┘   (from any state)
                    └→ Unavailable ─┘   (operator-initiated)
                    └→ Reserved ────┘   (after ReserveNow accepted)
```

Each transition produces a `cp.status` event. Dashboards usually project the latest status per connector; audit consumers tail the full stream.

---

## 4. Heartbeat: keepalive without events

Every `interval` seconds, the charger sends:

```text
[2, "msg-N", "Heartbeat", {}]
```

The gateway refreshes the TTL on the registry's online key and replies with the current time. **No event is published.** Heartbeats at fleet scale are noise — the fact that a charger heartbeated is reflected in `last_heartbeat_at` on the REST surface.

If three heartbeats go by without a message, the registry expires the key and the charger is considered offline. (Most chargers also re-establish the socket on timeout.)

---

## 5. Remote start: the asymmetric one

This is where the most common backend bug lives. Read carefully.

Your backend wants a session to start:

```bash
POST /api/v1/charge-points/CP_X/commands/remote-start
{ "id_tag": "USER_RFID_123", "connector_id": 1 }
```

The gateway:

1. Looks up `CP_X` in the registry. **If it's owned by another pod**, the request is forwarded over the Redis pub/sub bus; that pod executes step 2 and returns the result. Your REST caller never sees the hop.
2. The owning pod dispatches an OCPP CALL on the socket:

   ```text
   [2, "msg-K", "RemoteStartTransaction", {
     "idTag":       "USER_RFID_123",
     "connectorId": 1
   }]
   ```

3. The charger answers (typically within seconds): `[3, "msg-K", { "status": "Accepted" }]`.
4. The gateway returns `{ "status": "Accepted" }` to your REST caller.

**Here's the subtle bit.** `Accepted` means "I, the charger, acknowledge this command and will try to start a session". It does **not** mean a session has started. The session begins when the charger sends its own `StartTransaction.req` — which may not happen, and there's no time guarantee.

What can go wrong between `Accepted` and `StartTransaction`:

- User walks away (no tag tap, no plug — depends on charger config).
- Charger faults during preparation.
- Charger reports `Authorize` for a different `id_tag` than you specified.

**Your code must not assume `RemoteStart.Accepted` = session started.** The right signal is the `tx.started` event. Hold pending state on your side; resolve when `tx.started` arrives or after a timeout.

---

## 6. Authorize: the synchronous hop into your backend

Some chargers fire `Authorize` before `StartTransaction` (plugin-then-tap flows). Many also fire `Authorize` mid-session when the user taps to stop. Both look the same:

```text
[2, "msg-N", "Authorize", { "idTag": "USER_RFID_123" }]
```

The gateway:

1. Checks the Redis Authorize cache for a recent answer. **Cache hit**: replies immediately.
2. **Cache miss**: makes a synchronous REST call to your backend:

   ```http
   POST /api/eveys/authorize
   { "id_tag": "USER_RFID_123", "cp_id": "CP_X" }
   ```

   Your backend returns the `IdTagInfo` shape (`status` ∈ `Accepted/Blocked/Expired/Invalid/ConcurrentTx`, plus optional `parentIdTag`, `expiryDate`).

3. Caches the answer for `authorize_cache_ttl_seconds` (default 5 minutes).
4. Forwards the verdict to the charger verbatim.

**Latency budget**: the charger has 30 seconds on the OCPP wire. The gateway needs most of that for itself; your backend should answer in < 200 ms p99.

**Fallback**: when your backend is unreachable, the gateway falls back per `EVEYS_OCPP_BACKEND_AUTHORIZE_FALLBACK`:

- `reject` (default) — Returns `Invalid` to the charger. Safe; no unauthorized charging.
- `accept_offline` — Returns `Accepted`. Useful for pilot fleets where unavailability is preferable to outage. Don't use this in production without thinking it through.

---

## 7. StartTransaction: the session actually begins

```text
[2, "msg-N", "StartTransaction", {
  "connectorId": 1,
  "idTag":       "USER_RFID_123",
  "meterStart":  12000,
  "timestamp":   "2026-05-11T10:00:00Z"
}]
```

The gateway:

1. Authorizes (same path as §6 — yes, the charger may have just done this; cache-hit path makes it cheap).
2. Assigns a `transactionId` (server-side; monotonic per gateway).
3. Persists the `transactions` row.
4. Replies `[3, "msg-N", { "transactionId": 12345, "idTagInfo": { "status": "Accepted" } }]`.
5. Publishes `tx.started`.

This is the canonical "session has begun" signal. Your billing / receipt logic hangs off this event.

The `transactionId` returned here is what every subsequent `MeterValues` and `StopTransaction` carries.

---

## 8. MeterValues: the firehose

While the session runs, the charger periodically reports samples:

```text
[2, "msg-N", "MeterValues", {
  "connectorId":   1,
  "transactionId": 12345,
  "meterValue": [{
    "timestamp": "2026-05-11T10:01:00Z",
    "sampledValue": [
      { "value": "13500", "measurand": "Energy.Active.Import.Register", "unit": "Wh" },
      { "value": "230",   "measurand": "Voltage", "phase": "L1", "unit": "V" },
      { "value": "16",    "measurand": "Current.Import", "phase": "L1", "unit": "A" },
      { "value": "78",    "measurand": "SoC", "unit": "Percent" }
    ]
  }]
}]
```

The sampling interval is operator-configurable (`MeterValueSampleInterval` on the charger; defaults to 60s for most OEMs).

The gateway:

1. Validates the samples (negative energy is quarantined; impossible voltages are rejected).
2. Publishes `cp.meter` to Kafka — one envelope per `MeterValues` CALL.
3. The ClickHouse ingestor sidecar tails the topic and writes batched inserts to ClickHouse. Your dashboards query ClickHouse for time-series.

**This is the highest-volume event in the system.** At fleet scale (thousands of chargers, 60s sampling, multiple measurands per sample) it dominates everything else. The Kafka envelope is sized for this — `cp.meter` is not a webhook-friendly event.

---

## 9. RemoteStop / StopTransaction: ending cleanly

The reverse asymmetry of §5.

Your backend sends a stop:

```bash
POST /api/v1/charge-points/CP_X/commands/remote-stop
{ "transaction_id": 12345 }
```

The gateway dispatches `RemoteStopTransaction`. Charger replies `Accepted` — meaning "I'll try to stop". The actual session-end signal is the charger's `StopTransaction`:

```text
[2, "msg-N", "StopTransaction", {
  "transactionId": 12345,
  "idTag":         "USER_RFID_123",
  "meterStop":     18500,
  "timestamp":     "2026-05-11T10:30:00Z",
  "reason":        "Local",
  "transactionData": [/* final MeterValues snapshot */]
}]
```

The gateway:

1. Looks up the matching `transactions` row.
2. Updates `meter_stop_wh`, `stopped_*_at`, `stop_reason`.
3. Replies `[3, "msg-N", { "idTagInfo": { "status": "Accepted" } }]`.
4. Publishes `tx.stopped`.

`stop_reason` can be `Local` (user tapped to stop), `Remote` (your `RemoteStop` succeeded), `EmergencyStop`, `PowerLoss`, `Reboot`, `SoftReset`, `HardReset`, `EVDisconnected`, and a handful of others. Your billing logic can use it to handle edge cases differently.

---

## 10. After the session

The charger usually fires one more `StatusNotification` (`Finishing` → `Available`) and stays connected. Heartbeats resume. The socket stays open until power-cycle or the charger initiates a reconnect.

If the socket drops at any point, the gateway's `mark_offline` runs compare-and-delete on the registry key — only the pod that *owns* the charger can mark it offline, so a reconnect-to-different-pod race doesn't produce a spurious `cp.offline` event.

---

## 11. The two corners that surprise people

### 11.1 `Authorize` and `StartTransaction` can both fire authorization

Both inbound messages produce a backend call (cache permitting). Most production chargers `Authorize` once and then `StartTransaction` immediately afterwards. The cache makes the second call free.

A charger that **only** sends `StartTransaction` without a preceding `Authorize` (the "auth-first" flow vs "plugin-first" flow) still goes through the same backend check — `StartTransaction` is the spec-defined fallback authorization point.

### 11.2 The wire ordering is not the persistence ordering

`StartTransaction` arrives on the wire at time T₁; the gateway commits to Postgres at T₂; publishes `tx.started` at T₃. T₁ → T₂ → T₃ takes a few milliseconds normally, but a slow Postgres can stretch the window. Your `tx.started` consumer sees the event with `T₃` in `occurred_at`; the charger's claimed `timestamp` is in the payload.

If you need wall-clock truth, **trust the server `occurred_at`**. The charger's clock is per-spec untrusted — it has just been told what time it is in the boot reply, but plenty of chargers ignore that.

---

## 12. Failure paths in one paragraph each

**Charger drops mid-session.** The socket closes; the registry expires the online key; no `tx.stopped` event fires for the orphaned transaction. The charger reconnects (seconds to minutes), sends `StatusNotification`s, and (per spec) replays any `StopTransaction` it had queued. The gateway's idempotency cache dedupes the replay.

**Backend times out on `Authorize`.** The gateway uses the `EVEYS_OCPP_BACKEND_AUTHORIZE_FALLBACK` policy. With `reject` the charger sees `Invalid`; with `accept_offline` it sees `Accepted` and the gateway logs a warning.

**`RemoteStart` returns `Accepted` but no `tx.started` follows.** The user didn't engage — walked away, didn't plug in, or the charger faulted before the session could begin. Your backend should time out the pending state after a sensible window (60s is typical) and surface the failure to the user.

**Kafka is unreachable.** The producer buffers and retries. The gateway's persistent state (Postgres, Redis) is unaffected; `tx.started`/`tx.stopped` get delivered eventually. Backends that synchronously block on the event should consider listening to Kafka *and* using the REST `transactions` endpoint as a backstop.

---

## Where to go from here

- **Why duplicates happen and how the gateway protects against them:** [`idempotency-and-replay.md`](./idempotency-and-replay.md).
- **How commands route when the receiving pod doesn't own the charger:** [`multi-pod-and-routing.md`](./multi-pod-and-routing.md).
- **What's trusted at each boundary in this picture:** [`security-model.md`](./security-model.md).
