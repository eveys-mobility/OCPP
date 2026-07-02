# Quickstart — boot a charger, send a command, see an event

**Audience.** A developer trying the gateway for the first time. Docker Engine + Compose v2 on any workstation is enough.

**What this answers.** Bring the stack up, point a simulated charger at it, send a `RemoteStart` over REST, and watch the matching event land in Kafka.

This takes about ten minutes start to finish, most of which is the first-time image pull.

---

## What you'll have running

```
   tools.sim charger(s)            eveys/ocpp gateway pod          Postgres / Redis / Kafka / ClickHouse
   ─────────────────────           ────────────────────────         ────────────────────────────────────
   ws://localhost:19000  ◄──────►  WS  :9000  (host :19000)         in their own containers
   (you run this last)             REST :8080 (host :8080)
                                   gRPC :50051
                                   metrics :9100
```

Four containers come up alongside the gateway. You don't need to know their internals — just that they're managed by `docker compose` and you can tear them down in one command at the end.

---

## Prerequisites

| Tool | Why |
|---|---|
| Docker Engine + Compose v2 (Desktop 4.30+ or Linux server install) | Brings up the stack. |
| `make` | One-liner targets. |
| Python 3.13 + [`uv`](https://docs.astral.sh/uv/) | The simulator runs as a Python module. `uv` installs deps in seconds. |
| `curl` | For the REST call. Anything that POSTs JSON will do. |

To confirm everything's installed:

```bash
docker --version && docker compose version && make --version && python3 --version && uv --version
```

If any of those is missing, install it and come back.

---

## 1. Clone the repository

```bash
git clone https://github.com/eveys-mobility/OCPP.git
cd OCPP
```

## 2. Install the Python deps once

Used by the simulator and by a few helper scripts:

```bash
make install
```

This creates a `.venv/`, installs the package in editable mode with dev extras, and registers pre-commit hooks. Takes 30–60 seconds the first time.

## 3. Bring the stack up

```bash
make compose-up
```

This:

- Pulls Postgres, Redis, Kafka, ClickHouse, Envoy, and the gateway image.
- Runs database migrations against Postgres and ClickHouse.
- Starts every container and waits until the gateway answers `200 OK` on `/api/v1/ready`.

Expect 60–90 seconds the first time (image pulls). Subsequent runs take ~10 seconds.

You're ready when `make compose-up` exits 0. Confirm with:

```bash
curl -s http://localhost:8080/api/v1/ready
```

You should see a JSON document with `"status": "ready"`.

## 4. Point a virtual charger at the gateway

Open a second terminal so you can watch its output while you drive the REST API:

```bash
.venv/bin/python -m tools.sim --count 1 --duration 600 --target ws://localhost:19000
```

That brings up one simulated OCPP 1.6 charger called `sim-cp-001` (the default) and keeps it connected for ten minutes. You'll see structured log lines as it sends a `BootNotification`, starts heartbeating, and reports a `StatusNotification` of `Available`.

The charger is now online. You can prove that to yourself from another terminal:

```bash
curl -s http://localhost:8080/api/v1/charge-points/sim-cp-001 \
  -H "Authorization: Bearer change-me-in-dev" | python3 -m json.tool
```

`last_boot_at` is freshly populated and the `connectors` array shows the connector state.

> **Note on auth.** The local stack ships with `EVEYS_OCPP_REST_INBOUND_TOKENS=change-me-in-dev`. That's a development default. Never set the same value in production — see [`concepts/security-model.md`](./concepts/security-model.md).

## 5. Send a command

Tell the gateway to start a charging session on connector 1:

```bash
curl -s -X POST http://localhost:8080/api/v1/charge-points/sim-cp-001/commands/remote-start \
  -H "Authorization: Bearer change-me-in-dev" \
  -H "Content-Type: application/json" \
  -d '{"id_tag": "TEST_TAG_001", "connector_id": 1}' \
  | python3 -m json.tool
```

The response is `{"status": "Accepted", ...}`. The simulator's terminal will print a `StartTransaction.req` and the gateway will reply with a `transactionId` (the simulator logs both sides).

## 6. Watch the event land in Kafka

Open a third terminal. The Kafka container is reachable on `localhost:9092` from the host:

```bash
docker exec -it eveys-ocpp-kafka \
  kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic tx.started \
    --from-beginning
```

You'll see a single protobuf-encoded message: the `EventEnvelope` carrying the `TxStarted` payload that the gateway published when your `RemoteStart` resulted in a `StartTransaction`. (It's binary; for a human-readable view, [`reference/events.md`](./reference/events.md) shows how to deserialise it.)

## 7. Stop the session

```bash
curl -s -X POST http://localhost:8080/api/v1/charge-points/sim-cp-001/commands/remote-stop \
  -H "Authorization: Bearer change-me-in-dev" \
  -H "Content-Type: application/json" \
  -d '{"transaction_id": 1}' \
  | python3 -m json.tool
```

(Replace `1` with whatever `transactionId` came back in step 5.)

A `tx.stopped` event lands on the corresponding Kafka topic. Both events are also delivered as webhooks if you configure a destination — see [`guides/consume-events.md`](./guides/consume-events.md).

## 8. Tear it down

```bash
make compose-down
```

This stops the containers but **keeps your data volumes**. Next `make compose-up` resumes against the same state.

If you want a clean slate:

```bash
make compose-down-volumes
```

Drops the volumes too. Destructive — only use it when you want to start over.

---

## What just happened

You ran the gateway against a real data plane. A virtual charger logged in over WebSocket, you sent a backend-style REST call, the gateway found the charger's socket and dispatched the OCPP `RemoteStartTransaction.req`, the charger replied, the resulting `StartTransaction` got persisted to Postgres, and the gateway published a `TxStarted` envelope to Kafka.

That's the whole loop. Everything else in this documentation set elaborates on one of those steps.

---

## Where to go from here

- **Curious how the pieces fit together?** [`03-architecture.md`](./03-architecture.md).
- **Want to connect a real charger?** [`guides/connect-a-charger.md`](./guides/connect-a-charger.md).
- **Building a backend integration?** [`guides/use-the-rest-api.md`](./guides/use-the-rest-api.md) and [`guides/consume-events.md`](./guides/consume-events.md).
- **Want to understand the OCPP terms you just saw?** [`04-glossary.md`](./04-glossary.md).
