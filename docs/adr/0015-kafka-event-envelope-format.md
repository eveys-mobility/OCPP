# ADR-0015 — Kafka event envelope format

- **Status**: Accepted
- **Date**: 2026-04-30
- **Author**: Eveys engineering
- **Reviewers**: TBD

## Context

`eveys/ocpp` publishes a high-volume event firehose to Kafka — one record per relevant OCPP message. ADR-0004 already chose Kafka → ClickHouse as the path for high-volume telemetry; this ADR is about the **wire format** of those Kafka records, not the storage choice downstream.

Concretely we needed to decide:

1. **Encoding** — JSON, Avro, or protobuf?
2. **Topic-per-event-type vs single envelope on multiple topics** — every record `{Foo}` on its own topic, or every record wrapped in a common envelope and discriminated by a field?
3. **Where the envelope's metadata lives** — the same fields (server-receive time, trace id, schema version, charger id) repeat across every event type. Are they per-payload, or factored out?
4. **Partition key** — what guarantees per-charger ordering?
5. **Topic count** — one mega-topic with all events, or one topic per logical stream?

Constraints:

- Per-charger message ordering must be preserved (AGENTS rule). The ClickHouse consumer and the future mobile-BFF consumer both rely on it.
- Consumers are polyglot: ClickHouse (Kafka Engine table or sidecar), Python services, future Go services, future TypeScript BFF. The format must have well-supported codegen in all four.
- The OCPP gateway is the producer for *all* of these events; downstream consumers must not need to coordinate schema changes with each other.
- We expect schema evolution: every new OCPP version (2.0.1, 2.1) and every new platform feature will add fields. Removing or renumbering must not be needed in normal operation.

## Decision

**One canonical `EventEnvelope` protobuf message, fanned out across five logical topics, with `cp_id` as the partition key.**

- **Encoding**: protobuf v3, package `eveys.events.v1` (frozen at end of W3 — see `proto/events/v1/events.proto`).
- **Envelope shape**: a single `EventEnvelope` with metadata fields (`event_id`, `occurred_at`, `cp_id`, `schema_version`, `trace_id`) plus a `oneof payload` discriminator carrying the event-specific message (`CpConnected`, `CpBoot`, `CpStatus`, `CpMeter`, `TxStarted`).
- **Topics** (five): `cp.connected`, `cp.boot`, `cp.status`, `cp.meter`, `tx.started`. Each topic only receives envelopes whose `oneof payload` is set to the matching variant — the topic name and the payload type agree by convention.
- **Partition key**: `cp_id` is set as the Kafka record key on every produce. This guarantees a single ordered partition per charger.
- **Schema evolution rules** (encoded as a comment in `events.proto`): adding fields is allowed and consumers must ignore unknown ones; removing or renumbering fields is forbidden until a `v2` package; the `oneof payload` discriminator is the version-tolerant switch consumers branch on.

## Alternatives considered

- **JSON on each topic, no envelope** — simplest. Rejected: no codegen, no compile-time guarantees on field names, and JSON is ~3–5x larger on the wire than protobuf for the same `MeterValues` payload. At ~10⁵–10⁶ rows/min the bandwidth and storage difference is real money.

- **Avro with Confluent Schema Registry** — well-established in Kafka ecosystems; built-in schema evolution rules. Rejected for now: Avro adds a hard dependency on a schema registry we don't yet operate (planned in ADR-0014), and the polyglot codegen story for protobuf is at least as good for our consumer mix. We can revisit if the schema-registry ADR lands and the migration cost is small.

- **Topic-per-event-type without an envelope** (`MeterValues` as a bare `CpMeter` proto on `cp.meter`) — would have saved 5 metadata fields per record. Rejected: the metadata is genuinely cross-cutting (every consumer wants `event_id` for dedup, `occurred_at` for time-ordering, `trace_id` for correlation, `schema_version` for rolling schema changes). Putting it in one place lets a single shared library on each consumer extract it.

- **One envelope, one mega-topic** — every event on a single topic, consumers filter by `oneof` discriminator. Rejected: forces every consumer to read every event type. The ClickHouse `MeterValues` table only cares about `cp.meter`; the mobile BFF only cares about `cp.status`. Per-topic separation lets each consumer pick what it needs without a fan-out filter.

- **Use `message_id` (the OCPP wire-message UUID) as the partition key** instead of `cp_id` — would give finer-grained parallelism. Rejected: it breaks per-charger ordering, which is the property we explicitly need. `cp_id` is the right key precisely *because* it concentrates a charger's traffic on one partition.

## Consequences

### Positive

- **One schema repo, many languages.** `proto/events/v1/events.proto` is the contract. ClickHouse, Python, Go, TypeScript consumers all generate from it; no manual struct mirroring.
- **Cheap evolution.** Adding fields to any payload (new OCPP measurand, new metadata) is a non-event for existing consumers — protobuf v3 default-zero handles it.
- **Per-charger ordering preserved automatically.** Producers don't have to think about partition assignment; they hand `cp_id` to the producer and the broker does the rest.
- **Cross-cutting metadata in one place.** `event_id`, `occurred_at`, `trace_id`, `schema_version` are extracted once per consumer.
- **Server-receive time is authoritative.** The envelope's `occurred_at` is the gateway's clock; the charger's claimed timestamp lives inside the payload (`charger_reported_at`). Consumers know which to trust without a footnote.
- **Trace correlation is built in.** `trace_id` flows from the gRPC + WS hop into the Kafka record, so an operator can pivot from a Grafana dashboard to a single trace in Tempo / Jaeger.

### Negative / costs

- **Five topics, five Kafka consumer offsets per consumer.** Operationally a little more to monitor than one topic. The split is justified by the consumer-isolation argument above.
- **`oneof` discriminator carries a small per-record overhead** — one extra varint tag. Negligible at our volumes.
- **Protobuf codegen is required everywhere.** `make protoc` is now part of `make install`, and CI's `before_script` runs it (see `.gitlab-ci.yml`). New consumers in other repos must mirror this.
- **Proto stubs are generated, not committed.** Means engineers running unfamiliar IDEs need to know to run `make protoc` first. Documented in `AGENTS.md` and `docs/03-coding-standards.md`.

### Risks

- **Schema drift between producer and consumer** during a rolling deploy — a new field added to a payload that an old consumer doesn't yet know about. Mitigation: protobuf's "ignore unknown fields" default handles forward-compat by design; we still need an integration check that flags a *removed* field at MR time. Tracked under ADR-0014 (schema registry) — a registry would catch this automatically.
- **Topic-name to payload-type drift** — nothing at the Kafka layer enforces that `cp.meter` only ever carries `CpMeter` envelopes. Mitigation: the producer is centralized in `eveys/ocpp` (`KafkaEventProducer.publish(topic=...)` is called from one place per handler), so misuse would show up in code review. A consumer that receives the wrong payload type can branch on the `oneof` and reject.
- **Partition skew** — a small number of very busy chargers (think public DC-fast hubs) could overload one partition. Mitigation: the `MeterValues` topic is intentionally given more partitions in the cluster config (see deploy chart) so that even a hot charger has enough headroom. If a single charger ever saturates a partition, we'd shard by `(cp_id, connector_id)` for `cp.meter` only — a known escape hatch.
- **`oneof` evolution** — adding a sixth payload variant (e.g., `CpFirmware`) is a forward-compatible change in protobuf v3, but a consumer that assumes "always one of these five" must be hardened to handle "unset / unknown variant". Convention: every consumer's `match envelope.payload` switch must have a default arm.

### Reversibility

- **Reversible at moderate cost.** The producer (`KafkaEventProducer`) is one Python class behind a `Protocol`; the protobuf is one file in `proto/`. Switching to Avro or another framing would mean: (1) regenerate consumer stubs; (2) dual-write for one release window; (3) cut consumers over. Roughly one engineer-week of work — not a one-way door.

## Project conventions implied by this decision

- All Kafka events emitted by `eveys/ocpp` use `EventEnvelope`. Bare proto messages on a topic are forbidden.
- Producers always set the Kafka record key to `cp_id`. No exceptions.
- New event types add a new `oneof payload` variant *and* a new topic name. Both go in the same MR as the proto change.
- ClickHouse table schemas live alongside the protos in `proto/events/` (per ADR-0004) — when an envelope variant changes shape, the matching ClickHouse DDL changes in the same MR.
- The OCPP charger's claimed timestamp is never the envelope's `occurred_at`; it goes inside the payload as `charger_reported_at` (or equivalent). AGENTS rule 7 — charger clocks are untrusted.

## References

- [`proto/events/v1/events.proto`](../../proto/events/v1/events.proto) — the canonical schema this ADR formalizes.
- [ADR-0004 — ClickHouse as the time-series store](./0004-clickhouse-timeseries-store.md) — the storage end of the same data path.
- ADR-0014 (planned) — Schema Registry choice for Kafka events. Will revisit "JSON / Avro / protobuf" if a registry materially changes the trade-offs.
- [`AGENTS.md`](../../AGENTS.md) — hard rule 4 (`MeterValues` to Kafka, never Postgres) and the per-charger-ordering rule.
- [`docs/02-tasks.md`](../02-tasks.md) — E2-3 (proto frozen end of W3), E2-7 (producer), E2-8 (per-handler wiring).
