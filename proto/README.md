# proto/

Frozen v1 protobuf contracts for `eveys/ocpp`.

Two trees, both independently versioned:

| Path | What | Task |
|---|---|---|
| [`ocpp_gw/v1/gateway.proto`](./ocpp_gw/v1/gateway.proto) | gRPC service the platform calls *on us* to control chargers | E2-2 |
| [`events/v1/events.proto`](./events/v1/events.proto) | Kafka event envelopes we publish to the rest of the platform | E2-3 |

## Stability

These protos are **frozen for v1**. Per the project conventions:

- **Adding fields** is allowed. Consumers must ignore unknown fields.
- **Removing or renumbering fields** is forbidden in v1. Such changes go in a `v2/` sibling directory.
- **Adding new RPCs** is allowed.
- **Removing RPCs** requires either a deprecation cycle (`option deprecated = true;`) or a `v2`.

The `v1/` directory naming makes the boundary explicit and survives major bumps without breaking existing consumers.

## Code generation

Generated code is **not committed**; it's produced at build time. Per ADR-0001 we use `grpclib` (async-native) on the server side, so generated stubs live in:

- Python (server + Python clients): `src/eveys_ocpp/_generated/` (gitignored), produced from `pyproject.toml`'s build hook
- Other languages (Go, TypeScript): downstream teams generate into their own repos

A `make protoc` target lands with task **E2-4** (gRPC server scaffolding).

## Conventions

- **`cp_id`** is field number 1 on every charger-targeted RPC.
- **gRPC error model**: canonical `google.rpc.Status` codes for transport-level outcomes (`NOT_FOUND`, `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `FAILED_PRECONDITION`, `INVALID_ARGUMENT`). OCPP-level outcomes (Accepted / Rejected / NotSupported) are typed enums in the response message.
- **Kafka envelope**: every event uses `EventEnvelope` with a `oneof payload` discriminator. Partition key is always `cp_id` so each charger's stream is single-consumer-ordered (per AGENTS rule "message ordering is preserved per charger").
- **Timestamps**: ISO-8601 UTC strings, two of them — the envelope's `occurred_at` is the *server-receive* time (trustworthy); per-payload `charger_reported_at` is the *charger-claimed* time (untrusted; AGENTS OCPP rule 7).
- **Buf-style** linting will be added with task E2-12 (gRPC backward-compat tests in CI).

## Conformance pointer

Each gRPC command maps to one or more OCA Appendix C test IDs in [`docs/08-ocpp-conformance.md`](../docs/08-ocpp-conformance.md). Adding an RPC also requires a 🟡 row in the conformance matrix for the OCPP message it sends to the charger (AGENTS rule 8).
