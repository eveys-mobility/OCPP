# ADR-0023 — Backend REST integration: two surfaces, asymmetric envelope

- **Status**: Accepted
- **Date**: 2026-05-05
- **Author**: Eveys engineering (E3 platform integration; AI-assisted draft, human-reviewed)
- **Reviewers**: Project tech lead

## Context

Phase 3 connects `eveys/ocpp` (the OCPP gateway) to the Eveys backend. The integration target is a single backend speaking REST.

That single decision drives several follow-on choices that needed locking before any code lands:

1. **REST or gRPC** for the integration surface?
2. **One or two REST surfaces** — does the gateway only consume the backend's API, or does it also expose its own API for the backend to consume?
3. **Response envelope** — symmetric across both directions or asymmetric?
4. **How does the backend read time-series MeterValues**, given ADR-0004 says `eveys/ocpp` doesn't read from ClickHouse?
5. **Webhook signing scheme**.

Each had cheap-but-irreversible defaults that would shape the integration code; this ADR captures the calls.

## Decision

**1. REST.** Both directions use HTTP+JSON. The backend already speaks REST; adding gRPC would force the backend team to onboard `grpclib` / proto generation when they don't need to. The gateway keeps gRPC alongside REST for cross-pod command routing (E2-10) and as an alternative path for callers that want the lower-overhead binary protocol.

**2. Two surfaces, both documented:**

- **Backend-side** (`<backend>/api/eveys/...`, owned and implemented by the backend team) — synchronous OCPP-hot-path calls (`/authorize`, `/sessions/open`, `/sessions/close`, `/charge-points/register`, `/health`). Specified in [`docs/integration/01-backend-rest-contract.md`](../integration/01-backend-rest-contract.md).
- **Gateway-side** (`<gateway>/api/v1/...`, owned and implemented by the gateway) — read endpoints for state and time-series, plus 19 command endpoints wrapping the OCPP CSMS-initiated RPCs. Specified in [`docs/integration/02-gateway-rest-api.md`](../integration/02-gateway-rest-api.md).

The two-surface split is necessary because the integration is genuinely bidirectional: the gateway needs synchronous backend answers on the OCPP hot path (Authorize, session open/close), and the backend needs to query gateway-owned state (per-charger MeterValues, transaction lists, charger presence) and issue OCPP commands. Forcing all of that through a single direction would bloat one surface and atrophy the other.

**3. Asymmetric response shape**:

- **Backend endpoints** wrap every response in `{ "success", "data", "message" }` with `error_code` on failures.
- **Gateway endpoints** return raw JSON on success ("the response *is* the data") and a flat `{ "error", "error_code", "request_id" }` on failure.

The asymmetry is conscious: each surface uses the idiom that fits its style. Backend services often wrap; gRPC-style services usually don't. A backend developer consuming both surfaces remembers a single rule: **outbound (gateway → backend) returns the envelope; inbound (backend → gateway) returns raw JSON**. This rule is documented at the top of every integration doc.

**4. Gateway exposes a ClickHouse-backed read path.** `GET /api/v1/charge-points/{cp_id}/meter-values` and `/status-history` query ClickHouse via the gateway's own connection. This is a deliberate amendment to [ADR-0004](./0004-clickhouse-timeseries-store.md) §"`eveys/ocpp` does not query ClickHouse" — the backend doesn't read ClickHouse directly because that would couple the backend to a schema the gateway owns. Going through the gateway keeps schema evolution under one team's control.

**5. Webhook signing**: HMAC-SHA-256 over the raw request body, sent in `X-Eveys-Signature: sha256=<hex>`. Plus `X-Eveys-Event-Id` for idempotent dedup, `X-Eveys-Event-Type` for routing, `X-Eveys-Delivered-At` for ops. Same secret on both sides via `EVEYS_OCPP_WEBHOOK_SECRET`. At-least-once delivery with 5-attempt exponential backoff. Specified in [`docs/integration/03-webhooks.md`](../integration/03-webhooks.md).

**6. Authentication**: bearer tokens in both directions for v1; mTLS will overlay in Phase 5 (E5-5). One token per consumer per direction; rotation through whatever process the issuing side already uses.

## Alternatives considered

- **gRPC for both directions**, like the cross-pod bus. Rejected: the backend doesn't already speak gRPC, the operational tooling (browser dev tools, curl debugging, monitoring) is HTTP-native, and the overhead savings don't matter at the volumes Authorize / sessions/open run at (max ~10/s/charger × O(10k) chargers = O(100k)/s peak; HTTP+JSON handles it).

- **Single envelope on both surfaces** (option (A) considered first). Rejected at the user's instruction: gateway-side reads look bloated with `{ "success", "data" }` around every payload, and gRPC-shaped services usually don't wrap. The asymmetric rule is one extra thing to remember; the cleaner shape on each side is worth it.

- **Backend reads ClickHouse directly.** Rejected: couples the backend to the gateway's column schema, blocks the gateway from evolving the schema independently, and violates ADR-0004's "one writer" intent. The gateway becomes the single point of schema ownership.

- **One backend endpoint per OCPP-message type instead of business-shaped.** Rejected: forces the backend to learn OCPP semantics. The gateway's job is to translate OCPP into business operations the backend already understands (authorize a user, open/close a session). Backend endpoints are domain-shaped.

- **Webhook delivery via Kafka topic mirror only.** The Kafka firehose already exists (E2-8). Rejected as the *only* mechanism: backends that don't speak Kafka would be left out. Webhooks are documented as an alternative; the Kafka topics remain authoritative and replayable.

## Consequences

### Positive

- **Clean separation of concerns.** Backend owns auth / sessions / users / billing; gateway owns OCPP state / time-series / commands. Each surface evolves independently.
- **Low operational overhead.** REST + JSON + bearer token is the standard everyone's tools already understand.
- **Backend can choose subscription style.** Webhooks for low-volume events; Kafka for high-volume (`cp.meter`); gateway REST polling for one-off reads.
- **Phase 5 mTLS overlays cleanly.** Tokens stay as a defence-in-depth layer; mTLS handles the wire.

### Negative / costs

- **Two surfaces means two operational stories.** Two sets of monitoring, two sets of credentials, two sets of failure modes. Mitigated by the conventions in [`README.md`](../integration/README.md) — same auth, same correlation IDs, same timestamp shape, same idempotency mechanic.
- **Asymmetric envelope means one extra thing to remember.** Mitigated by documenting it on every doc and using consistent examples that show both shapes side by side.
- **ClickHouse-via-gateway adds a hop for time-series reads.** A direct backend-to-ClickHouse read would be one fewer network hop and one fewer process. The hop is the cost of schema ownership.

### Risks

- **Backend hot-path latency.** The gateway calls `/authorize` synchronously inside an OCPP handler with a 30 s charger timeout. If the backend's P99 on `/authorize` is > 1 s, the operator's user experience degrades (charger spins). Mitigated by the configured fallback policy (default `reject`, optional `accept_offline`) and by Redis-cached `Authorize` results (E3-4) with a short TTL.
- **Webhook reliability under sustained backend outage.** 5 attempts then drop. Mitigated by Kafka subscription as the durable replay channel.
- **JSON contract drift.** Without a `buf breaking`-equivalent for JSON, additive vs breaking changes are policed manually. Mitigated by the convention "additive ≤ breaking; new versions bump path to `/api/v2/...`."

### Reversibility

Reversible. Switching either surface to gRPC, or adding a third (e.g. GraphQL for an analytics dashboard), is purely additive — the existing surfaces stay until consumers migrate. Tearing down the two-surface model and consolidating into one direction is harder; it would require either the backend or the gateway to become the sole owner of state currently split between them, which is a deeper architectural change than this ADR attempts.

## References

- [`docs/integration/README.md`](../integration/README.md) — index for the three sub-docs.
- [`docs/integration/01-backend-rest-contract.md`](../integration/01-backend-rest-contract.md) — backend-side endpoints.
- [`docs/integration/02-gateway-rest-api.md`](../integration/02-gateway-rest-api.md) — gateway-side endpoints.
- [`docs/integration/03-webhooks.md`](../integration/03-webhooks.md) — webhook event catalog.
- [ADR-0004](./0004-clickhouse-timeseries-store.md) — ClickHouse as the time-series store. This ADR amends §"`eveys/ocpp` does not query ClickHouse" with a narrow exception for the gateway-exposed read path.
- [ADR-0015](./0015-kafka-event-envelope-format.md) — Kafka envelope format.
- [`docs/02-tasks.md`](../02-tasks.md) — Phase 3 task IDs.
