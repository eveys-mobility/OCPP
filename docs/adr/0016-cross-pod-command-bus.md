# ADR-0016 — Cross-pod command bus over Redis pub/sub

- **Status**: Accepted
- **Date**: 2026-04-30
- **Author**: Eveys engineering (E2-10; AI-assisted draft, human-reviewed and merged)
- **Reviewers**: Project tech lead (post-merge sign-off)

## Context

The gRPC command surface (E2-5, E2-6) routes charger-targeted RPCs to whichever pod owns the charger's WebSocket. Until E2-10 the off-pod branch returned `UNAVAILABLE` and required the caller to retarget — a workable stopgap for single-pod deployments but a hard block on Phase 3 platform integration and on running more than one gateway pod in production. The Redis online registry (E2-9) already publishes `cp:online:{cp_id} → pod_id`, so the *routing* decision is solved; what's missing is the *delivery* mechanism.

Forces:

- We already run Redis (registry + planned auth cache). Adding a second messaging substrate (NATS, RabbitMQ) doubles the operational surface.
- Latency budget per RPC is tight: gRPC clients use a 30s ceiling; bus overhead must stay sub-second.
- Failure mode must be predictable: a pod dying mid-RPC should produce the same gRPC error a flaky charger would, not a hang.
- Charger session counts per pod will reach ~10k at load (E4-6). Anything per-charger needs to scale O(1) in subscription count.

## Decision

Implement E2-10 as a Redis pub/sub command bus on the existing Redis pool. Two channel families:

- `cp:cmd:{cp_id}` — one publish per off-pod RPC. Every gateway pod psubscribes to `cp:cmd:*` and answers if the cp is in its local `ConnectionMap`.
- `cp:reply:{request_id}` — one publish per RPC reply. Every pod psubscribes to `cp:reply:*` and routes incoming replies to its in-flight `request_id` futures.

Envelope is JSON with a `v` field; mismatch is a hard drop. Owning-side dispatch reuses the existing OCPP request dataclasses via a small `rpc_name → dataclass` registry; the wire payload is `dataclasses.asdict(...)`.

## Alternatives considered

- **NATS request/reply** — cleaner request/reply primitive than Redis pub/sub. Rejected: adds a new dependency to deploy, monitor, and secure for a problem we can solve with infrastructure we already run.
- **Kafka request/reply on a dedicated topic** — durable, partitioned. Rejected: latency is too high for a synchronous gRPC ceiling, and Kafka request/reply patterns require correlation-ID consumer infrastructure that pub/sub solves natively. Kafka is the right tool for the firehose (E2-7/E2-8), not for command routing.
- **Pod-to-pod gRPC with service-discovery lookup** — fastest path. Rejected: requires every pod to be reachable from every other pod plus a working service-discovery story for K8s headless services, and re-implements load-balancing concerns we don't need. We can revisit if pub/sub becomes a bottleneck.
- **Per-charger Redis subscriptions** (rather than a pod-level pattern subscription) — simpler envelope filter (just listen to your own chargers). Rejected: at 10k chargers/pod the subscription count approaches Redis client-side limits and balloons NUMSUB lookups. A single `cp:cmd:*` pattern subscription per pod is O(1) regardless of charger count.

## Consequences

### Positive

- Multi-pod deployment is unblocked — the same gRPC RPC works regardless of which pod the caller hits.
- Phase 3 platform integration (auth, session, device clients) can proceed without a sticky-routing assumption.
- Operationally simple: one more Redis usage pattern on infrastructure that's already tier-1 monitored.
- Bus is internal-only; chargers never see it. Envelope can evolve via `v` without OCPP-spec coordination.

### Negative / costs

- Pub/sub is at-most-once. A pod that dies between publish and reply will produce `DEADLINE_EXCEEDED` on the requester. This is the same outcome callers already handle for flaky chargers, so no new error path — but it's a correctness property worth flagging.
- Every pod sees every reply (because we use a `cp:reply:*` pattern subscription) and discards ones not in its `_inflight` map. Cheap CPU cost, but it does scale O(N pods × replies/sec). At three-digit pod counts we'd switch to per-request subscriptions.
- Adds a dispatch registry (`_OCPP_CALL_DISPATCH`) and an envelope schema as new things that must stay in sync with proto/OCPP. Mitigated by keeping the registry in the same file as the gRPC method bodies that already use it.
- Bus envelope is JSON, not protobuf. Marginal CPU cost; we accept it because the bus is internal and small.

### Risks

- **Redis being down means the bus is down.** Single-pod RPCs still work (same-pod path bypasses the bus); cross-pod RPCs will time out. We already depend on Redis for the online registry, so this isn't a new failure mode. Detection: existing Redis SLO + bus error log volume on `bus.request.timeout`.
- **Subscription cardinality at scale.** Pattern subscriptions dedupe across clients in `PUBSUB NUMPAT`. Per-pod CPU cost of receiving and discarding non-local messages grows linearly with cluster RPC volume. Monitor: per-pod CPU on `bus-cmd-subscriber`/`bus-reply-subscriber` tasks.
- **Forward-version skew during rolling deploys.** A v2 envelope arriving at a v1 pod is hard-dropped (logged, no reply). The v2 caller will see `DEADLINE_EXCEEDED`. Mitigation: deploy version bumps in lock-step or add a v2-or-fall-back path.

### Reversibility

Reversible. Switching to NATS, gRPC pod-to-pod, or per-request Redis subscriptions is a swap of the `CommandBus` implementation behind the existing `bus` dependency injection point in `OcppGatewayService`. The dispatch registry, envelope schema, and gRPC translators all stay. Estimated migration: 1–2 days plus an integration test sweep.

## References

- E2-9 (`docs/02-tasks.md`) — the online registry that this builds on.
- E2-5 / E2-6 (`docs/02-tasks.md`) — the gRPC command bodies whose `UNAVAILABLE` branch this ADR replaces.
- `src/eveys_ocpp/bus.py` — implementation.
- `src/eveys_ocpp/transport/grpc_server.py` — `_OCPP_CALL_DISPATCH` registry, `_call_via_bus`, `_dispatch_local_for_bus`.
- `tests/e2e/test_two_pod_dispatch.py` — acceptance test for the "two-pod test passes" criterion.
