# ADR-0007 — Envoy as the load balancer

- **Status**: Accepted
- **Date**: 2026-05-08
- **Author**: Eveys engineering (E5-1)
- **Reviewers**: Project tech lead

## Context

The gateway holds long-lived charger WebSockets, one per `cp_id`,
typically 24/7. The cross-pod model (ADR-0016 — Redis-backed
registry + pub/sub command bus) only works if **the same charger
keeps landing on the same pod across reconnects**. Without
sticky-by-`cp_id` routing, every brief WS drop forces a registry
ownership change, an idempotency-cache state migration, and a
cross-pod gRPC bus round-trip on every command — not unworkable,
but the cache hit-rate degrades and the bus traffic explodes.

Concretely, the load balancer needs to:

1. **Consistent-hash on a custom key** (the `cp_id` carried in the
   URL path). Round-robin and least-connections are wrong for this
   workload.
2. **Terminate TLS** so chargers speak `wss://` while the gateway
   stays plain HTTP/1.1 internally. The gateway never sees a TLS
   handshake; that's an operations concern, not a protocol
   concern.
3. **Speak WebSocket** — preserve the HTTP/1.1 upgrade, never reap
   long-idle streams the way short-RPC defaults do.
4. **Health-check upstreams on a real signal**, ideally an HTTP
   path. Pairs with the graceful-drain readiness endpoint (PR #43).
5. **Slow-start a recovering pod** so a freshly-restarted pod
   doesn't get the entire fleet's reconnect storm at once.

A future evolution will add per-IP rate-limit (E5-2), Basic Auth
at the WS edge (E5-6), and mTLS to the gateway (E5-5). Picking a
load balancer that absorbs those naturally, rather than pushing
each into a separate sidecar, is preferable.

## Decision

**Envoy** is the load balancer. Static config in
`deploy/envoy/envoy.yaml` and `envoy.local.yaml`, deployed via
the Helm chart at `deploy/helm/eveys-ocpp/`. Routing uses
`RING_HASH` LB policy with `hash_policy.header: ":path"`; the
`cp_id` lives in the URL path per the gateway's WS server, so
hashing the path gives consistent-hash on `cp_id`.

## Alternatives considered

- **Nginx OSS**. Solid, well-known, lighter than Envoy. Rejected
  because consistent-hash on a *custom* request component requires
  either Nginx Plus (paid) or one of several third-party modules
  (`ngx_http_upstream_hash_module` variants); the GA OSS shape
  hashes only on a fixed set of variables (`$remote_addr`,
  `$cookie_*`, etc.). The `cp_id`-from-path requirement isn't
  reachable without leaving the supported configuration surface.
- **AWS ALB (Application Load Balancer)**. Native to AWS, manages
  TLS via ACM, supports WebSockets. Rejected because ALB doesn't
  do consistent-hash at all — it routes by target-group affinity
  (cookie-based) which doesn't apply to chargers (no cookie jar)
  and round-robin otherwise. The cross-pod registry model would
  fail under ALB.
- **AWS NLB (Network Load Balancer)**. L4 only. Rejected because
  consistent-hash on application-level data (the URL path) is
  impossible at L4 — the load balancer never sees the HTTP request.
  Also can't terminate TLS such that the gateway sees plain HTTP.
- **HAProxy**. Capable, mature, supports consistent-hash. Rejected
  for ergonomic reasons more than technical: configuration is a
  flat-file format with no native templating support, the WebSocket
  story is workable but less first-class than Envoy's, and the
  team has Envoy operational experience from sibling services. We
  would revisit if Envoy ever turns out to be too heavy at the
  fleet scale we're projecting (10k+ chargers).
- **Kubernetes Service of type LoadBalancer with `service.spec.sessionAffinity: ClientIP`**.
  Rejected because client IP isn't a useful sticky key — many
  chargers can NAT to a single IP (e.g. fleet operator's office
  network), defeating per-charger stickiness. Also: no ability to
  hash on the URL path.

## Consequences

### Positive

- **Cross-pod model works.** Charger-to-pod stickiness preserves
  registry ownership and minimises bus traffic.
- **TLS terminates at the edge.** Gateway code stays HTTP/1.1
  internally; certs live where they belong (cert-manager / vault),
  not in the application config.
- **The filter chain is the right hook for E5-2 + E5-6 + E5-5.**
  Per-IP rate limit, edge auth, and mTLS to the upstream all fit
  into `http_filters` / `transport_socket` slots that already exist
  in `envoy.yaml`. No new substrate.
- **Health checks pair with graceful drain.** Envoy probes
  `/api/v1/ready` (PR #43); a 503 here removes the pod from the
  rotation before SIGTERM tears it down.

### Negative / costs

- **Operational footprint.** Envoy is more configuration surface
  than Nginx or a managed cloud LB. The `deploy/envoy/` files
  + Helm chart are real artifacts to maintain.
- **Two deploy components** instead of one. The Helm chart
  ships a gateway Deployment + an Envoy Deployment with their
  own PDB / Service / readiness probes. Twice the failure
  surface than a single managed LB.
- **xDS / dynamic config not used yet.** Static YAML is
  deliberately the v0 — operator pushes a new ConfigMap for
  config changes, gets a rolling Envoy restart. Fine for
  occasional tuning; insufficient when config-as-data starts
  changing on the order of hours.

### Risks

- **The `:path` hash key is a contract with the URL shape.** If a
  future protocol evolution puts `cp_id` somewhere else (a query
  string, a custom header, OCPP 2.0.1 connection profile shape),
  the hash key becomes inconsistent and every charger reshuffles
  across pods. **Mitigation**: this ADR documents the coupling
  loudly; any URL-shape change goes through an Envoy config change
  in the same PR. The `deploy/envoy/README.md` calls it out at the
  top.
- **Slow-start on RING_HASH is partial.** Envoy's `slow_start_config`
  is on `ROUND_ROBIN` / `LEAST_REQUEST` policies; with `RING_HASH`,
  the equivalent is the consistent hash itself — most existing
  chargers keep their existing pod, only new connections hash to
  the new pod, which produces a natural ramp. Documented as a gap;
  revisit if the reconnect-storm scenario (E4-7) ever shows a
  newly-recovered pod getting slammed.

### Reversibility

Reversible at the cost of one staging-cluster cutover. The
cross-pod model is what depends on consistent-hash, not Envoy
specifically — moving to HAProxy or Nginx-Plus later would
preserve every other architectural decision.

## References

- [ADR-0016](./0016-cross-pod-command-bus.md) — the cross-pod model
  that requires sticky-by-`cp_id` routing.
- [PR #43](https://github.com/eveys-mobility/OCPP/pull/43) —
  graceful-drain readiness endpoint Envoy health-checks.
- `deploy/envoy/envoy.local.yaml` — compose-mode dev config.
- `deploy/envoy/envoy.yaml` — k8s production config.
- `deploy/helm/eveys-ocpp/` — Helm chart that deploys both gateway
  and Envoy.
- Envoy docs:
  [`ring_hash_lb`](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/load_balancers#ring-hash),
  [`hash_policy`](https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/route/v3/route_components.proto#envoy-v3-api-msg-config-route-v3-routeaction-hashpolicy).
