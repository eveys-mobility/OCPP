# ADR-0011 — Internal mTLS: Envoy ↔ gateway

- **Status**: Accepted
- **Date**: 2026-05-08
- **Author**: Eveys engineering (E5-5)
- **Reviewers**: Project tech lead

## Context

The roadmap (Phase 5 / E5-5) calls for "mTLS between internal
services. Pod-to-pod calls require valid cert." Mapping that to the
actual hops in this codebase, only one is a real candidate for
gateway-code-level mTLS:

| Hop | mTLS candidate? |
|---|---|
| Charger → Envoy | No — that's edge TLS (E5-1, ADR-0007), already done |
| **Envoy → gateway pod** | **Yes — this ADR** |
| gateway → gateway (gRPC bus) | Bus is via Redis pub/sub (ADR-0016); no direct pod→pod gRPC |
| gateway → Postgres / Redis / Kafka / ClickHouse | Operator/platform config (TLS-on-DSN), not gateway code |
| gateway → backend (REST) | Already TLS — server-cert + bearer; mTLS upgrade is a backend-team coordination |

The Envoy → gateway leg is **the in-cluster authentication
boundary**. Without mTLS, anything that can reach `:9000` (any
sidecar, any debugger, any compromised pod) can open a charger
WebSocket. The gateway's idempotency / rate-limit / sanity layers
mitigate damage but they don't prevent imposterhood.

## Decision

The gateway's WS server can require **client TLS authentication**
on the inbound socket via `ssl.CERT_REQUIRED`. Envoy presents a
client certificate on its upstream cluster; the gateway verifies it
against a private CA. Disabled by default (`ws_mtls_enabled=False`)
so dev / compose / e2e stay on plain WS; enabled in production via
the Helm chart's `gateway.mtls.enabled=true`.

Concretely:

- Four new `Settings` fields: `ws_mtls_enabled`, `ws_mtls_cert_path`,
  `ws_mtls_key_path`, `ws_mtls_ca_path`.
- `transport/_tls.py::build_server_ssl_context` builds the
  `SSLContext` at boot; missing paths fail loud.
- `transport/ws_server.serve_forever` passes the context to
  `websockets.serve(..., ssl=ctx)` when present, runs plain WS
  otherwise.
- Envoy production config (`deploy/envoy/envoy.yaml`) grows a
  `transport_socket: tls` on the upstream cluster.
- Helm chart provisions a single TLS Secret referenced by both
  the gateway pod (server side) and the Envoy pod (client side)
  so cert rotation is one-Secret-update.

## Alternatives considered

- **mTLS via a service mesh (Linkerd, Istio, Cilium ClusterMesh)**.
  Standard answer for "internal mTLS" in larger organisations.
  Rejected for now: introduces a third runtime substrate
  (sidecars per pod, control plane, certificate authority of its
  own) that nobody on the team operates today. The Envoy↔gateway
  shape we already have absorbs the same security goal with the
  components we already deploy. Worth revisiting when the
  platform adds a mesh for unrelated reasons.
- **SPIFFE / SPIRE**. The strict, audit-friendly answer to
  workload identity. Same rejection as service mesh — adds a
  control plane we don't operate. The mTLS shape here is
  *compatible* with SPIRE: when SPIFFE lands, the Helm chart's
  Secret reference becomes a SPIRE-Workload-API mount, and the
  gateway code doesn't change.
- **Single-listener, no mTLS, lock down the network instead**
  (NetworkPolicy + namespace boundaries). Rejected as the
  primary defence: NetworkPolicy is a defence in depth, not an
  authentication boundary. A misconfigured NetworkPolicy is a
  hole; a missing client cert is a clear, observable rejection.
- **Verify by client-cert subject (CN / SAN) in the gateway**.
  Rejected because the verify-by-CA is sufficient for the
  threat model: anyone with a CA-signed cert is authorised.
  Adding subject pinning would tighten the boundary but
  require coordinating cert subjects with Helm values. Skip
  unless we learn the CA-grant is too broad.

## Consequences

### Positive

- **Real authentication boundary** between Envoy and the gateway.
  An attacker who lands in the cluster can't impersonate Envoy
  without the client cert.
- **Cert rotation is one operation** — replace the TLS Secret;
  both sides pick up the new material on pod restart (or
  cert-manager automation, depending on the source).
- **Compatible with SPIRE / service-mesh** future migration. The
  Settings + helper boundaries don't bake in any cert-source
  assumptions.

### Negative / costs

- **Operator burden**. Provisioning the CA + cert + key Secret
  is now a precondition for the production deploy. The Helm
  chart's `required:` directive on `gateway.mtls.secretName`
  surfaces this loud at deploy time.
- **One more rotation surface**. If the cert expires and nobody
  rotates, charger WS upgrades fail. cert-manager + a
  monitoring alert close that gap; the operations runbook needs
  to document it.
- **Compose-mode complexity skipped**. Compose dev stays plain
  WS. That's a deliberate trade-off — adding compose-mode mTLS
  introduces friction with no security gain at the dev-laptop
  scope. The Tier-3 compose-smoke tests therefore don't
  exercise mTLS; an integration drill against staging is the
  right validation.

### Risks

- **Envoy cert expires while gateway runs forever**. Envoy stops
  being able to open new upstream connections; existing WS
  sessions stay alive. Detect via cert-expiry alerts on the
  Secret; the failure mode is loud (5xx on new connections,
  flat in logs).
- **Path leak ≠ secret leak.** A leaked Settings dump now
  reveals the cert *paths*, not the cert *values* — but that
  helps an attacker who's already on the pod. Mitigated by
  proper file-mode on the Secret mount (k8s default 0644 for
  data, 0600 for the SecretVolumeSource).
- **A widened CA bundle effectively disables the auth.** Mount
  a tightly-scoped private CA, never a public root.

### Reversibility

Fully reversible. `ws_mtls_enabled=False` flips the gateway back to
plain WS without code changes. The Envoy `transport_socket: tls`
block can be removed via a Helm-values toggle (default values keep
it template-rendered; without the matching Secret mount, Envoy
fails to start, which is the loud-fail intended).

## References

- [ADR-0007](./0007-envoy-as-the-load-balancer.md) — Envoy is the LB.
- `src/eveys_ocpp/transport/_tls.py` — `SSLContext` builder.
- `deploy/envoy/envoy.yaml` — `transport_socket: tls` on the upstream.
- `deploy/helm/eveys-ocpp/values.yaml` — `gateway.mtls` and `envoy.upstreamMtls`.
- Python `ssl` docs:
  [`CERT_REQUIRED`](https://docs.python.org/3/library/ssl.html#ssl.CERT_REQUIRED).
- websockets docs:
  [`serve(... ssl=...)`](https://websockets.readthedocs.io/en/stable/reference/asyncio/server.html).
