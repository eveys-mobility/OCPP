# Security model

**Audience.** Anyone preparing this gateway for production or doing a security review.

**What this answers.** What the gateway trusts, what it does not, where the authentication boundaries are, and the threat model in one page.

> The operational levers (TLS, mTLS, tokens, secrets) live in [`../guides/deploy-to-production.md`](../guides/deploy-to-production.md). This page is the *why* — the mental model behind those levers.

---

## The boundaries

The gateway sits between two kinds of system: chargers (low-trust, deployed in the field, sometimes physically accessible to the public) and your platform (higher-trust, inside your own infrastructure). Every connection crossing those boundaries has an authentication mechanism and a clear answer to "what does the gateway accept based on what evidence".

```
                    Chargers
        (low trust; field-deployed; some are in public spaces)
                       │
                       │  wss://  ───────────────  TLS (cert from your CA or public)
                       │           charger Basic Auth (cp_id:password)
                       │           optional charger client cert (mTLS at the edge)
                       ▼
                  ┌────────┐
                  │ Envoy  │
                  └────┬───┘
                       │  TCP cleartext  ─────────  (optional) mTLS Envoy↔gateway
                       │                              gateway authenticates Envoy
                       ▼
                ┌────────────┐
                │  Gateway   │
                └────┬───────┘
                     │
        ┌────────────┼────────────────────┐
        │            │                    │
        ▼            ▼                    ▼
  Postgres /     Your backend       Webhook endpoints
  Redis /        (REST hot path)    (your endpoints)
  Kafka          - bearer token     - HMAC-SHA256 sig
  - DSN auth     - server TLS       - server TLS (yours)
                 - mTLS optional
                       ▲
                       │
                       │  Inbound REST / gRPC  ──  bearer token in `Authorization`
                       │                            (rate-limited per token)
                  Your platform
                  (your services, your dashboards, your scripts)
```

---

## What the gateway trusts

In one list, ordered from "most trusted" to "least":

1. **Settings from environment + the runtime overrides Redis key.** These are bound at startup by your platform.
2. **The TLS chain to your backend, Postgres, Redis, Kafka, ClickHouse.** Standard PKI; you control the certs.
3. **The bearer tokens you provisioned** for REST/gRPC inbound auth. Trusted because they came from your secret store.
4. **Envoy** for the edge — once authenticated via mTLS (when enabled), the gateway treats incoming WebSocket connections as coming from Envoy.
5. **The charger's Basic Auth credentials** (when the charger presents them). Trusted because the bcrypt hash matches what's in the per-charger credentials table.
6. **The charger's OCPP wire content** — *only the schema and the fields that have transport-level meaning*. The charger's claimed clock, claimed `id_tag`, claimed measurand values: these are accepted but not authoritative.
7. **Nothing else.**

---

## What the gateway does *not* trust

Spelling out what gets second-guessed:

- **The charger's clock.** Every charger-reported `timestamp` is captured and persisted, but the canonical wall-clock time for everything is the gateway's server-receive time. The boot reply tells the charger what time it is; some chargers ignore it. Don't build billing logic that uses `charger_reported_at` as truth.
- **Charger-provided identifiers.** A charger can report any `id_tag` it likes. The gateway forwards the value to your backend's `Authorize` endpoint; **your backend is the source of truth for whether this `id_tag` represents an authorized user**.
- **The charger's identity beyond Basic Auth.** Anyone who has stolen a charger's credential can impersonate it. mTLS at the edge (charger client certificate) is the additional layer when you need it; most fleets don't, but production-grade ones should.
- **Measurand sanity.** Meter readings are filtered (negative energy → quarantined; impossible voltages → dropped). A maliciously crafted `MeterValues` cannot poison ClickHouse with rubbish that breaks downstream queries.
- **Replays.** The same `BootNotification` / `StartTransaction` / `StopTransaction` can arrive twice; the gateway dedupes via the idempotency cache. See [`idempotency-and-replay.md`](./idempotency-and-replay.md).
- **Any incoming command from inside the cluster on the gateway's WebSocket port.** With internal mTLS enabled, only Envoy can speak to the gateway's WS port. Without it, a compromised sidecar in the same namespace could potentially open an OCPP socket and pretend to be a charger.

---

## Threats in scope

| Threat | Mitigation |
|---|---|
| **A bad actor on the public internet trying to open a charger socket** | TLS at the edge (Envoy); WS server requires `Authorization: Basic` on every upgrade. |
| **Stolen charger credentials** | Per-charger Basic Auth, rotatable per charger via REST. Optional charger-side mTLS as a second factor. |
| **A bad actor inside your cluster trying to send OCPP commands** | Internal mTLS (Envoy ↔ gateway). Without mTLS, NetworkPolicies are your only line. |
| **A bad actor calling REST without credentials** | Bearer-token check; rate limiting per token. |
| **A leaked bearer token** | Tokens are a CSV; rotate by adding the new token, deploying, removing the old. |
| **A leaked webhook signing secret** | HMAC-SHA256 over the body; rotate with the same overlap strategy. |
| **A charger sending forged events** | Cannot — the charger can only send messages on *its own* socket, identified by its `cp_id` (URL path). It can lie about its `id_tag`, but cannot pose as a different charger. |
| **Backend compromise** | Out of scope; the gateway's blast radius is "the platform stops authorizing users". The gateway itself does not authorize. |
| **OCPP protocol bugs** | Schema validation at the wire boundary; malformed messages are rejected before reaching the handler. |
| **Replay attacks at the OCPP layer** | Idempotency cache; replays are at-most-once-effective. |
| **Denial of service** | Rate limits per token, per charger. Kafka and webhook deliveries have bounded retry budgets. |
| **Data exfiltration via the OpenAPI surface** | `EVEYS_OCPP_REST_OPENAPI_ENABLED=false` for internet-exposed deployments. |

---

## Threats *out of scope*

The gateway does not address:

- **Physical access to a charger.** A jailbroken charger can do whatever the OEM's firmware allows. The OCPP protocol assumes the charger is a participant, not an attacker.
- **Compromised user devices.** RFID cloning, app-token theft — your platform's identity layer owns these.
- **Side-channel attacks on the data plane.** Disk encryption, network-level monitoring, supply-chain integrity of the container images — your platform owns these.
- **Insider threat at your operator level.** Anyone with cluster admin can do anything the gateway can do. Limit that role accordingly.

---

## The auth boundaries in detail

### Charger → Envoy

- **TLS**: the certificate the charger sees must chain back to a CA the charger trusts. Either a public CA (Let's Encrypt) if your edge hostname is public, or your own CA installed on every charger in the fleet.
- **Optional mTLS**: configure Envoy to require a charger client certificate. Useful for high-trust fleets where every charger is provisioned with its own cert; overkill for most.

### Envoy → Gateway

- Cleartext by default — the assumption is that this hop lives inside a namespace or VPC.
- **mTLS recommended in production.** Both sides reference the same `Secret`; rotation stays in lockstep. Without it, anything else in the same network that can reach the WebSocket port can pretend to be Envoy.

### Charger → Gateway (above the TLS layer)

- **Basic Auth.** `Authorization: Basic <base64(cp_id:password)>` on the WebSocket upgrade. Password is bcrypt-hashed in `charge_point_credentials`. Rotation: write a new hash and the charger picks it up on its next connection.
- **Strict mode** (`EVEYS_OCPP_WS_BASIC_AUTH_REQUIRED=true`) is the production setting — no credential row, no connection.

### Your platform → Gateway (REST / gRPC)

- **Bearer token.** CSV in `EVEYS_OCPP_REST_INBOUND_TOKENS`. Rotation: add new, deploy, update callers, remove old, deploy.
- **Rate limited** per token. Exceeding the limit returns 429.
- **TLS on the public side** is your responsibility — `kubectl port-forward` for dev; a properly-fronted service in production.

### Gateway → Your backend

- **Server TLS**: the gateway trusts your backend's TLS cert via the system CA trust store. Roll a private CA by adding it to the container's trust roots at image-build time.
- **Bearer token**: `EVEYS_OCPP_BACKEND_TOKEN`, sent in `Authorization: Bearer ...`.
- **mTLS to the backend** is not in the chart today; if you need it, the backend integration would need to grow that option.

### Gateway → Webhook endpoints

- **Server TLS**: gateway respects your endpoint's cert.
- **HMAC-SHA256 over the body**: `X-Eveys-Signature: sha256=<lowercase-hex>`. Your endpoint verifies; an attacker who learns your webhook URL cannot forge a delivery without the secret.

---

## Where you should think hardest

A small list of things to actually decide for your deployment:

1. **What CA signs your charger TLS certs?** A public CA is simplest if your edge is public. A private CA is unavoidable if chargers aren't on the public internet. Document the trust roots installed on every charger and how you rotate them.
2. **Do you want charger mTLS?** Only worth it for fleets where credential theft is plausible. Adds operational complexity proportional to fleet size.
3. **How are bearer tokens distributed and rotated?** A secret manager, not a config file. Define the rotation cadence; rehearse it.
4. **Is your backend's `Authorize` endpoint protected against the gateway being compromised?** I.e., if someone got the gateway's `BACKEND_TOKEN`, could they cause the backend to admit users who shouldn't be admitted? If so, scope the token (rate limits, IP allow-list) on the backend side.
5. **What does `BACKEND_AUTHORIZE_FALLBACK` say when your backend is down?** `reject` is the safe default. `accept_offline` is operationally convenient and a real risk if it stays on. Pick deliberately.
6. **Does the OpenAPI surface need to be public?** It exposes the API shape but not data. Most prod deployments turn it off; some leave it on behind a VPN.

---

## Where to go from here

- Concrete production hardening: [`../guides/deploy-to-production.md`](../guides/deploy-to-production.md).
- What's actually configurable: [`../reference/configuration.md`](../reference/configuration.md).
- The message flows these boundaries are protecting: [`how-ocpp-flows-work.md`](./how-ocpp-flows-work.md).
