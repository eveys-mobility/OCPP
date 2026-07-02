# eveys/ocpp — user documentation

**Audience.** Developers and operators new to this gateway. Comfortable with Python 3.13, Docker, Kubernetes, REST, and Kafka. Not assumed to know OCPP.

**What this answers.** Where to find what, which order to read in for your role, and what you can rely on across versions.

---

## What this set is

This is the *external* documentation for the eveys/ocpp gateway — the OCPP CSMS gateway that owns charger WebSocket connections and exposes a stable surface to the rest of the platform.

It is **self-contained**. Every concept, command, and contract a reader needs is inside this directory. There are no references to internal design notes, ADRs, or implementation walkthroughs.

If a page mentions a file path under `src/eveys_ocpp/...`, it's because you may want to open that file directly. That is the only kind of cross-link you'll see.

---

## Pick your reading path

### "I just want to try it." (~30 minutes)

1. [`01-what-is-this.md`](./01-what-is-this.md) — what the project is and what OCPP is in 60 seconds.
2. [`02-quickstart.md`](./02-quickstart.md) — Docker Compose, simulator charger, send a command, watch an event. Ten minutes if your workstation already has Docker.
3. [`03-architecture.md`](./03-architecture.md) — one diagram, one page. Now you know what you ran.

### "I'm building a backend integration." (~2 hours, repeat as needed)

1. [`03-architecture.md`](./03-architecture.md) — see how the gateway expects to talk to your backend.
2. [`guides/use-the-rest-api.md`](./guides/use-the-rest-api.md) — auth, errors, pagination, the commands you'll call most.
3. [`guides/consume-events.md`](./guides/consume-events.md) — Kafka vs webhooks, idempotency on your side.
4. [`reference/rest-api.md`](./reference/rest-api.md), [`reference/events.md`](./reference/events.md) — keep open while you code.
5. [`concepts/how-ocpp-flows-work.md`](./concepts/how-ocpp-flows-work.md) — when you hit "why is this event arriving twice?" or "why didn't a `StartTransaction` follow my `RemoteStart`?".

### "I'm operating this in production." (read in order)

1. [`03-architecture.md`](./03-architecture.md) — what owns what.
2. [`guides/install.md`](./guides/install.md) — Helm chart and what it expects from your cluster.
3. [`guides/deploy-to-production.md`](./guides/deploy-to-production.md) — TLS, mTLS, autoscaling, secrets, drain. Ends in a pre-flight sign-off checklist.
4. [`concepts/security-model.md`](./concepts/security-model.md) — what you're trusting; non-negotiable to read before going live.
5. [`guides/operate.md`](./guides/operate.md) — health probes, logs, metrics, drain, rollback.
6. [`reference/configuration.md`](./reference/configuration.md), [`reference/metrics.md`](./reference/metrics.md) — keep open.
7. [`guides/upgrade.md`](./guides/upgrade.md) — when the next version drops.

---

## Full table of contents

### Top level

- [`01-what-is-this.md`](./01-what-is-this.md) — product framing and a 60-second OCPP primer.
- [`02-quickstart.md`](./02-quickstart.md) — 10-minute hands-on: simulator → boot → `RemoteStart` → event.
- [`03-architecture.md`](./03-architecture.md) — one diagram, one page, all four surfaces.
- [`04-glossary.md`](./04-glossary.md) — OCPP terms, one line each, alphabetised. Keep nearby.

### Guides — read in order if you're new

- [`guides/install.md`](./guides/install.md) — bring the stack up locally (Compose) or in a cluster (Helm).
- [`guides/connect-a-charger.md`](./guides/connect-a-charger.md) — first real charger; URL, auth, common failures.
- [`guides/use-the-rest-api.md`](./guides/use-the-rest-api.md) — drive the gateway from a backend.
- [`guides/consume-events.md`](./guides/consume-events.md) — Kafka and webhooks, one envelope.
- [`guides/deploy-to-production.md`](./guides/deploy-to-production.md) — TLS, mTLS, autoscaling, sign-off checklist.
- [`guides/operate.md`](./guides/operate.md) — probes, logs, metrics, drain, rollback.
- [`guides/upgrade.md`](./guides/upgrade.md) — version policy and zero-downtime motions.

### Reference — keep open while you code or operate

- [`reference/rest-api.md`](./reference/rest-api.md) — every HTTP endpoint.
- [`reference/grpc-api.md`](./reference/grpc-api.md) — every gRPC RPC.
- [`reference/events.md`](./reference/events.md) — every Kafka topic and webhook event.
- [`reference/configuration.md`](./reference/configuration.md) — every environment variable.
- [`reference/metrics.md`](./reference/metrics.md) — every Prometheus series.

### Concepts — read once, refer back

- [`concepts/how-ocpp-flows-work.md`](./concepts/how-ocpp-flows-work.md) — the charging session end-to-end.
- [`concepts/security-model.md`](./concepts/security-model.md) — what is trusted, what is not.
- [`concepts/multi-pod-and-routing.md`](./concepts/multi-pod-and-routing.md) — Envoy ring-hash, registry, cross-pod dispatch.
- [`concepts/idempotency-and-replay.md`](./concepts/idempotency-and-replay.md) — duplicate-message handling.

---

## Conventions used in these pages

- Every page opens with bold **Audience** and **What this answers** lines, in that order. If you read those and the page isn't for you, close it.
- The first time any OCPP-specific term appears on a page (`BootNotification`, `MeterValues`, `id_tag`, …), it links to [`04-glossary.md`](./04-glossary.md).
- Every code block declares its language. Every shell example is copy-pasteable as written.
- Every reference page opens with a "use this if you…" line so you can decide from the TOC.
- No internal design or implementation prose is linked. Source-code paths (e.g. `src/eveys_ocpp/...`) appear only when you may want to open the file directly.

---

## What to do if something is wrong

If a step in these pages doesn't work, treat it as a doc bug. Open an issue against the repository describing what you ran, what happened, and what you expected. The docs should be the single source of truth; if reality has drifted, the docs are wrong and we fix them.
