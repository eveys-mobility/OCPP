# Userdocs implementation plan

**Status:** approved, in progress.
**Started:** 2026-05-11.

This file is the source of truth for the `userdocs/` build-out. Each task is checked off as it lands. If a task is split, sub-bullets appear under it. If a task is dropped, it stays in the list with `~~strikethrough~~` and a one-line reason.

---

## Audience promise

A developer comfortable with Python 3.13, Docker, Kubernetes basics, REST, and Kafka — **new to OCPP and to this gateway**. Every page assumes that baseline; nothing more.

## Self-containment rule

This set is complete on its own. **No links to or references of any internal / implementation / ADR documents.** Source-code path references (e.g. `src/eveys_ocpp/...`) are allowed when an example needs a path, but no narrative cross-links to existing prose docs.

## Conventions every page follows

- One H1 (the page title), then a one-line **audience** note, then a one-line **what this answers** note. Both bold, on their own lines. Then content.
- First mention of any OCPP term links to `userdocs/04-glossary.md`.
- Every code block specifies its language. Every shell example is copy-pasteable.
- Every reference page opens with a "use this if you…" line.
- No external / implementation prose links.

---

## Final tree (18 pages)

```
userdocs/
  _plan.md                        — this file
  README.md                       — entry, TOC, audience promise, self-containment rule
  01-what-is-this.md              — product framing + 60-second OCPP primer
  02-quickstart.md                — Docker Compose: simulator → boot → RemoteStart → event (10 min max)
  03-architecture.md              — 1 diagram + 1-page narrative covering all 4 surfaces
  04-glossary.md                  — OCPP terms with one-line definitions, alphabetised
  guides/
    install.md                    — bring up the stack locally (Compose) and in Kubernetes (Helm)
    connect-a-charger.md          — first real charger; networking, basic auth, common failures
    use-the-rest-api.md           — driving the gateway from a backend; auth, pagination, error model
    consume-events.md             — Kafka + webhooks; one envelope, two transports, when to use each
    deploy-to-production.md       — TLS, mTLS, autoscaling, secrets, drain semantics; closes with sign-off checklist
    operate.md                    — health/ready, structured logs, metrics, drain, rollback motions
    upgrade.md                    — version policy, supported upgrade paths, stability across releases
  reference/
    rest-api.md                   — every endpoint, request/response, errors
    grpc-api.md                   — every RPC, payload shapes, code examples
    events.md                     — every event, envelope, delivery semantics, idempotency
    configuration.md              — every env var, default, stability tier
    metrics.md                    — every Prometheus series, label set, what it means
  concepts/
    how-ocpp-flows-work.md        — Authorize → Start → Meter → Stop end-to-end
    security-model.md             — what the gateway trusts, what it doesn't
    multi-pod-and-routing.md      — ring-hash, registry, cross-pod dispatch, drain
    idempotency-and-replay.md     — duplicate-message handling, two-layer dedup
```

---

## Tasks (execution order)

- [x] **0. Scaffold the plan file on disk.** (this file)
- [x] **1. Scaffold the 18 doc files.** Each file gets the H1 + audience + what-this-answers stub. Nothing else.
- [x] **2. Entry README.** TOC, audience promise, self-containment rule, three suggested read paths (trying it / shipping a backend integration / operating in production).
- [x] **3. Top-level four pages.** Aim: readable in 20 min total.
  - [x] 3a. `01-what-is-this.md`
  - [x] 3b. `02-quickstart.md`
  - [x] 3c. `03-architecture.md`
  - [x] 3d. `04-glossary.md`
- [x] **4. Guides** (order matches real user journey):
  - [x] 4a. `guides/install.md`
  - [x] 4b. `guides/connect-a-charger.md`
  - [x] 4c. `guides/use-the-rest-api.md`
  - [x] 4d. `guides/consume-events.md`
  - [x] 4e. `guides/deploy-to-production.md`
  - [x] 4f. `guides/operate.md`
  - [x] 4g. `guides/upgrade.md`
- [x] **5. Reference pages.**
  - [x] 5a. `reference/rest-api.md`
  - [x] 5b. `reference/grpc-api.md`
  - [x] 5c. `reference/events.md`
  - [x] 5d. `reference/configuration.md`
  - [x] 5e. `reference/metrics.md`
- [x] **6. Concepts.**
  - [x] 6a. `concepts/how-ocpp-flows-work.md` (load-bearing; first)
  - [x] 6b. `concepts/security-model.md`
  - [x] 6c. `concepts/multi-pod-and-routing.md`
  - [x] 6d. `concepts/idempotency-and-replay.md`
- [x] **7. Cross-link sweep.** First-mention glossary links; "see also" footer on each guide.
- [x] **8. Self-containment check.** `grep -rE "docs/|adr/|ADR-" userdocs/` returns nothing.
- [x] **9. Final read-through.** Walk every page as the target reader; one tightening pass.

---

## Final pass notes (2026-05-11)

Fixes applied during the read-through:

- Footer heading standardised: "Where to go next" → "Where to go from here" in `02-quickstart.md` and `03-architecture.md`.
- Metric name typo: `eveys_ocpp_ws_active_connections` → `eveys_ocpp_ws_connections_active` across `operate.md` and `deploy-to-production.md`.
- Non-existent metric `eveys_ocpp_kafka_publish_failures_total` → `eveys_ocpp_kafka_publish_total{outcome="failed"}` in `operate.md`.
- Env var corrections:
  - `EVEYS_OCPP_WEBHOOK_SIGNING_SECRET` → `EVEYS_OCPP_WEBHOOK_SECRET` (4 pages).
  - `EVEYS_OCPP_OTLP_ENDPOINT` → `EVEYS_OCPP_TRACING_OTLP_ENDPOINT` (2 pages).
  - `EVEYS_OCPP_DB_POOL_MAX_OVERFLOW` → `EVEYS_OCPP_DB_MAX_OVERFLOW`.
  - Removed fictional `EVEYS_OCPP_BACKEND_CA_PATH` reference; replaced with the actual approach (trust roots at image-build time).
- Reworded one sentence in `01-what-is-this.md` that implied an "elsewhere in this set" doc that doesn't exist.
- Verified: every internal `.md` link resolves; every reference page opens with "Use this if you"; every page opens with bold **Audience** + **What this answers**.
- Verified: zero hits for `docs/`, `adr/`, `ADR-` anywhere in `userdocs/` outside `_plan.md`.

The set is ready.

---

## Iteration history (for the record)

**v1** — Three subdirs + 16 files; OCPP terms assumed known; dev-only quickstart; metrics misfiled under "reference"; no prod / security / upgrade pages.

**Iteration 1** — Caught: subdirs heavy for ~16 pages; no prod page; no security; metrics misclassified; no upgrade page; quickstart scope undisciplined.

**Iteration 2** — Caught: glossary must stay top-level (referenced constantly); `send-your-first-command.md` was filler; missing OCPP-101 primer; reader-intent grouping clearer than format-based; prod needs checklist shape.

**Iteration 3** — Caught: don't fragment prod checklist (fold into deploy page); concept order matters (how-ocpp-flows first); security its own page after deploy; webhooks + Kafka collapse to `events.md`; reference pages need "use this if you…" opener; stability promise needs explicit upgrade page.

**Final:** 18 pages, two-tier nav, self-containment rule pinned, audience promise pinned, conventions codified above.
