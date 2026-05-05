# ADR-0018 — gRPC + Kafka-event backward-compat enforced in CI

- **Status**: Accepted
- **Date**: 2026-05-01
- **Author**: Eveys engineering (E2-12; AI-assisted draft, human-reviewed and merged)
- **Reviewers**: Project tech lead (post-merge sign-off)

## Context

`eveys/ocpp` publishes two frozen v1 wire contracts:

- `proto/ocpp_gw/v1/gateway.proto` — the gRPC surface the platform calls *on us* (E2-2).
- `proto/events/v1/events.proto` — the Kafka event envelopes we emit (E2-3).

Both are consumed by other Eveys services (mobile BFF, future billing/session services, ClickHouse via the Kafka table engine) **and** generated stubs in unknown downstream repos. The contracts are explicitly stability-promised: adding fields is allowed, removing/renumbering is forbidden until v2 (see `proto/README.md`).

That promise is currently a code-review check. It works as long as every reviewer remembers it on every proto-touching MR. **It scales to wrong** — once enough consumers depend on `cp.boot`/`cp.status`/`tx.started` (post-E2-8) and `OcppGateway.RemoteStart` (post-E2-6), an accidental field rename in a proto-touching MR ships a wire-incompatible release. Recovering means an emergency revert plus consumer firefighting.

The `proto/README.md` already anticipated this: line 38 says *"Buf-style linting will be added with task E2-12."*

Forces:

- We need a CI gate that fails when a backward-incompatible proto change lands.
- Tooling has to be reproducible (lockable version, deterministic output) and operate on Git history (compare HEAD against `main`).
- Adding the gate must not slow `lint`/`types`/`tests` jobs noticeably.
- The check must catch JSON wire breakage, not just binary protobuf breakage. The Kafka event-envelope is consumed in JSON form by some downstream pipelines (e.g. ClickHouse string columns); a JSON-name change is as bad as a tag-number change.
- An intentional breaking change (rare; only at v1→v2 transitions) needs an explicit, documented bypass, not a silent override.

## Decision

**Run `buf breaking` in CI on every MR, against `main` as baseline, with the `WIRE_JSON` rule set, hard-failing on any breaking change.** Both proto trees (`proto/ocpp_gw/` and `proto/events/`) are in scope.

Concretely:

- A repo-root `buf.yaml` declares the breaking-change rule set: `WIRE_JSON` (strictest of the practical sets — protects both binary protobuf and protobuf-JSON consumers).
- A new `proto-breaking` job in the `quality` stage of `.gitlab-ci.yml` (alongside `lint` and `types`) installs the pinned `buf` binary, fetches `main`, and runs `buf breaking <module> --against '.git#branch=main,subdir=<module>'` once per module (the v2 multi-module workspace needs per-module invocation; comparing the whole workspace against itself conflates the two modules' image counts).
- The job is hard-fail. No "warn-only" period. The proto contracts are already frozen at v1 (per `proto/README.md`); the gate only enforces what the docs already promise.
- An intentional break (e.g. v1→v2 rollout) bypasses the gate by adding a `bypass-breaking-change` MR label. The job script reads `$CI_MERGE_REQUEST_LABELS` and exits 0 with a loud `WARNING` log if the label is present. Bypass is rare by design — every use will appear in the MR history.
- The check runs only on MR pipelines (where there's a `main` to compare against). On `main` push pipelines it's a no-op (you can't "break against yourself").

Not implementing right now (deferred until they earn their keep):

- `buf lint` (style/naming rules) — useful but separate concern; this ADR is about wire-compat only.
- `buf format` (auto-format protos) — drive-by; wait for the first time a proto MR has formatting drift.
- `buf push` (publish to Buf Schema Registry) — premium dependency; revisit if/when ADR-0014 (schema registry) lands.

## Alternatives considered

- **`protolint` instead of `buf`** — does style/lint, doesn't do breaking-change detection. Wrong tool for the goal. Rejected.

- **Custom Python script that `protoc`-compiles old + new and compares descriptor sets** — one less third-party binary, full control. Rejected: re-implements 90% of `buf breaking` poorly. Field-tag tracking, oneof handling, JSON-name preservation, and reserved-range enforcement are non-trivial; getting the rule set exactly right is months of work. `buf` is the de facto standard, MIT-licensed, single static Go binary.

- **`buf` via the `bufbuild/buf-action` GitLab include** — convenient on GitHub Actions; less reliable on GitLab where the equivalent template isn't first-party. Direct `buf` install is one shell line; use it.

- **Manual review only (status quo)** — the thing this ADR replaces. Doesn't scale beyond a 4-person team and one consumer.

- **Warn-only initially, hard-fail later** — soft launches where the gate is non-blocking get ignored. The proto contracts are *already* frozen; a wider-soft-launch policy would let breaking changes slip through during the soft window. Reject; hard-fail from day one. (We can add a label-based bypass for the rare intentional break — see below.)

- **`WIRE` rule set instead of `WIRE_JSON`** — less strict; allows JSON-name changes (e.g. `cp_id` → `cpId`). Rejected because consumers reading the Kafka envelope as JSON (ClickHouse `JSONExtract`, ad-hoc analytics, mobile BFF if it ever decodes JSON instead of binary) would silently break. `WIRE_JSON` catches both. Marginal cost: a few extra rules that don't fire today.

- **`FILE` rule set** — most permissive. Rejected; defeats the purpose.

## Consequences

### Positive

- Accidental field rename / removal / renumbering / oneof-shape change fails CI before it can land. The class of bug is gone.
- The gate is **machine-checked**; reviewer attention frees up for design and behavior questions.
- One pinned binary, one config file, one CI job. Operational footprint stays small.
- Catches JSON-name regressions (a real risk for ClickHouse / BFF consumers) that the binary-only `WIRE` set would miss.
- The `bypass-breaking-change` label provides a documented escape hatch — every use leaves a trail in MR history.

### Negative / costs

- Adds `buf` (a third-party binary) to CI. Pin the version in the install command; bump deliberately.
- One more CI job (~5–10 s end-to-end after install). Runs in parallel with `lint`/`types`/`tests`, so wall-clock impact is zero.
- Engineers running CI locally need `buf` installed if they want to reproduce a CI failure. Document in `docs/03-coding-standards.md` (follow-up MR).
- Generated code (`src/eveys_ocpp/_generated/`) is excluded from the scan via `buf.yaml`'s `excludes`. Without the exclude, regenerating stubs would trigger spurious diffs.

### Risks

- **`buf` binary version drift.** A point release changing default rule semantics could turn a green pipeline red. Mitigation: pin to a specific `vX.Y.Z` in the install step; bump in a deliberate MR with a regression sweep.
- **`main` ref availability in CI.** GitLab's MR pipelines fetch only the source branch by default; the breaking check needs `main` as well. Mitigation: the job's `before_script` fetches `main` explicitly. If that ever fails, the breaking check skips with a clear error rather than silently passing.
- **Bypass label misuse.** An engineer applies `bypass-breaking-change` to skip a check on a non-breaking-change MR. Reviewer responsibility: check the MR description's justification when the label is present. Tracked in the contributing checklist (follow-up).
- **Future schema registry overlap.** ADR-0014 (planned, schema registry) might subsume part of this gate. Until then, `buf breaking` is the local source of truth. When the registry lands, this ADR is the precedent for what stays in CI vs what moves to the registry.
- **False sense of completeness.** `buf breaking` checks the *protobuf* contract. It does NOT check (a) gRPC method semantics, (b) handler behavior changes, (c) Kafka topic-name changes (those live in our settings, not protos). Reviewers still own the semantic-compat call.

### Reversibility

Reversible. Removing the gate is a 5-line revert: drop the CI job, drop `buf.yaml`. No code or build artifact depends on it. Switching tools (e.g. to a future first-party GitLab proto-compat check, or to a schema-registry-based check) is the same shape: drop this ADR's job, add the new one, retire `buf.yaml` if applicable.

## Project conventions implied by this decision

- The repo-root `buf.yaml` is the canonical breaking-change config. Edit only via an ADR amendment (this ADR's lifecycle), not as a drive-by.
- Adding a new proto file or a new proto tree (e.g. a future `proto/admin/v1/`) requires extending `buf.yaml`'s `modules` block in the same MR. The CI job will fail on stranded protos.
- An intentional `bypass-breaking-change` label requires:
  1. The MR description explains *why* the break is necessary.
  2. The MR also bumps the package version (e.g. `eveys.events.v1` → `eveys.events.v2`) in the same commit.
  3. Tech-lead approval is mandatory (per `04-contributing.md`'s ADR-equivalence clause for contract changes).

## References

- `proto/README.md` (in repo root, outside the docs tree) — declares the v1 freeze and anticipates this gate (line 38).
- [`buf` documentation — breaking-change detection](https://buf.build/docs/breaking-overview).
- [E2-2, E2-3, E2-12](../02-tasks.md) — the proto contracts and this gate.
- [ADR-0015](./0015-kafka-event-envelope-format.md) — schema-evolution rules this gate enforces.
- [ADR-0014 (planned)](../05-architecture-decisions.md) — schema registry; will revisit overlap when it lands.
