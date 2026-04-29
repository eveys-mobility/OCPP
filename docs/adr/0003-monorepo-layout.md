# ADR-0003 — Monorepo layout (`eveys/<service>`)

- **Status**: Accepted
- **Date**: 2026-04-29
- **Author**: Eveys engineering
- **Reviewers**: TBD

## Context

Eveys is a multi-service platform. `eveys/ocpp` is the first service; we expect 3+ more within 12 months, each with independent persistence, independent runtime, and independent deploy cadence.

We need to decide:

- One repo or many?
- If many, how do gRPC contracts and shared utilities propagate?
- How do CI, release, and deployment pipelines stay coherent?

Constraints:

- Small team (4 engineers initially). Can't afford CI sprawl or N independent release pipelines.
- gRPC contracts (`.proto`) are the boundary between services and must be versioned consistently.
- Future services should land in the same place without re-architecting the layout.

## Decision

**One Eveys monorepo at `/eveys/`. Each service is a top-level directory.**

```
eveys/
├── ocpp/                # this service
│   ├── pyproject.toml
│   ├── src/eveys_ocpp/
│   ├── proto/
│   ├── docs/
│   ├── tests/
│   ├── deploy/
│   └── ...
├── (future) station/
├── (future) shared-protos/   # if/when contracts need to be deduplicated
├── .gitlab-ci.yml            # one CI pipeline definition for all services
└── README.md
```

Each service:

- Owns its own dependencies, lockfile, container image, and release cadence.
- Has its own `docs/` directory.
- Publishes its `.proto` contracts in its own `proto/` subdir.
- Is independently deployable.

Cross-service `.proto` reuse, if it ever becomes painful, can later be promoted to a top-level `shared-protos/` package — but **not until** at least three services need the same definitions. Until then, copy is fine.

## Alternatives considered

- **Per-service repo (`eveys-ocpp`, `eveys-<other>`, …)** — full isolation, but quickly creates: contract drift, CI duplication, harder cross-service refactors, more places to update credentials. Rejected for a small team.
- **Deeper monorepo nesting (`eveys/services/ocpp/`, `eveys/libs/`, `eveys/infra/`)** — rejected as premature. We have one service today; let structure emerge from need.
- **Use a build tool like Bazel or Pants** — overkill at our scale. Each service has its own `pyproject.toml`/`Makefile` and that's enough.

## Consequences

### Positive

- **Atomic cross-service changes.** A `.proto` update + the consumer change can land in one MR.
- **Single CI configuration.** One GitLab CI pipeline covers all services.
- **Easier onboarding.** Engineers see the whole platform in one clone.
- **No artifact registry needed for local development.** Other services are imports/files, not packages to install.

### Negative / costs

- **Repo grows over time.** Mitigated by keeping each service's directory self-contained.
- **CI must be smart about what to rebuild.** Path-based filters per service so a non-`ocpp` change doesn't rerun all `ocpp` tests.
- **Permissions are repo-wide.** If we ever need to restrict access to one service, we'd have to split. Mitigation: use CODEOWNERS for review gates.

### Risks

- **Tight coupling creep.** Engineers are tempted to import across services. Mitigation: lint rule that forbids cross-service imports; gRPC is the only inter-service contract.
- **Releases get coupled accidentally.** Mitigation: each service has its own version, tag, and deploy pipeline.

### Reversibility

- **Reversible at moderate cost.** Splitting a monorepo into per-service repos later is mechanical (git filter-repo). Going from per-service back to mono is harder. So defaulting to mono is the safer starting choice.

## Implementation rules

1. **No cross-service Python imports.** `eveys_<other>` cannot `import eveys_ocpp`. Communicate via gRPC.
2. **Each service has its own `pyproject.toml`.** Dependencies are not shared.
3. **CI** uses path filters: `paths: ['ocpp/**']` runs the `ocpp` workflow only.
4. **CODEOWNERS** maps directories to teams/individuals so MR review is auto-routed.
5. **Top-level `README.md`** explains the layout and links to each service's README.
6. **Container images** are tagged `eveys/<service>:<version>`.

## References

- Google's monorepo paper: <https://research.google/pubs/why-google-stores-billions-of-lines-of-code-in-a-single-repository/>
- Trunk-based development: <https://trunkbaseddevelopment.com/>
