# CLAUDE.md

> This file is required by some AI tooling for project discovery. The
> canonical AI-assistant guide for this repo is **[`AGENTS.md`](./AGENTS.md)**
> — read it first. It covers architecture, version-isolation rules,
> toolchain, commands, style, and testing conventions.

## Top reminders (don't skip even if you skim AGENTS.md)

- **Never cross-import** between `ocpp.v16` / `ocpp.v201` / `ocpp.v21`, or between our own `handlers/v16/` / `handlers/v201/`. Mixing protocol versions silently produces invalid messages on the wire.
- **JSON Schemas in `mobilityhouse/ocpp` are authoritative.** Don't edit dataclasses without checking the schema; don't edit schemas without an upstream-spec reason.
- **Run `make tests` before declaring a task done.** Same gate CI runs (ruff, mypy --strict, pytest with ≥ 80% coverage).
- **No opportunistic refactors.** Stay scoped — don't reformat, rename, or retype unrelated code.
- **No new runtime dependencies** without a justification in the PR description.
- **No cross-service imports.** `eveys/ocpp` talks to other Eveys services only via gRPC + Kafka.
- **Tests ship with code.** A PR with implementation but no tests is not done.

## Useful commands

| Command | Purpose |
|---|---|
| `make install` | Install deps via `uv` / Poetry |
| `make format`  | Apply isort + black |
| `make lint`    | Run `ruff check` |
| `make types`   | Run `mypy --strict src/` |
| `make tests`   | Full pre-commit gate (ruff, mypy, pytest with coverage) |

## Where to find the rest

- Project overview, layout, public API → [`AGENTS.md`](./AGENTS.md)
- Roadmap & phases → [`docs/01-roadmap.md`](./docs/01-roadmap.md)
- Task IDs (used in PR titles) → [`docs/02-tasks.md`](./docs/02-tasks.md)
- Full coding standards → [`docs/03-coding-standards.md`](./docs/03-coding-standards.md)
- Workflow & PR rules → [`docs/04-contributing.md`](./docs/04-contributing.md)
- ADRs (the *why*) → [`docs/adr/`](./docs/adr/)
- Local development setup (full stack on a laptop) → [`docs/07-local-dev-setup.md`](./docs/07-local-dev-setup.md)
- OCPP conformance matrix (every handler PR updates this) → [`docs/08-ocpp-conformance.md`](./docs/08-ocpp-conformance.md)
- OCPP 1.6 certification readiness playbook → [`docs/09-certification-readiness.md`](./docs/09-certification-readiness.md)
- Building the docs site (Sphinx + MyST) → [`docs/README.md`](./docs/README.md#building-this-site)
