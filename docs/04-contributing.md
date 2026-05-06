# 04 — Contributing & Workflow

> How we ship code in `eveys/ocpp`. Read this before opening your first PR.

## Branching & PRs

| | |
|---|---|
| **Default branch** | `main` |
| **Branch naming** | `<type>/<task-id>-<short-slug>` — e.g. `feat/E1-5-boot-notification-handler`, `fix/E2-9-redis-ttl-leak`, `chore/E0-3-ci-pipeline` |
| **Branch lifetime** | Hours to a few days. **No long-lived feature branches.** |
| **Merge strategy** | Squash on merge. Linear history. |
| **Force-push** | Never to shared branches. Local rebase is fine. |
| **Direct push to `main`** | Forbidden. Even for tech leads. Even for hot-fixes. |

## Pull request rules

Every PR must:

- [ ] Reference at least one task ID from [`02-tasks.md`](./02-tasks.md) in the title (e.g. `feat(handlers): BootNotification handler (E1-5)`).
- [ ] Be **< 400 lines of diff** — or include a checklist in the description explaining why it had to be larger.
- [ ] Have a **green CI**: lint, types, unit + integration tests, coverage ≥ 80%.
- [ ] Include **tests for new behavior**. PRs without tests are rejected on first review.
- [ ] Update **relevant docs** when behavior or contracts change. Stale docs are a bug.
- [ ] Be reviewed by **at least one other engineer**. Tech-lead approval for: gRPC contract changes, ADRs, deploy/infra changes, security-sensitive code.
- [ ] Pass **all pre-commit hooks** locally before pushing.

### Pre-commit hooks

`make install` activates the hooks (configured in `.pre-commit-config.yaml`). They run automatically on every `git commit`:

- **ruff** — lint + format. Same rules as CI's `lint` job; fixes most issues automatically.
- **mypy** — strict type-check on `src/` only.
- **stock hygiene** — trailing whitespace, end-of-file fixer, large-file guard, merge-conflict markers, mixed line endings.
- **conventional-pre-commit** — enforces the commit-message format below.

To run the hooks against the whole tree without committing:

```bash
make precommit
```

If a hook auto-fixes something, re-stage and re-commit. **Never bypass hooks with `--no-verify`** (AGENTS.md hard rule 7).

## Commit conventions

[Conventional Commits](https://www.conventionalcommits.org). Format:

```
<type>(<scope>): <subject> (E<phase>-<seq>)
```

Examples:
- `feat(handlers): implement BootNotification (E1-5)`
- `fix(registry): refresh TTL on every heartbeat (E2-9)`
- `chore(ci): cache uv dependencies (E0-3)`
- `docs(roadmap): adjust Phase 3 scope`
- `refactor(commands): split v16 from v201`

Types: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `perf`, `ci`, `build`.

A commit body is optional but encouraged for non-trivial changes — explain *why*, not *what*.

## Code review

### Reviewer's job

- **Block on**: correctness, OCPP-spec violations, performance regressions in hot paths, security issues, broken tests, missing tests, missing docs.
- **Don't block on**: style nits a formatter could catch, personal preferences. Comment as `nit:` so the author can choose.
- **Prefer questions over commands.** "Why this approach over X?" beats "do X."
- **Assume good faith.** AI-assisted code is still owned by the human author.

### Author's job

- **Self-review your diff** before requesting review. Catch the obvious stuff first.
- **Reply to every comment.** Either change the code, push back with reasoning, or "good catch, will follow up in next MR."
- **Don't merge while review is in flight.** Wait for explicit approval.

### How fast?

- Reviewers respond within **1 business day**.
- Authors push fixes within **1 business day** of receiving review.
- A PR sitting > 5 days without movement gets closed; reopen when ready to land.

## Releases

- **Versioning**: SemVer. `MAJOR.MINOR.PATCH`. Internal releases use `0.MAJOR.MINOR` until v1 GA.
- **Tagging**: `v0.5.3` from `main`. CI builds and publishes the container image.
- **Changelog**: `CHANGELOG.md` updated as part of the release MR. Follow [Keep a Changelog](https://keepachangelog.com/) format.
- **Deploy**: GitOps — merging the release tag to `deploy/prod` triggers ArgoCD to roll out.
- **Docs site**: tag with the `docs-v<n>` prefix to trigger the `docs` CI workflow, which produces a 30-day HTML artifact. See [docs/README.md — Building this site](./README.md#building-this-site).

## Hot-fixes

When prod is on fire:

1. Branch off `main`: `fix/hotfix-<slug>`.
2. Smallest possible change, focused tests.
3. PR with **`HOTFIX`** prefix in title.
4. Tech-lead review only (skip second reviewer).
5. Merge → tag → deploy.
6. **Within 24h**, file a follow-up PR with the proper tests, docs, and post-mortem.

## Things that are easy to get wrong

A few footguns in this codebase that have bitten contributors. Read once.

- **OCPP-spec compliance** is non-negotiable. Read the actual spec; don't trust derived summaries. JSON Schemas in `mobilityhouse/ocpp` are authoritative — validate against them, not against memory.
- **Never cross-import** between `ocpp.v16` and `ocpp.v201` (or between our own `handlers/v16/` and `handlers/v201/`). Mixing protocol versions silently produces invalid messages on the wire. See `03-coding-standards.md`.
- **Race conditions** in transaction state machines are a recurring class of bug. Walk through the state diagram before changing handler order.
- **Charger-vendor interop quirks** come from real-world experience. Vendors interpret the spec differently; what works against one firmware may not against another.
- **Trust-and-safety code** (auth, rate limits, input validation) gets explicit review attention. Don't merge changes here without sign-off from someone who's familiar with the threat model.
- **Production incident response** is a tech-lead call, not a freestyle. Follow the hot-fix flow above.

## Issue tracking

- **Bugs**: GitHub Issues with `bug` label, severity (`p0`/`p1`/`p2`/`p3`).
- **Features**: GitHub Issues with `feature` label, link to relevant task ID from `02-tasks.md`.
- **Spikes / research**: `spike` label, time-boxed (max 3 days).
- **Tech debt**: `debt` label, evaluated quarterly.

## Definition of Done

A task is **done** when:

1. ✅ Code is merged to `main` via PR.
2. ✅ CI is green.
3. ✅ Coverage ≥ 80% maintained.
4. ✅ Relevant docs updated.
5. ✅ Task's "Output" line in `02-tasks.md` is satisfied.
6. ✅ Deployed to staging at minimum (production for cross-cutting features).
7. ✅ Tech lead has marked the task closed.

"Done" does not mean "code compiles." It means "I can hand this to the next person."

## Communication

- **Architecture decisions** → ADR in `docs/adr/`.
- **Multi-PR effort** → tracking issue with task IDs.
- **Cross-team contract change** → message in `#eveys-platform`, then MR.
- **Production incident** → page on-call, PIR within 48h.
- **Random ideas** → drop in `#eveys-ocpp-random` first, then issue if it sticks.

## Onboarding checklist (new engineer / new AI session)

- [ ] Read `docs/00-overview.md`, `docs/03-coding-standards.md`, this file, and `AGENTS.md`.
- [ ] Clone the repo, run `make install`, run `make tests` — all green locally.
- [ ] Read the `mobilityhouse/ocpp` library README and 1.6 examples.
- [ ] Pick a `good-first-issue` task and ship it.
