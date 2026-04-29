# 04 — Contributing & Workflow

> How we ship code in `eveys/ocpp`. Read this before opening your first MR.

## Branching & MRs

| | |
|---|---|
| **Default branch** | `main` |
| **Branch naming** | `<type>/<task-id>-<short-slug>` — e.g. `feat/E1-5-boot-notification-handler`, `fix/E2-9-redis-ttl-leak`, `chore/E0-3-ci-pipeline` |
| **Branch lifetime** | Hours to a few days. **No long-lived feature branches.** |
| **Merge strategy** | Squash on merge. Linear history. |
| **Force-push** | Never to shared branches. Local rebase is fine. |
| **Direct push to `main`** | Forbidden. Even for tech leads. Even for hot-fixes. |

## Merge request rules

Every MR must:

- [ ] Reference at least one task ID from [`02-tasks.md`](./02-tasks.md) in the title (e.g. `feat(handlers): BootNotification handler (E1-5)`).
- [ ] Be **< 400 lines of diff** — or include a checklist in the description explaining why it had to be larger.
- [ ] Have a **green CI**: lint, types, unit + integration tests, coverage ≥ 80%.
- [ ] Include **tests for new behavior**. MRs without tests are rejected on first review.
- [ ] Update **relevant docs** when behavior or contracts change. Stale docs are a bug.
- [ ] Be reviewed by **at least one other engineer**. Tech-lead approval for: gRPC contract changes, ADRs, deploy/infra changes, security-sensitive code.
- [ ] Pass **all pre-commit hooks** locally before pushing.

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
- An MR sitting > 5 days without movement gets closed; reopen when ready to land.

## Releases

- **Versioning**: SemVer. `MAJOR.MINOR.PATCH`. Internal releases use `0.MAJOR.MINOR` until v1 GA.
- **Tagging**: `v0.5.3` from `main`. CI builds and publishes the container image.
- **Changelog**: `CHANGELOG.md` updated as part of the release MR. Follow [Keep a Changelog](https://keepachangelog.com/) format.
- **Deploy**: GitOps — merging the release tag to `deploy/prod` triggers ArgoCD to roll out.
- **Docs site**: tag with the `docs-v<n>` prefix to trigger the `docs:build` CI job, which produces a 30-day HTML artifact. See [docs/README.md — Building this site](./README.md#building-this-site).

## Hot-fixes

When prod is on fire:

1. Branch off `main`: `fix/hotfix-<slug>`.
2. Smallest possible change, focused tests.
3. MR with **`HOTFIX`** prefix in title.
4. Tech-lead review only (skip second reviewer).
5. Merge → tag → deploy.
6. **Within 24h**, file a follow-up MR with the proper tests, docs, and post-mortem.

## AI-assisted development

This project actively uses AI coding assistants (Claude Code, Cursor, Copilot, Aider). Rules apply equally to AI-generated and human-written code.

### Hard rules

- **AI writes — humans review.** No auto-merge. Author is responsible for every line.
- **AI must produce tests alongside code.** An MR with implementation but no tests is rejected.
- **AI does not commit.** Humans run `git commit`.
- **AI does not push.** Humans run `git push`.
- **AI does not deploy.** Deploys are GitOps and require a tagged release.
- **AI does not edit secrets, IAM, or production database migrations.**
- **AI does not bypass hooks.** No `--no-verify`.

### What AI is great at

- Boilerplate handlers (each OCPP action has a shape — AI emits the shape; humans verify spec compliance).
- Test scaffolding (Given/When/Then case generation; humans verify assertions).
- gRPC client/server code from `.proto`.
- SQL migrations from a schema sketch.
- Helm / Envoy / k8s YAML.
- Documentation that mirrors code.

### What humans must own

- Architecture decisions (write an ADR).
- Race conditions, especially in transaction state machines.
- OCPP-spec compliance — read the spec, don't trust the model.
- Charger-vendor interop quirks — comes from real-world experience.
- Production incident response.
- Trust-and-safety (auth, rate limits, validation).

### When AI is wrong

- AI confidently generates wrong code regularly. Don't trust output without reading it.
- AI generates plausible-looking JSON Schemas that don't match the actual OCPP spec. Always validate against `mobilityhouse/ocpp` schemas, not against the model's memory.
- AI may suggest cross-importing `ocpp.v16` and `ocpp.v201`. **Reject this every time** (see `03-coding-standards.md`).

### Pair-programming model

> **Human drives intent. AI drives keystrokes.**

The human decides *what* to build and *why*. The AI proposes *how*. The human verifies, edits, and signs.

If you can't explain why a piece of AI-generated code is correct, **don't merge it**.

## Issue tracking

- **Bugs**: GitLab Issues with `bug` label, severity (`p0`/`p1`/`p2`/`p3`).
- **Features**: GitLab Issues with `feature` label, link to relevant task ID from `02-tasks.md`.
- **Spikes / research**: `spike` label, time-boxed (max 3 days).
- **Tech debt**: `debt` label, evaluated quarterly.

## Definition of Done

A task is **done** when:

1. ✅ Code is merged to `main` via MR.
2. ✅ CI is green.
3. ✅ Coverage ≥ 80% maintained.
4. ✅ Relevant docs updated.
5. ✅ Task's "Output" line in `02-tasks.md` is satisfied.
6. ✅ Deployed to staging at minimum (production for cross-cutting features).
7. ✅ Tech lead has marked the task closed.

"Done" does not mean "code compiles." It means "I can hand this to the next person."

## Communication

- **Architecture decisions** → ADR in `docs/adr/`.
- **Multi-MR effort** → tracking issue with task IDs.
- **Cross-team contract change** → message in `#eveys-platform`, then MR.
- **Production incident** → page on-call, PIR within 48h.
- **Random ideas** → drop in `#eveys-ocpp-random` first, then issue if it sticks.

## Onboarding checklist (new engineer / new AI session)

- [ ] Read `docs/00-overview.md`, `docs/03-coding-standards.md`, this file, and `AGENTS.md`.
- [ ] Clone the repo, run `make install`, run `make tests` — all green locally.
- [ ] Read the `mobilityhouse/ocpp` library README and 1.6 examples.
- [ ] Pick a `good-first-issue` task and ship it.
