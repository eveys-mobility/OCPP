# Contributing

Thanks for considering a contribution. The full workflow guide lives at
[`docs/04-contributing.md`](./docs/04-contributing.md) — it covers
branching, PRs, code review, releases, hot-fixes, and the definition of
done.

This file is the short version GitHub surfaces in its UI.

## Quick rules

- Fork or branch off `main` (`feature/<task-id>-<slug>`,
  `fix/<task-id>-<slug>`, `chore/<task-id>-<slug>`).
- PRs should be **under 400 lines of diff** when possible. Larger PRs
  need a checklist in the description explaining why.
- Every PR must:
  - reference a task ID from [`docs/02-tasks.md`](./docs/02-tasks.md)
    in the title (e.g. `feature(api): gateway REST commands (E3-8)`);
  - have green CI (lint, types, tests, ≥ 80% coverage);
  - include tests for new behavior;
  - update relevant docs when behavior or contracts change.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/):
  `<type>(<scope>): <subject>` where `<type>` is one of
  `feature`, `fix`, `chore`, `docs`, `refactor`, `test`, `perf`, `ci`,
  `build`, `revert`.

## Getting started

```bash
git clone git@github.com:eveys-mobility/OCPP.git
cd OCPP
make install      # creates .venv, installs deps, sets up pre-commit
make tests        # full pre-commit gate locally
```

See [`docs/07-local-dev-setup.md`](./docs/07-local-dev-setup.md) for
the full local stack (Postgres, Redis, Kafka, ClickHouse via
docker-compose) and [`docs/12-connecting-real-charger.md`](./docs/12-connecting-real-charger.md)
for connecting a real OCPP charger to the gateway.

## Reporting issues

Bugs and features go to GitHub Issues. Use the templates if available;
include reproduction steps for bugs.

## Code of Conduct

This project follows the [Code of Conduct](./CODE_OF_CONDUCT.md). By
participating you agree to abide by it.
