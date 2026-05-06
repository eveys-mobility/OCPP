# Contributing

Thanks for considering a contribution.

## Workflow

- Branch off `main`: `feature/<short-slug>`, `fix/<short-slug>`,
  `chore/<short-slug>`.
- PRs should be **under 400 lines of diff** when possible. Larger PRs
  need a checklist in the description explaining why.
- Every PR must:
  - have green CI (lint, types, tests, ≥ 80% coverage);
  - include tests for new behavior;
  - update relevant docs when behavior or contracts change;
  - be reviewed by at least one other engineer.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/):
  `<type>(<scope>): <subject>` where `<type>` is one of
  `feature`, `fix`, `chore`, `docs`, `refactor`, `test`, `perf`, `ci`,
  `build`, `revert`.
- Squash on merge. Linear history. Never force-push to a shared
  branch (local rebase is fine).

## Getting started

```bash
git clone git@github.com:eveys-mobility/OCPP.git
cd OCPP
make install      # creates .venv, installs deps, sets up pre-commit
make tests        # full pre-commit gate locally
```

The `README.md` covers what the service does, how to run it, the
configuration knobs, and the project layout.

## Reporting issues

Bugs and features go to GitHub Issues. Use the templates; include
reproduction steps for bugs.

## Security

Security issues do **not** go through the public issue tracker. See
[`SECURITY.md`](./SECURITY.md) for the private reporting channel.

## Code of Conduct

This project follows the [Code of Conduct](./CODE_OF_CONDUCT.md). By
participating you agree to abide by it.
