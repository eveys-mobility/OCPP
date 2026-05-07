# eveys/ocpp — Documentation

Documentation for **eveys/ocpp**, the OCPP gateway service of the Eveys EV-charging platform.

> **Scope of this document set**: full — roadmap, tasks, coding standards, ADRs, implementation plan, conformance matrix, certification readiness, testing strategy, and the generated configuration reference. Code is shipped under `src/` and tested per the four-tier ladder in [10-testing-strategy.md](./10-testing-strategy.md).

## Contents

| # | Document | Purpose | Audience |
|---|---|---|---|
| 00 | [Overview](./00-overview.md) | What `eveys/ocpp` is, what it owns, what it does **not** own | Everyone |
| 01 | [Roadmap](./01-roadmap.md) | Phased milestones from foundations to production | Leadership, PMs, engineers |
| 02 | [Tasks](./02-tasks.md) | Work breakdown — concrete tickets per phase | Engineers, tech lead |
| 03 | [Coding standards](./03-coding-standards.md) | Python style, project conventions, naming, testing | Engineers, AI assistants |
| 04 | [Contributing & workflow](./04-contributing.md) | Branches, MRs, reviews, releases, AI-assisted development rules | Engineers |
| 05 | [Architecture decisions](./05-architecture-decisions.md) | Index of ADRs (Architecture Decision Records) | Engineers, future-us |
| 06 | [Implementation plan](./06-implementation-plan.md) | Week-by-week schedule (W0–W13) mapping tasks to calendar | Tech lead, engineers, manager |
| 07 | [Local development setup](./07-local-dev-setup.md) | Bring the full stack up on a laptop (docker-compose + k3d/kind) | Engineers (day 1) |
| 08 | [OCPP conformance matrix](./08-ocpp-conformance.md) | Per-test-case (Appendix C TC ID) → handler → status; the cert-grade record | Engineers, QA, OCTT examiners, auditors |
| 09 | [Certification readiness](./09-certification-readiness.md) | Cert-program playbook: streams, PICS prep, lab engagement, exit gate | TL, manager, QA |
| 10 | [Testing strategy](./10-testing-strategy.md) | The four-tier test trust ladder — what each CI job guarantees and what bug class it catches (see ADR-0024) | Engineers, anyone debugging a CI failure |
| 11 | [Configuration reference](./11-configuration-reference.md) | Every env var: category, default, range, stability, secret-flag, what it does, what changes if you change it (see ADR-0025) | Operators, SREs, anyone tuning the service |
| 12 | [Connecting a real charger](./12-connecting-real-charger.md) | Operator/integrator guide: install, connect a real OCPP 1.6 device, watch activity in logs/Postgres/ClickHouse/Kafka, use the REST API from `curl` or Postman | Operators, integrators, anyone connecting a charger for the first time |
| 13 | [Load testing](./13-load-testing.md) | The `tools/load` rig — how to drive the gateway at scale and capture pass/fail evidence per the Phase 4 exit criteria (E4-6) | SREs, anyone planning a staging load run |

## ADRs

Architecture Decision Records live in the [`adr/` directory](./05-architecture-decisions.md). Each ADR records *one* significant decision, *why* it was made, and *what was rejected*. ADRs are append-only — superseded ones are marked, not deleted.

Current:

- [ADR-0001 — Python 3.13 + asyncio as the primary runtime](./adr/0001-python-asyncio-stack.md)
- [ADR-0002 — Adopt `mobilityhouse/ocpp` as the protocol library](./adr/0002-mobilityhouse-ocpp-library.md)
- [ADR-0003 — Monorepo layout (`eveys/<service>`)](./adr/0003-monorepo-layout.md)

## How to read this

- **First time?** Read `00-overview.md`, then `01-roadmap.md`, then skim the rest.
- **Planning a sprint?** Read `06-implementation-plan.md` for the weekly schedule and `02-tasks.md` for the IDs.
- **Contributing code?** Read `03-coding-standards.md` and `04-contributing.md`.
- **Making a significant decision?** Add an ADR. Don't bury it in code or chat.

## Status & ownership

| Field | Value |
|---|---|
| Project | `eveys/ocpp` |
| Phase | Phase 3 (platform integration) — Phase 0 + 2 closed; long-tail E2-1 + Phase 3 in progress |
| Tech lead | TBD |
| Source of architecture truth | The ADRs (see [Architecture decisions](./05-architecture-decisions.md)) |
| License | Proprietary — Eveys |

## Building this site

This document set is rendered as a static HTML site by **Sphinx** with the **MyST** Markdown parser and the **Furo** theme. The Markdown source files in this directory are the canonical source — the HTML is generated from them.

### Requirements

- Python 3.10 or newer on `PATH` (Python 3.13 is the project target — see [ADR-0001](./adr/0001-python-asyncio-stack.md))
- A working network connection on first build (to install Sphinx and extensions)

The Makefile creates a self-contained `docs/.venv/` so the build does not depend on the engineer's system Python or any project-level virtualenv. The macOS system Python (3.8) is too old for modern Sphinx, so this isolation matters.

### Local build

From the `docs/` directory:

```bash
make install   # creates docs/.venv/ if missing and installs Sphinx, MyST, Furo
make html      # renders the site to docs/_build/html/ (depends on `install`)
```

Open `docs/_build/html/README.html` in a browser to view the rendered site.

If a Python 3.10+ interpreter is not on `PATH`, `make install` exits with a clear error pointing at the project Python target. On macOS, install Python 3.13 via Homebrew (`brew install python@3.13`) — it lands at `/opt/homebrew/bin/python3.13` and the Makefile picks it up automatically.

To override the interpreter (for example, to test on 3.12), pass `PYTHON=` explicitly:

```bash
make install PYTHON=$(which python3.12)
```

The build runs with `-W --keep-going -n`, so any warning (broken link, undefined reference, malformed table) fails the build. Fix warnings rather than relaxing the flags.

### Sharing a local build over the LAN

To share the rendered site with a teammate on the same network (Wi-Fi / office LAN / VPN), run the following from the repository root:

```bash
cd ocpp/docs
make install
make html
python3 -m http.server 8000 --bind 0.0.0.0 --directory _build/html
```

The site is then reachable at `http://<host-LAN-IP>:8000/` for anyone on the same network. Find the host's LAN IP with `ipconfig getifaddr en0` (macOS) or `hostname -I` (Linux). Stop the server with `Ctrl+C`.

Caveats:

- The teammate must be on the same network — RFC1918 addresses (`10.x`, `192.168.x`, `172.16-31.x`) are not reachable from the public internet.
- macOS may prompt to allow Python to accept incoming connections — accept it.
- For sharing outside the LAN or for a persistent URL, use the CI artifact path below instead.

### Cleanup

Two cleanup targets, both run from `docs/`:

```bash
make clean       # removes docs/_build/ only — keeps the venv, next build is fast
make distclean   # removes docs/_build/ AND docs/.venv/ — next build re-installs everything
```

Use `make clean` between routine rebuilds. Use `make distclean` when upgrading `requirements.txt`, switching Python versions, or troubleshooting a corrupt venv. Both are safe — they only touch directories listed in `.gitignore`, never source files.

### CI build

The `docs` workflow in `.github/workflows/docs.yml` runs on **tags matching `docs-v*`** only. It produces a 30-day artifact named `eveys-ocpp-docs-<tag>` containing `docs/_build/html`. To trigger a docs release:

```bash
git tag docs-v<n>
git push --tags
```

The artifact is downloadable from the GitHub Actions run page. The site is **not** auto-published — it is an internal-only artifact.

### Build configuration

| File | Purpose |
|---|---|
| `conf.py` | Sphinx config (theme, MyST extensions, exclusions, strict warnings) |
| `requirements.txt` | Pinned doc-build deps (Sphinx, MyST, Furo, sphinx-copybutton, linkify-it-py) |
| `Makefile` | `install` / `html` / `clean` targets |
| `_static/` | Theme assets (currently empty) |

The two hidden `{toctree}` directives below this section define the navigation. Add new top-level docs to the **Foundations** toctree; new ADRs are picked up automatically by the `adr/*` glob.

```{toctree}
:hidden:
:caption: Foundations

00-overview
01-roadmap
02-tasks
03-coding-standards
04-contributing
05-architecture-decisions
06-implementation-plan
07-local-dev-setup
08-ocpp-conformance
09-certification-readiness
10-testing-strategy
11-configuration-reference
12-connecting-real-charger
13-load-testing
```

```{toctree}
:hidden:
:caption: Architecture Decisions
:glob:

adr/*
```

```{toctree}
:hidden:
:caption: Backend integration

integration/README
integration/01-backend-rest-contract
integration/02-gateway-rest-api
integration/03-webhooks
```
