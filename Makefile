# eveys/ocpp — top-level Makefile.
#
# Targets are grouped by concern. Run `make help` for a printed summary.

# Use bash for recipe execution so `set -euo pipefail` works in scripts.
SHELL := /usr/bin/env bash

# uv-managed virtualenv (created by `make install`).
VENV       ?= .venv
PYTHON     ?= $(shell command -v python3.13 || command -v python3.12)
UV         ?= $(shell command -v uv)
# `--env-file .env` is conditional: present when the developer has
# created a repo-root .env, absent otherwise (so CI / fresh checkouts
# don't fail because the file is missing). Compose's own variable
# substitution still uses the file when it's there.
COMPOSE_ENV := $(if $(wildcard .env),--env-file .env,)
COMPOSE    := docker compose -f deploy/compose/docker-compose.yml $(COMPOSE_ENV)
# Observability sidecar (Grafana + Prometheus). Overlays the base
# stack so Grafana joins the same Docker network and resolves the
# datasources by service name. Opt-in via `make grafana-up`.
COMPOSE_GRAFANA := docker compose \
    -f deploy/compose/docker-compose.yml \
    -f deploy/compose/docker-compose.grafana.yml \
    $(COMPOSE_ENV)

.PHONY: help doctor install format lint types tests e2e smoke compose-smoke precommit clean distclean \
        compose-up compose-down compose-status compose-down-volumes compose-wait \
        build-image protoc ch-migrate docs docs-clean config-export config-export-check config-schema \
        openapi-export openapi-export-check audit get-token grafana-up grafana-down

# ---- meta -------------------------------------------------------------------

help:
	@echo "Environment:"
	@echo "  make doctor             check local-dev tools (per docs/07-local-dev-setup.md)"
	@echo "  make install            create $(VENV)/ via uv and install runtime + dev deps"
	@echo ""
	@echo "Code quality:"
	@echo "  make format             apply isort + black"
	@echo "  make lint               run ruff check"
	@echo "  make types              run mypy --strict on src/"
	@echo "  make tests              full pre-commit gate: lint + types + pytest with coverage"
	@echo "  make audit              pip-audit against resolved venv (E5-8)"
	@echo "  make precommit          run all pre-commit hooks against every file (no commit needed)"
	@echo ""
	@echo "Local stack (data plane):"
	@echo "  make compose-up         start Postgres + Redis + Kafka + ClickHouse + service"
	@echo "  make compose-wait       wait for all containers to report 'healthy'"
	@echo "  make compose-down       stop containers, keep data volumes"
	@echo "  make compose-down-volumes  stop AND wipe data (DESTRUCTIVE)"
	@echo "  make compose-status     show container health"
	@echo "  make build-image        build the eveys-ocpp:dev container image"
	@echo "  make ch-migrate         apply ClickHouse migrations (E2-13, ADR-0020)"
	@echo "  make e2e                full e2e: compose-up + alembic + ch-migrate + e2e tests + compose-down"
	@echo "  make compose-smoke      Tier-3 smoke: build image + bring real compose stack up + assert it stays up + drive a charger flow (see ADR-0024)"
	@echo "  make get-token          print a bearer token from EVEYS_OCPP_REST_INBOUND_TOKENS (paste into Swagger UI Authorize)"
	@echo "  make grafana-up         start Grafana + Prometheus sidecar on the compose network (http://localhost:3000)"
	@echo "  make grafana-down       stop the Grafana + Prometheus sidecar (data volumes kept)"
	@echo ""
	@echo "Docs:"
	@echo "  make docs               build the docs site (Sphinx + MyST)"
	@echo "  make docs-clean         remove docs/_build/"
	@echo ""
	@echo "Config reference (ADR-0025):"
	@echo "  make config-export       regenerate docs/11-configuration-reference.md + .env.example"
	@echo "  make config-export-check fail if either file is out of date with src/eveys_ocpp/settings.py"
	@echo "  make openapi-export      regenerate docs/api/openapi.{json,yaml} from the FastAPI app"
	@echo "  make openapi-export-check fail if the committed OpenAPI files drift from the FastAPI app"
	@echo "  make config-schema       print Settings as JSON Schema (for Helm validators, operator UIs)"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean              remove build/test caches"
	@echo "  make distclean          clean + remove $(VENV)/ and docs/.venv/"

doctor:
	@./scripts/doctor.sh

# ---- environment ------------------------------------------------------------

$(VENV)/bin/python:
	@if [ -z "$(UV)" ]; then \
		echo "ERROR: uv not found on PATH — run 'brew install uv' (see make doctor)"; \
		exit 1; \
	fi
	@if [ -z "$(PYTHON)" ]; then \
		echo "ERROR: Python 3.13 not found — run 'brew install python@3.13'"; \
		exit 1; \
	fi
	$(UV) venv --python $(PYTHON) $(VENV)

install: $(VENV)/bin/python
	$(UV) pip install --python $(VENV)/bin/python -e ".[dev]"
	@$(MAKE) protoc
	@# Activate pre-commit hooks if the config exists. Idempotent — safe
	@# to re-run; pre-commit detects an already-installed hook script.
	@if [ -f .pre-commit-config.yaml ] && [ -d .git ]; then \
		$(VENV)/bin/pre-commit install --install-hooks >/dev/null 2>&1 && \
		$(VENV)/bin/pre-commit install --hook-type commit-msg >/dev/null 2>&1 && \
		echo "pre-commit hooks installed" >&2; \
	fi

# Regenerate Python stubs from proto/ into src/eveys_ocpp/_generated/.
# Generated files are .gitignored — regenerate after pyproject install or
# whenever a .proto file changes. Idempotent.
protoc: $(VENV)/bin/python
	@mkdir -p src/eveys_ocpp/_generated
	@# protoc-gen-grpclib_python ships with the `grpclib` package and is
	@# at $(VENV)/bin/. Pass via --plugin so $PATH lookup isn't needed.
	$(VENV)/bin/python -m grpc_tools.protoc \
		--plugin=protoc-gen-grpclib_python=$(VENV)/bin/protoc-gen-grpclib_python \
		--proto_path=proto \
		--python_out=src/eveys_ocpp/_generated \
		--grpclib_python_out=src/eveys_ocpp/_generated \
		proto/ocpp_gw/v1/gateway.proto \
		proto/events/v1/events.proto
	@# protoc emits files into directory hierarchy matching package; add
	@# __init__.py at every nested level so they're importable. The
	@# top-level _generated/__init__.py is hand-written and adds itself
	@# to sys.path so the generated absolute imports resolve.
	@find src/eveys_ocpp/_generated -mindepth 1 -type d -exec touch {}/__init__.py \;

# ---- code quality -----------------------------------------------------------

format: install
	$(VENV)/bin/isort src tests scripts
	$(VENV)/bin/black src tests scripts

lint: install
	$(VENV)/bin/ruff check src tests scripts

types: install
	$(VENV)/bin/mypy

tests: lint types
	$(VENV)/bin/pytest

# E5-8 — pip-audit scans the resolved venv against the PyPI advisory DB
# (PyUp / OSV). Local mirror of the CI job in .github/workflows/security.yml
# so engineers can repro a CVE finding before pushing. Non-zero exit on
# any unfixed advisory.
#
# `--skip-editable` excludes the eveys-ocpp package itself (it's installed
# editable; pip-audit can't look it up on PyPI). The project path argument
# tells pip-audit to audit the resolved environment from `pyproject.toml`,
# which is the surface we actually ship.
audit: install
	$(VENV)/bin/pip-audit --strict --skip-editable .

precommit: install
	$(VENV)/bin/pre-commit run --all-files

# ---- local stack ------------------------------------------------------------

build-image:
	docker build -f deploy/Dockerfile -t eveys-ocpp:dev .

compose-up:
	$(COMPOSE) up -d
	@echo ""
	@echo "Stack starting. Use 'make compose-status' to watch health."

# Opt-in observability sidecar: Grafana + Prometheus joined to the
# same compose network. The Grafana container pulls the ClickHouse
# plugin on first boot — first run needs network, subsequent runs
# read from the cached volume. The base stack must already be up
# (`make compose-up` first) — Grafana's datasources resolve service
# names like `prometheus` and `clickhouse` over the project network.
grafana-up:
	$(COMPOSE_GRAFANA) up -d prometheus grafana
	@echo ""
	@echo "Grafana:    http://localhost:3000  (anonymous Admin — dev only)"
	@echo "Prometheus: http://localhost:9090"
	@echo ""
	@echo "Dashboards land under folder 'eveys/ocpp'. The per-charger"
	@echo "drill-down (03) is the one most likely to have data on a fresh"
	@echo "stack — it reads from ClickHouse, which the gateway populates"
	@echo "as soon as a charger connects."

grafana-down:
	$(COMPOSE_GRAFANA) stop grafana prometheus
	$(COMPOSE_GRAFANA) rm -f grafana prometheus

# Print a bearer token an operator can paste into the Swagger UI's
# "Authorize" dialog. Reads from the shell env first, then falls back
# to the repo-root `.env` file (the documented dev path). The
# allowlist is CSV; we print the first entry.
#
# Quiet target — `@` on every line so the output is just the token,
# safe to pipe into pbcopy / xclip.
get-token:
	@token="$$EVEYS_OCPP_REST_INBOUND_TOKENS"; \
	if [ -z "$$token" ] && [ -f .env ]; then \
	  token=$$(grep -E '^EVEYS_OCPP_REST_INBOUND_TOKENS=' .env | head -n1 | cut -d= -f2- | tr -d '"' | tr -d "'"); \
	fi; \
	if [ -z "$$token" ]; then \
	  echo "ERROR: EVEYS_OCPP_REST_INBOUND_TOKENS is not set in shell env or .env" >&2; \
	  echo "       Add a value to .env (e.g. EVEYS_OCPP_REST_INBOUND_TOKENS=dev-token) and re-run." >&2; \
	  exit 1; \
	fi; \
	echo "$$token" | cut -d, -f1

compose-down:
	$(COMPOSE) down

compose-down-volumes:
	@echo "WARNING: this will DELETE all local Postgres / Kafka / ClickHouse data."
	@read -p "Continue? [y/N] " ans; [ "$$ans" = "y" ] || [ "$$ans" = "Y" ] || (echo "Aborted." && exit 1)
	$(COMPOSE) down --volumes

compose-status:
	$(COMPOSE) ps

# Block until every container with a healthcheck reports `healthy`. Used
# both by `make e2e` and as a standalone debugging tool when a service
# is slow to start (Kafka in particular takes 20–30s on first boot).
compose-wait:
	@echo "Waiting for stack to be healthy (timeout 120s)..."
	@for i in $$(seq 1 60); do \
		not_ready=$$($(COMPOSE) ps --format json 2>/dev/null | python3 -c "\
import sys, json; \
names = []; \
[names.append(c.get('Name', '?')) for line in sys.stdin if line.strip() for c in [json.loads(line)] if c.get('Health') and c['Health'] != 'healthy']; \
print(' '.join(names))" 2>/dev/null); \
		if [ -z "$$not_ready" ]; then echo "All containers healthy."; exit 0; fi; \
		echo "  still waiting on: $$not_ready"; \
		sleep 2; \
	done; \
	echo "ERROR: timed out waiting for healthy stack."; \
	$(COMPOSE) ps; \
	exit 1

# Local equivalent of the GitHub Actions `e2e` workflow. Brings up the data
# plane, applies schema, runs the e2e tests, tears down. Idempotent —
# safe to re-run.
# ClickHouse schema migrator. Idempotent; reads SQL files from
# src/eveys_ocpp/clickhouse/ddl/ and applies any not yet recorded in
# the schema_migrations table. See ADR-0020.
#
# Default HTTP port is 8124 to match docker-compose's host mapping. The
# canonical CH HTTP port is 8123, but a Homebrew `clickhouse server`
# already running on the laptop will steal it (loopback bind wins over
# docker's `*:8123`) and migrations silently target the wrong server.
# CI overrides this via E2E_CH_HTTP_PORT=8123 because the GitHub
# Actions runner binds CH directly on 8123 with no collision risk.
ch-migrate: install
	@echo ">> applying ClickHouse migrations..."
	@$(VENV)/bin/python -m eveys_ocpp.clickhouse.migrate \
	    --host $${E2E_CH_HOST:-localhost} \
	    --port $${E2E_CH_HTTP_PORT:-8124} \
	    --db $${EVEYS_OCPP_CLICKHOUSE_DB:-eveys_ocpp}

e2e: install
	@echo ">> bringing up local data plane..."
	@$(MAKE) compose-up
	@$(MAKE) compose-wait
	@echo ">> applying schema..."
	@$(VENV)/bin/alembic upgrade head
	@$(MAKE) ch-migrate
	@echo ">> running e2e tests..."
	@$(VENV)/bin/pytest tests/e2e -v --no-cov; \
	rc=$$?; \
	echo ">> tearing down..."; \
	$(MAKE) compose-down; \
	exit $$rc

# Tier-3 compose-smoke (ADR-0024). Builds the production-shaped image
# from `deploy/Dockerfile`, brings the entire compose stack up, applies
# Postgres + ClickHouse schemas, runs `tests/compose_smoke/` against
# the running stack, tears down. Catches the bug class that ships
# green through unit + integration tests but breaks `docker compose up`
# on a fresh dev laptop or in production. See `docs/10-testing-strategy.md`.
#
# Slower than `make tests` (~ 90s); not run as part of `make tests` so
# the fast inner loop stays fast. CI runs it on MRs that touch
# `deploy/`, `tests/compose_smoke/`, or `pyproject.toml`.
compose-smoke: install
	@echo ">> Tier-3 compose smoke (ADR-0024)..."
	@echo ">> running compose-smoke tests against the production-shaped stack..."
	@echo "   (the suite's session fixture owns docker compose up/down + schema apply)"
	@COMPOSE_SMOKE=1 $(VENV)/bin/pytest tests/compose_smoke -v --no-cov; \
	rc=$$?; \
	echo ">> capturing container logs as artifacts (best-effort)..."; \
	mkdir -p .compose-smoke-logs && \
	for c in eveys-ocpp eveys-ocpp-clickhouse-ingestor eveys-ocpp-postgres eveys-ocpp-redis eveys-ocpp-kafka eveys-ocpp-clickhouse; do \
	    docker logs $$c > .compose-smoke-logs/$$c.log 2>&1 || true; \
	done; \
	exit $$rc

# ---- E3-10 mock backend -----------------------------------------------------
# Boots the dev-only mock implementing docs/integration/01-backend-rest-contract.md.
# Used by E3-2..E3-6 wiring work; not part of the production runtime.
# Defaults: bind 0.0.0.0:9200, bearer token "dev-token", accept all id_tags.
# Override via MOCK_BACKEND_* env vars (see tests/mock_backend/__init__.py).
mock-backend: install
	@echo ">> booting mock backend on http://localhost:$${MOCK_BACKEND_PORT:-9200} ..."
	@$(VENV)/bin/python -m tests.mock_backend

# ---- docs (delegates to docs/Makefile) --------------------------------------

docs:
	$(MAKE) -C docs html

docs-clean:
	$(MAKE) -C docs clean

# ---- config reference (ADR-0025) --------------------------------------------
#
# `Settings` is the source of truth for docs/11-configuration-reference.md
# and .env.example. Regenerate both with `make config-export` whenever you
# add or change a field. CI runs `--check` (E0-14) to refuse drift.

config-export: install
	$(VENV)/bin/python scripts/render_config_reference.py

config-export-check: install
	$(VENV)/bin/python scripts/render_config_reference.py --check

# Print Settings as JSON Schema. Plumbing for downstream surfaces
# (Helm values validators, operator UIs, CLI --help) that want the
# metadata in a machine-consumable shape rather than re-parsing the
# Markdown reference. No file is written; pipe to a file when needed
# (`make config-schema > settings.schema.json`). Per ADR-0025.
config-schema: install
	@$(VENV)/bin/python -c "import json; from eveys_ocpp.settings import Settings; print(json.dumps(Settings.model_json_schema(), indent=2))"

# ---- OpenAPI spec ----------------------------------------------------------
#
# Regenerate `docs/api/openapi.{json,yaml}` from the FastAPI app. This
# is the canonical artifact for sharing with backend teams / Postman /
# external Swagger UIs. The runtime-mounted UI at `/api/v1/docs` (gated
# by `EVEYS_OCPP_REST_OPENAPI_ENABLED=true`) is the dev-time clickable
# equivalent. CI runs `--check` to refuse drift.
openapi-export: install
	$(VENV)/bin/python scripts/export_openapi.py

openapi-export-check: install
	$(VENV)/bin/python scripts/export_openapi.py --check

# ---- cleanup ----------------------------------------------------------------

clean: docs-clean
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov coverage.xml
	find . -type d -name '__pycache__' -not -path './$(VENV)/*' -not -path './docs/.venv/*' -exec rm -rf {} +

distclean: clean
	rm -rf $(VENV)
	$(MAKE) -C docs distclean
