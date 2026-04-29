# eveys/ocpp — top-level Makefile.
#
# Targets are grouped by concern. Run `make help` for a printed summary.

# Use bash for recipe execution so `set -euo pipefail` works in scripts.
SHELL := /usr/bin/env bash

# uv-managed virtualenv (created by `make install`).
VENV       ?= .venv
PYTHON     ?= $(shell command -v python3.13 || command -v python3.12)
UV         ?= $(shell command -v uv)
COMPOSE    := docker compose -f deploy/compose/docker-compose.yml

.PHONY: help doctor install format lint types tests e2e smoke clean distclean \
        compose-up compose-down compose-status compose-down-volumes compose-wait \
        build-image docs docs-clean

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
	@echo ""
	@echo "Local stack (data plane):"
	@echo "  make compose-up         start Postgres + Redis + Kafka + ClickHouse + service"
	@echo "  make compose-wait       wait for all containers to report 'healthy'"
	@echo "  make compose-down       stop containers, keep data volumes"
	@echo "  make compose-down-volumes  stop AND wipe data (DESTRUCTIVE)"
	@echo "  make compose-status     show container health"
	@echo "  make build-image        build the eveys-ocpp:dev container image"
	@echo "  make e2e                full e2e: compose-up + alembic + e2e tests + compose-down"
	@echo ""
	@echo "Docs:"
	@echo "  make docs               build the docs site (Sphinx + MyST)"
	@echo "  make docs-clean         remove docs/_build/"
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

# ---- code quality -----------------------------------------------------------

format: install
	$(VENV)/bin/isort src tests
	$(VENV)/bin/black src tests

lint: install
	$(VENV)/bin/ruff check src tests

types: install
	$(VENV)/bin/mypy

tests: lint types
	$(VENV)/bin/pytest

# ---- local stack ------------------------------------------------------------

build-image:
	docker build -f deploy/Dockerfile -t eveys-ocpp:dev .

compose-up:
	$(COMPOSE) up -d
	@echo ""
	@echo "Stack starting. Use 'make compose-status' to watch health."

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

# Local equivalent of the GitLab `tests:e2e` job. Brings up the data
# plane, applies schema, runs the e2e tests, tears down. Idempotent —
# safe to re-run.
e2e: install
	@echo ">> bringing up local data plane..."
	@$(MAKE) compose-up
	@$(MAKE) compose-wait
	@echo ">> applying schema..."
	@$(VENV)/bin/alembic upgrade head
	@echo ">> running e2e tests..."
	@$(VENV)/bin/pytest tests/e2e -v --no-cov; \
	rc=$$?; \
	echo ">> tearing down..."; \
	$(MAKE) compose-down; \
	exit $$rc

# ---- docs (delegates to docs/Makefile) --------------------------------------

docs:
	$(MAKE) -C docs html

docs-clean:
	$(MAKE) -C docs clean

# ---- cleanup ----------------------------------------------------------------

clean: docs-clean
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov coverage.xml
	find . -type d -name '__pycache__' -not -path './$(VENV)/*' -not -path './docs/.venv/*' -exec rm -rf {} +

distclean: clean
	rm -rf $(VENV)
	$(MAKE) -C docs distclean
