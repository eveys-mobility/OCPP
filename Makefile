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

.PHONY: help doctor install format lint types tests smoke clean distclean \
        compose-up compose-down compose-status compose-down-volumes \
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
	@echo "  make compose-down       stop containers, keep data volumes"
	@echo "  make compose-down-volumes  stop AND wipe data (DESTRUCTIVE)"
	@echo "  make compose-status     show container health"
	@echo "  make build-image        build the eveys-ocpp:dev container image"
	@echo "  make smoke              run the local-stack smoke test (E1-13)"
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

# Lands with E1-13. Until then, stub that explains the gap.
smoke:
	@if [ ! -f tests/e2e/test_local_smoke.py ]; then \
		echo "tests/e2e/test_local_smoke.py does not exist yet (delivered by task E1-13)."; \
		echo "Manual verification commands are listed in docs/07-local-dev-setup.md."; \
		exit 1; \
	fi
	$(VENV)/bin/pytest tests/e2e/test_local_smoke.py -v

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
