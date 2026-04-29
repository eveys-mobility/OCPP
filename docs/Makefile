# Minimal Makefile for Sphinx documentation
#
# Builds the docs site in a self-contained virtualenv (.venv/) so the build
# does not depend on the system Python. The project targets Python 3.13
# (see ADR-0001); Sphinx 7.3+ requires Python 3.10+ regardless.

SPHINXOPTS    ?= -W --keep-going -n
SOURCEDIR     = .
BUILDDIR      = _build
VENV          = .venv
PYTHON        ?= $(shell command -v python3.13 || command -v python3.12 || command -v python3.11 || command -v python3.10)
VENV_PY       = $(VENV)/bin/python
VENV_PIP      = $(VENV)/bin/pip
SPHINXBUILD   = $(VENV)/bin/sphinx-build

.PHONY: help html clean install venv

help:
	@echo "Targets:"
	@echo "  make install   create $(VENV)/ and install Sphinx + extensions"
	@echo "  make html      build the HTML site into $(BUILDDIR)/html"
	@echo "  make clean     remove $(BUILDDIR)/"
	@echo "  make distclean remove $(BUILDDIR)/ and $(VENV)/"

$(VENV)/bin/activate:
	@if [ -z "$(PYTHON)" ]; then \
		echo "ERROR: no Python 3.10+ interpreter found on PATH."; \
		echo "Install Python 3.13 (project target, per ADR-0001) and retry."; \
		exit 1; \
	fi
	@echo "Creating virtualenv with $(PYTHON) ($$($(PYTHON) --version))"
	$(PYTHON) -m venv $(VENV)
	$(VENV_PIP) install --upgrade pip

venv: $(VENV)/bin/activate

install: venv
	$(VENV_PIP) install -r requirements.txt

html: install
	$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS)

clean:
	rm -rf "$(BUILDDIR)"

distclean: clean
	rm -rf "$(VENV)"
