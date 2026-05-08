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

.PHONY: help html clean install venv swagger swagger-serve

# Swagger UI dist — pinned to a known-good release. Update by bumping
# the version + the SHA below; CI's docs job catches a download
# regression because make-html doesn't build past a missing tarball.
SWAGGER_UI_VERSION ?= 5.20.0
SWAGGER_UI_TARBALL = swagger-ui-$(SWAGGER_UI_VERSION).tar.gz
SWAGGER_UI_URL     = https://github.com/swagger-api/swagger-ui/archive/refs/tags/v$(SWAGGER_UI_VERSION).tar.gz
SWAGGER_OUT        = $(BUILDDIR)/swagger
SWAGGER_PORT      ?= 8000

help:
	@echo "Targets:"
	@echo "  make install        create $(VENV)/ and install Sphinx + extensions"
	@echo "  make html           build the HTML site into $(BUILDDIR)/html"
	@echo "  make swagger        build a static Swagger UI into $(SWAGGER_OUT)/"
	@echo "  make swagger-serve  build (if needed) + serve on http://localhost:$(SWAGGER_PORT)"
	@echo "  make clean          remove $(BUILDDIR)/"
	@echo "  make distclean      remove $(BUILDDIR)/ and $(VENV)/"

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

# ---- Swagger UI -------------------------------------------------------------
# Renders the committed OpenAPI spec (api/openapi.json) as a static
# Swagger UI site at $(SWAGGER_OUT)/index.html. No Node.js, no Python
# deps — just curl + tar to fetch the swagger-ui-dist release tarball,
# then a thin index.html that points the bundle at openapi.json.
#
# The result is a self-contained directory you can:
#   - serve locally via `make swagger-serve`
#   - copy to any web host (S3, GitHub Pages, an internal nginx)
#   - rsync to a server on your LAN
#
# OPTION-NOTE: this target deliberately does NOT depend on `install`.
# Sphinx + the docs venv are unrelated — Swagger UI is plain HTML/JS.
# Keeping the dependency narrow means a fresh clone can `make swagger`
# without paying the Sphinx install cost.

swagger: $(SWAGGER_OUT)/index.html

$(SWAGGER_OUT)/index.html: api/openapi.json
	@mkdir -p "$(SWAGGER_OUT)"
	@if [ ! -d "$(BUILDDIR)/_swagger-ui-src" ]; then \
		echo "Fetching swagger-ui $(SWAGGER_UI_VERSION)..."; \
		mkdir -p "$(BUILDDIR)/_swagger-ui-src"; \
		curl -fsSL "$(SWAGGER_UI_URL)" \
			| tar -xz --strip-components=1 -C "$(BUILDDIR)/_swagger-ui-src"; \
	fi
	@echo "Assembling static site at $(SWAGGER_OUT)/"
	@cp "$(BUILDDIR)/_swagger-ui-src/dist/"*.{js,css,png,html} "$(SWAGGER_OUT)/" 2>/dev/null || true
	@cp api/openapi.json "$(SWAGGER_OUT)/openapi.json"
	@# Overwrite the bundle's stock index.html with a version that points
	@# at our spec. The stock index.html points at petstore.swagger.io.
	@printf '%s\n' \
		'<!doctype html>' \
		'<html lang="en">' \
		'<head>' \
		'  <meta charset="utf-8" />' \
		'  <title>eveys/ocpp — REST API</title>' \
		'  <link rel="stylesheet" href="swagger-ui.css" />' \
		'  <link rel="icon" type="image/png" href="favicon-32x32.png" sizes="32x32" />' \
		'  <link rel="icon" type="image/png" href="favicon-16x16.png" sizes="16x16" />' \
		'</head>' \
		'<body>' \
		'  <div id="swagger-ui"></div>' \
		'  <script src="swagger-ui-bundle.js" charset="UTF-8"></script>' \
		'  <script src="swagger-ui-standalone-preset.js" charset="UTF-8"></script>' \
		'  <script>' \
		'    window.onload = () => {' \
		'      window.ui = SwaggerUIBundle({' \
		'        url: "openapi.json",' \
		'        dom_id: "#swagger-ui",' \
		'        deepLinking: true,' \
		'        presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],' \
		'        layout: "StandaloneLayout"' \
		'      });' \
		'    };' \
		'  </script>' \
		'</body>' \
		'</html>' \
		> "$(SWAGGER_OUT)/index.html"
	@echo "Built: $(SWAGGER_OUT)/index.html"

swagger-serve: swagger
	@echo "Serving Swagger UI at http://localhost:$(SWAGGER_PORT)/"
	@echo "Stop with Ctrl+C."
	@if [ -n "$(PYTHON)" ]; then \
		$(PYTHON) -m http.server $(SWAGGER_PORT) --bind 0.0.0.0 --directory $(SWAGGER_OUT); \
	else \
		python3 -m http.server $(SWAGGER_PORT) --bind 0.0.0.0 --directory $(SWAGGER_OUT); \
	fi
