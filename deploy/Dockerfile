# Multi-stage build for the eveys/ocpp service.
#
# Builder stage: Python 3.13 slim with build toolchain. Installs runtime deps
# into /opt/venv via uv (matching the project's local Makefile).
# Runtime stage: Google distroless python3 (debian12). No shell, no package
# manager, runs as non-root by default. Image target < 200 MB (task E0-5).

# ---- builder ----------------------------------------------------------------
FROM python:3.13-slim AS builder

# Install uv (the same package manager we use locally — see ADR-0001 / E0-2).
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv

WORKDIR /build

# Copy only the metadata first so the dependency layer caches across source
# changes. Hatchling needs README.md present at build time.
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Create a self-contained venv at /opt/venv with runtime-only deps + the
# package itself. `--no-dev` excludes the `[dev]` extras (ruff, mypy, pytest).
RUN uv venv /opt/venv \
    && uv pip install --python /opt/venv/bin/python --no-cache-dir .

# ---- runtime ----------------------------------------------------------------
# Distroless python3, debian13. We copy our own Python 3.13 from the builder
# (per ADR-0001) — distroless ships an older Python that's a few minor
# versions behind. Debian13 base matches `python:3.13-slim`'s GLIBC ≥ 2.38.
#
# Why distroless: minimal attack surface (no shell, no apt), reproducible,
# small. Trade-off: debugging in-container needs `kubectl debug --image=busybox`
# style ephemeral containers.
FROM gcr.io/distroless/python3-debian13:nonroot AS runtime

# Copy Python 3.13 + the venv from the builder. The builder's slim image has
# Python 3.13 at /usr/local; distroless has 3.11 at /usr/bin. We use the
# builder's interpreter to run our venv so the version matches ADR-0001.
COPY --from=builder /usr/local/lib/python3.13 /usr/local/lib/python3.13
COPY --from=builder /usr/local/lib/libpython3.13.so.1.0 /usr/local/lib/libpython3.13.so.1.0
COPY --from=builder /usr/local/bin/python3.13 /usr/local/bin/python3.13
COPY --from=builder /opt/venv /opt/venv

# Make the venv's interpreter the default `python` (distroless has no shell,
# so we set PATH instead of activating). LD_LIBRARY_PATH lets the dynamic
# linker find libpython3.13.so since /usr/local/lib isn't on the default path
# in the distroless base.
ENV PATH="/opt/venv/bin:/usr/local/bin:${PATH}" \
    LD_LIBRARY_PATH="/usr/local/lib" \
    PYTHONPATH="/opt/venv/lib/python3.13/site-packages" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Distroless `nonroot` user is uid 65532; image already runs as that user.
USER nonroot

# WS, gRPC, Prometheus metrics. Match `Settings` defaults.
EXPOSE 9000 50051 9100

ENTRYPOINT ["/usr/local/bin/python3.13", "-m", "eveys_ocpp"]
