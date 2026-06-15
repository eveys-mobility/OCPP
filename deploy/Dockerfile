# Multi-stage build for the eveys/ocpp service.
#
# - codegen stage: dev deps installed; generates protobuf / grpclib stubs
#   into src/eveys_ocpp/_generated/.
# - builder stage: runtime deps only; copies the generated stubs from
#   codegen and installs the package against them. This is the venv
#   shipped to runtime — no dev tools.
# - runtime stage: distroless Python 3.13. Image target < 200 MB (E0-5).

# ---- codegen ----------------------------------------------------------------
FROM python:3.13.14-slim AS codegen

COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv

WORKDIR /codegen

# Codegen needs grpcio-tools (Python plugin) + grpclib (the
# protoc-gen-grpclib_python plugin binary). Install ONLY those in a
# throwaway venv — keeps this stage's footprint minimal.
RUN uv venv /opt/codegen-venv \
    && uv pip install --python /opt/codegen-venv/bin/python --no-cache-dir \
        "grpcio-tools>=1.66" "grpclib>=0.4.7"

COPY proto/ ./proto/
COPY src/eveys_ocpp/_generated/__init__.py ./_generated_init.py

RUN mkdir -p out/eveys_ocpp/_generated \
    && cp _generated_init.py out/eveys_ocpp/_generated/__init__.py \
    && /opt/codegen-venv/bin/python -m grpc_tools.protoc \
        --plugin=protoc-gen-grpclib_python=/opt/codegen-venv/bin/protoc-gen-grpclib_python \
        --proto_path=proto \
        --python_out=out/eveys_ocpp/_generated \
        --grpclib_python_out=out/eveys_ocpp/_generated \
        proto/ocpp_gw/v1/gateway.proto \
        proto/events/v1/events.proto \
    && find out/eveys_ocpp/_generated -mindepth 1 -type d -exec touch {}/__init__.py \;

# ---- builder ----------------------------------------------------------------
# Installs runtime-only deps into /opt/venv. Generated code copied from
# `codegen` so we never need grpcio-tools at runtime.
FROM python:3.13.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv

WORKDIR /build

COPY pyproject.toml README.md ./
COPY src/ ./src/
# Alembic config + migration scripts. Shipped in the runtime image so
# the compose init-container (and the Helm pre-install hook) can run
# `python -m alembic upgrade head` without a host bind-mount. See
# eveys-mobility/OCPP#234.
COPY alembic.ini ./alembic.ini
COPY alembic/ ./alembic/
# Replace the empty _generated dir from src/ with the generated stubs.
COPY --from=codegen /codegen/out/eveys_ocpp/_generated/ ./src/eveys_ocpp/_generated/

# Runtime install only (no `.[dev]` — no grpcio-tools, no pytest).
RUN uv venv /opt/venv \
    && uv pip install --python /opt/venv/bin/python --no-cache-dir .

# ---- runtime ----------------------------------------------------------------
# Distroless python3, debian13. We copy our own Python 3.13 from the builder
# (per ADR-0001) — distroless ships an older Python that's a few minor
# versions behind. Debian13 base matches `python:3.13-slim`'s GLIBC ≥ 2.38.
FROM gcr.io/distroless/python3-debian13:nonroot AS runtime

COPY --from=builder /usr/local/lib/python3.13 /usr/local/lib/python3.13
COPY --from=builder /usr/local/lib/libpython3.13.so.1.0 /usr/local/lib/libpython3.13.so.1.0
COPY --from=builder /usr/local/bin/python3.13 /usr/local/bin/python3.13
COPY --from=builder /opt/venv /opt/venv
# Carry alembic.ini + alembic/ from the builder into the runtime
# layer (under /build, same path the builder used) so `python -m
# alembic` finds them when the init-container runs. Two thin copies;
# the contents are <50KB.
COPY --from=builder /build/alembic.ini /build/alembic.ini
COPY --from=builder /build/alembic/ /build/alembic/

WORKDIR /build

ENV PATH="/opt/venv/bin:/usr/local/bin:${PATH}" \
    LD_LIBRARY_PATH="/usr/local/lib" \
    PYTHONPATH="/opt/venv/lib/python3.13/site-packages" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER nonroot

# WS, gRPC, Prometheus metrics. Match `Settings` defaults.
EXPOSE 9000 50051 9100

# ENTRYPOINT/CMD split: the entrypoint is just the interpreter, the
# default command runs the gateway. Splitting them this way lets a
# sibling container (the `clickhouse-ingestor`) reuse this image and
# run a different module by overriding `command:` in compose without
# having to override the entrypoint as well. With the previous combined
# `ENTRYPOINT ["python", "-m", "eveys_ocpp"]` shape, compose's
# `command:` was silently appended as positional args and the entry
# module ran regardless — the kind of misconfiguration the new
# compose-smoke tier exists to catch (see ADR-0024).
ENTRYPOINT ["/usr/local/bin/python3.13"]
CMD ["-m", "eveys_ocpp"]
