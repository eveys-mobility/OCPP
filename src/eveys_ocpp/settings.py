"""Environment-driven configuration for eveys/ocpp.

All runtime configuration goes through this module. Direct `os.environ`
reads elsewhere are forbidden (see `03-coding-standards.md`).

Every field carries metadata required by ADR-0025: `description=` plus
`json_schema_extra={category, impact, secret, stability}`. The doc at
`docs/11-configuration-reference.md` and `.env.example` are generated
from this model by `scripts/render_config_reference.py`.
"""

from __future__ import annotations

import socket
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Top-level settings, populated from environment variables.

    All vars are prefixed `EVEYS_OCPP_` to avoid collisions with sibling
    services in the same monorepo.
    """

    model_config = SettingsConfigDict(
        env_prefix="EVEYS_OCPP_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    # ---- WS server ------------------------------------------------------
    ws_host: str = Field(
        default="0.0.0.0",
        description="Bind address for the OCPP WebSocket server.",
        json_schema_extra={
            "category": "ws_server",
            "impact": (
                "Restricting from `0.0.0.0` (all interfaces) to a specific "
                "NIC limits which network reaches chargers."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    ws_port: int = Field(
        default=9000,
        ge=1,
        le=65535,
        description="Port the WS server listens on.",
        json_schema_extra={
            "category": "ws_server",
            "impact": (
                "Must match the docker-compose container port mapping and the "
                "charger's CSMS URL. Container exposes 9000 internally; host "
                "port may be remapped (e.g. 19000)."
            ),
            "secret": False,
            "stability": "structural",
        },
    )

    # ---- gRPC server ----------------------------------------------------
    grpc_host: str = Field(
        default="0.0.0.0",
        description=(
            "Bind address for the inbound gRPC server (sibling services "
            "call into it for `RemoteStart`, `Reset`, etc.)."
        ),
        json_schema_extra={
            "category": "grpc_server",
            "impact": "Same as WS_HOST: which NIC accepts gRPC.",
            "secret": False,
            "stability": "structural",
        },
    )
    grpc_port: int = Field(
        default=50051,
        ge=1,
        le=65535,
        description="Port the gRPC server listens on.",
        json_schema_extra={
            "category": "grpc_server",
            "impact": (
                "All sibling services must agree on this; changing it "
                "requires a coordinated rollout."
            ),
            "secret": False,
            "stability": "structural",
        },
    )

    # ---- REST server (ADR-0026, E3-7..E3-8) -----------------------------
    rest_enabled: bool = Field(
        default=True,
        description=(
            "Whether to start the in-process REST server alongside WS "
            "and gRPC. Set to False in shapes that share this image but "
            "should not serve HTTP (e.g. the clickhouse-ingestor sidecar)."
        ),
        json_schema_extra={
            "category": "rest_server",
            "impact": (
                "When False the gateway pod has no `/api/v1/*` surface; "
                "the backend cannot poll read state."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    rest_host: str = Field(
        default="0.0.0.0",
        description="Bind address for the inbound REST API (ADR-0026).",
        json_schema_extra={
            "category": "rest_server",
            "impact": (
                "Restricting from `0.0.0.0` to a specific NIC limits which "
                "network reaches the backend-facing REST surface."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    rest_port: int = Field(
        default=8080,
        ge=1,
        le=65535,
        description="Port the REST server listens on.",
        json_schema_extra={
            "category": "rest_server",
            "impact": (
                "Production network policy must allow only the backend / "
                "operator UI to reach this port — distinct from the WS "
                "(9000) charger-facing port."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    rest_inbound_tokens: SecretStr = Field(
        default=SecretStr(""),
        description=(
            "Comma-separated bearer tokens accepted on inbound REST "
            "requests. Multi-value to support rotation across consumers "
            "(eveys-backend, billing back-fill, operator UI). Each token "
            "must match exactly; whitespace is stripped."
        ),
        json_schema_extra={
            "category": "auth",
            "impact": (
                "Empty allowlist + `rest_auth_disabled=False` (the "
                "default) → all inbound requests are rejected with 401. "
                "Stored as `SecretStr` (E5-7) so a stray `print(settings)` "
                "or unstructured-log dump shows the redacted placeholder; "
                "call `.get_secret_value()` at the explicit point of use."
            ),
            "secret": True,
            "stability": "tunable",
        },
    )
    rest_auth_disabled: bool = Field(
        default=False,
        description=(
            "Disable bearer-token validation entirely. Dev / laptop / "
            "unit-test convenience only — never set in production."
        ),
        json_schema_extra={
            "category": "auth",
            "impact": (
                "When True the gateway accepts any (or no) Authorization "
                "header on `/api/v1/*`. The boot-time log line "
                "`rest_auth.disabled=True` makes a forgotten flip "
                "obvious in any log review."
            ),
            "secret": False,
            "stability": "dev-only",
        },
    )
    rest_default_page_size: int = Field(
        default=100,
        ge=1,
        le=10_000,
        description="Default `limit` for cursor-paginated read endpoints.",
        json_schema_extra={
            "category": "rest_server",
            "impact": (
                "Higher → fewer round-trips for the backend, more rows "
                "per response and per query. Lower → opposite."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    rest_max_page_size: int = Field(
        default=500,
        ge=1,
        le=10_000,
        description="Hard cap on `limit` for cursor-paginated read endpoints.",
        json_schema_extra={
            "category": "rest_server",
            "impact": (
                "Operators can lower this to defend against a misbehaving "
                "client requesting huge pages. The contract spec promises "
                "1..500; raising past 500 is a contract change."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    rest_openapi_enabled: bool = Field(
        default=False,
        description=(
            "Mount OpenAPI schema + Swagger UI + ReDoc on the gateway "
            "REST surface. Default False per ADR-0026 — the gateway "
            "does not self-publish a discoverable schema in production. "
            "Operators flip this to True in dev / staging / behind a "
            "VPN to get a clickable spec at `/api/v1/docs`."
        ),
        json_schema_extra={
            "category": "rest_server",
            "impact": (
                "When True the gateway serves three new paths under "
                "the REST port: `/api/v1/openapi.json`, `/api/v1/docs` "
                "(Swagger UI), and `/api/v1/redoc`. Auth still applies "
                "to these paths — only token-bearers can read the spec. "
                "A boot-time WARNING log makes a forgotten flip "
                "obvious in any log review. The static spec at "
                "`docs/api/openapi.yaml` is regenerated by "
                "`make openapi-export` and is the canonical artifact "
                "for sharing with backend teams / Postman."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- Kafka producer (ADR-0019) --------------------------------------
    kafka_brokers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers (comma-separated host:port).",
        json_schema_extra={
            "category": "kafka_producer",
            "impact": (
                "Wrong broker → producer cannot start, gateway exits at "
                "boot. Inside the compose network use the INTERNAL listener "
                "(`kafka:29092`); from a laptop use `localhost:9092`."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    kafka_acks: Literal["all", "1", "0"] = Field(
        default="all",
        description=(
            "Producer ack mode. `all` waits for full ISR (durable to "
            "leader crash); `1` only the leader; `0` fire-and-forget."
        ),
        json_schema_extra={
            "category": "kafka_producer",
            "impact": (
                "Lowering trades durability for latency. `tx.started` is on "
                "the financial path — never lower in production. ADR-0019."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    kafka_enable_idempotence: bool = Field(
        default=True,
        description="aiokafka producer-side dedup on retry.",
        json_schema_extra={
            "category": "kafka_producer",
            "impact": (
                "Disabling lets a retried-after-lost-ack request duplicate. "
                "Pairs with E2-11's inbound replay dedup; both layers exist "
                "for defence in depth."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    kafka_linger_ms: int = Field(
        default=5,
        ge=0,
        le=1000,
        description="How long the producer waits to batch before sending (ms).",
        json_schema_extra={
            "category": "kafka_producer",
            "impact": (
                "Lower → tighter `cp.meter` end-to-end latency, smaller "
                "batches, higher per-message overhead. Higher → bigger "
                "batches, more delay before billing-relevant `tx.started` "
                "lands. ADR-0019 § 'Per-topic linger'."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    kafka_request_timeout_ms: int = Field(
        default=30_000,
        ge=1_000,
        le=120_000,
        description="How long a single produce request waits for the broker (ms).",
        json_schema_extra={
            "category": "kafka_producer",
            "impact": (
                "Tighter than aiokafka's 40 s default so a stuck broker "
                "trips the handler's publish-failed log path quickly. "
                "Raising hides broker-stall incidents from observability."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    kafka_retry_backoff_ms: int = Field(
        default=200,
        ge=10,
        le=10_000,
        description="Wait between aiokafka retries on a recoverable error (ms).",
        json_schema_extra={
            "category": "kafka_producer",
            "impact": (
                "Lower → faster recovery from transient broker blips, more "
                "load on a struggling broker. Higher → opposite."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- Kafka topics (frozen v1 contract; ADR-0018) --------------------
    # The four topic names are part of the frozen v1 contract with downstream
    # consumers (proto/events/v1/events.proto). Renaming is an externally
    # visible breaking change — treat as structural.
    kafka_topic_cp_connected: str = Field(
        default="cp.connected",
        description=(
            "WS-connect events. Source for the `cp.online` webhook. "
            "Published by the WS server immediately after the registry "
            "marks the charger online."
        ),
        json_schema_extra={
            "category": "kafka_topics",
            "impact": "Renaming detaches every existing consumer.",
            "secret": False,
            "stability": "structural",
        },
    )
    kafka_topic_cp_disconnected: str = Field(
        default="cp.disconnected",
        description=(
            "WS-disconnect events. Source for the `cp.offline` webhook. "
            "Published by the WS server only when the registry's "
            "compare-and-delete confirms we still owned the key (so a "
            "reconnect-to-different-pod race never produces a spurious "
            "offline event)."
        ),
        json_schema_extra={
            "category": "kafka_topics",
            "impact": "Renaming detaches every existing consumer.",
            "secret": False,
            "stability": "structural",
        },
    )
    kafka_topic_cp_offline_duration: str = Field(
        default="cp.offline_duration",
        description=(
            "Per-CP offline-duration events. Emitted on reconnect when the "
            "gateway finds the marker its prior disconnect left in Redis; "
            "carries went_offline_at, came_online_at and offline_seconds. "
            "ClickHouse `cp_offline_duration` is ingested from this topic."
        ),
        json_schema_extra={
            "category": "kafka_topics",
            "impact": "Renaming detaches every existing consumer.",
            "secret": False,
            "stability": "structural",
        },
    )
    kafka_topic_cp_meter: str = Field(
        default="cp.meter",
        description=(
            "Firehose topic for `MeterValues`. ClickHouse ingestor consumes from here (E2-14)."
        ),
        json_schema_extra={
            "category": "kafka_topics",
            "impact": (
                "Renaming detaches every existing consumer (ClickHouse ingestor, billing pipeline)."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    kafka_topic_cp_boot: str = Field(
        default="cp.boot",
        description="`BootNotification` events.",
        json_schema_extra={
            "category": "kafka_topics",
            "impact": "Renaming detaches every existing consumer.",
            "secret": False,
            "stability": "structural",
        },
    )
    kafka_topic_cp_status: str = Field(
        default="cp.status",
        description="`StatusNotification` events.",
        json_schema_extra={
            "category": "kafka_topics",
            "impact": "Renaming detaches every existing consumer.",
            "secret": False,
            "stability": "structural",
        },
    )
    kafka_topic_cp_firmware_status: str = Field(
        default="cp.firmware_status",
        description=(
            "`FirmwareStatusNotification` events. Source for the "
            "`cp.firmware_status_changed` webhook. Low volume (a few "
            "per charger per firmware-update lifecycle)."
        ),
        json_schema_extra={
            "category": "kafka_topics",
            "impact": "Renaming detaches every existing consumer.",
            "secret": False,
            "stability": "structural",
        },
    )
    kafka_topic_cp_diagnostics_status: str = Field(
        default="cp.diagnostics_status",
        description=(
            "`DiagnosticsStatusNotification` events. Source for the "
            "`cp.diagnostics_status_changed` webhook. Low volume "
            "(a few per charger per diagnostics-upload lifecycle)."
        ),
        json_schema_extra={
            "category": "kafka_topics",
            "impact": "Renaming detaches every existing consumer.",
            "secret": False,
            "stability": "structural",
        },
    )
    kafka_topic_tx_started: str = Field(
        default="tx.started",
        description="`StartTransaction` events (financial path).",
        json_schema_extra={
            "category": "kafka_topics",
            "impact": "Renaming detaches every existing consumer.",
            "secret": False,
            "stability": "structural",
        },
    )
    kafka_topic_tx_stopped: str = Field(
        default="tx.stopped",
        description=(
            "`StopTransaction` events emitted after a successful DB "
            "commit. Belt-and-braces signal alongside the synchronous "
            "`/sessions/close` REST call — see "
            "`docs/integration/03-webhooks.md`."
        ),
        json_schema_extra={
            "category": "kafka_topics",
            "impact": "Renaming detaches every existing consumer.",
            "secret": False,
            "stability": "structural",
        },
    )
    kafka_topic_cp_security_event: str = Field(
        default="cp.security_event",
        description=(
            "`SecurityEventNotification` events from chargers "
            "(OCPP 1.6 Security Whitepaper §4). Audit-grade; "
            "downstream SIEM consumers tail this for alerting on "
            "invalid signatures, cert tampering, etc."
        ),
        json_schema_extra={
            "category": "kafka_topics",
            "impact": "Renaming detaches every existing consumer.",
            "secret": False,
            "stability": "structural",
        },
    )
    kafka_topic_cp_credential_rotated: str = Field(
        default="cp.credential_rotated",
        description=(
            "Per-charger Basic Auth credential rotations (TC_073). "
            "Emitted when an operator sets, rotates, or removes a "
            "charger's password via the REST surface. Audit-grade; "
            "SIEM consumers tail this alongside `cp.security_event`. "
            "The password is never carried in the payload."
        ),
        json_schema_extra={
            "category": "kafka_topics",
            "impact": "Renaming detaches every existing consumer.",
            "secret": False,
            "stability": "structural",
        },
    )
    kafka_topic_cp_csr_submitted: str = Field(
        default="cp.csr_submitted",
        description=(
            "`SignCertificate` CSRs from chargers (OCPP 1.6 Security "
            "Whitepaper §4.13). Operator review hook — the gateway "
            "persists each CSR to `pending_certificate_signings` and "
            "publishes here so external systems can observe pending "
            "work. The actual signing pipeline is a separate concern."
        ),
        json_schema_extra={
            "category": "kafka_topics",
            "impact": "Renaming detaches every existing consumer.",
            "secret": False,
            "stability": "structural",
        },
    )

    # ---- Redis (online registry + pub/sub bus, ADR-0016) ----------------
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description=(
            "Single Redis client shared by registry, command bus, "
            "idempotency cache, Authorize cache."
        ),
        json_schema_extra={
            "category": "redis",
            "impact": ("Wrong DSN → gateway exits at boot. Compose uses `redis://redis:6379/0`."),
            "secret": False,
            "stability": "structural",
        },
    )
    redis_online_ttl_seconds: int = Field(
        default=120,
        ge=30,
        le=600,
        description=(
            "TTL on `cp:online:{cp_id}` keys. Heartbeat refreshes the key; "
            "if the charger goes silent the key expires and the charger "
            "is considered offline."
        ),
        json_schema_extra={
            "category": "redis",
            "impact": (
                "120 s aligns with OCPP 1.6 default heartbeat 60 s — gives "
                "~2 missed heartbeats before declaring offline. Lower → "
                "quicker offline detection but more false positives on "
                "flaky links."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- Identity (Kubernetes downward-API) -----------------------------
    pod_id: str = Field(
        default_factory=lambda: socket.gethostname(),
        description=(
            "Identity of this pod for cross-pod routing. The Redis registry "
            "records 'charger X is held by pod Y'."
        ),
        json_schema_extra={
            "category": "identity",
            "impact": (
                "In Kubernetes set this from the downward API: "
                "`valueFrom: { fieldRef: { fieldPath: metadata.name } }`. "
                "Two pods with the same `pod_id` will fight over charger "
                "ownership."
            ),
            "secret": False,
            "stability": "structural",
        },
    )

    # ---- Postgres -------------------------------------------------------
    db_url: SecretStr = Field(
        default=SecretStr("postgresql+asyncpg://eveys:eveys@localhost:5432/eveys_ocpp"),
        description=(
            "SQLAlchemy async DSN for the gateway's relational state "
            "(charge points, transactions, reservations, profiles). "
            "Stored as `SecretStr` (E5-7) — the embedded password "
            "never appears in `repr(settings)` / log dumps."
        ),
        json_schema_extra={
            "category": "postgres",
            "impact": (
                "Wrong DSN → gateway exits at boot. Schema changes go "
                "through Alembic — never edit the DB directly. The default "
                "carries the dev password; production DSNs always carry a "
                "real password and must be handled as a secret."
            ),
            "secret": True,
            "stability": "structural",
        },
    )
    db_pool_size: int = Field(
        default=10,
        ge=1,
        le=100,
        description="SQLAlchemy connection-pool size per gateway pod.",
        json_schema_extra={
            "category": "postgres",
            "impact": (
                "Higher → more concurrent DB load capacity per pod, more "
                "idle connections. Total DB connections = "
                "`pool_size + max_overflow` x number of pods."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    db_max_overflow: int = Field(
        default=20,
        ge=0,
        le=200,
        description="Extra connections allowed beyond pool size during bursts.",
        json_schema_extra={
            "category": "postgres",
            "impact": (
                "Set together with `DB_POOL_SIZE`. Postgres' "
                "`max_connections` ceiling is the hard limit."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- Logging --------------------------------------------------------
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Minimum log level emitted.",
        json_schema_extra={
            "category": "logging",
            "impact": (
                "`DEBUG` produces several per-message lines per charger — "
                "high volume on a real fleet; use only briefly to "
                "investigate an incident."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    log_json: bool = Field(
        default=True,
        description="Emit JSON logs (machine-readable) vs console (developer-readable).",
        json_schema_extra={
            "category": "logging",
            "impact": (
                "Production sets `true` so the log aggregator parses fields. "
                "Local dev sets `false` for readability."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- OCPP defaults --------------------------------------------------
    heartbeat_interval_seconds: int = Field(
        default=300,
        ge=30,
        le=86400,
        description=("Sent back in `BootNotification.interval`; the charger pings us this often."),
        json_schema_extra={
            "category": "ocpp_defaults",
            "impact": (
                "Lower → quicker offline detection at the cost of fleet-"
                "wide heartbeat traffic. Coordinate with "
                "`REDIS_ONLINE_TTL_SECONDS` (rule of thumb: TTL ~= 2x "
                "heartbeat). 300 s is the OCPP-recommended default."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- Cross-pod command bus (ADR-0016) -------------------------------
    bus_request_timeout_seconds: int = Field(
        default=30,
        ge=1,
        le=120,
        description="How long a requesting pod waits for a cross-pod reply.",
        json_schema_extra={
            "category": "cross_pod_bus",
            "impact": (
                "Defaults to the 30 s OCPP request ceiling — the bus "
                "shouldn't add headroom over the underlying call. Raising "
                "risks letting an OCPP RPC outlive the charger's own "
                "timeout."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- Idempotency cache (E2-11) --------------------------------------
    idempotency_ttl_seconds: int = Field(
        default=300,
        ge=30,
        le=3600,
        description="Window for treating a repeat `(cp_id, message_id)` as a replay.",
        json_schema_extra={
            "category": "idempotency",
            "impact": (
                "OCPP retry storms resolve within seconds; 5 min gives "
                "ample margin. Longer windows accumulate keys without "
                "benefit; OCPP message_ids are UUIDs and never reused "
                "across power cycles."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- ClickHouse ingestion sidecar (ADR-0020) ------------------------
    # The ingestor is a separate process (`python -m
    # eveys_ocpp.clickhouse.ingestor`); these settings configure it but
    # the gateway itself does not connect to ClickHouse.
    clickhouse_host: str = Field(
        default="localhost",
        description="Where the ingestor + migrator find ClickHouse.",
        json_schema_extra={
            "category": "clickhouse_ingest",
            "impact": "Compose uses `clickhouse`.",
            "secret": False,
            "stability": "structural",
        },
    )
    clickhouse_port: int = Field(
        default=9000,
        ge=1,
        le=65535,
        description=(
            "Native protocol port (8123 is HTTP, 9000 is native). The "
            "ingestor uses native; the migrator uses HTTP."
        ),
        json_schema_extra={
            "category": "clickhouse_ingest",
            "impact": (
                "If you change this you must also update the migrator's "
                "`--port` if it differs from 8123."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    clickhouse_db: str = Field(
        default="eveys_ocpp",
        description="ClickHouse database name.",
        json_schema_extra={
            "category": "clickhouse_ingest",
            "impact": ("Schema migrations target this DB; the migrator creates it on first run."),
            "secret": False,
            "stability": "structural",
        },
    )
    clickhouse_ingestor_group: str = Field(
        default="eveys-ocpp-clickhouse-ingestor",
        description=(
            "Kafka consumer-group ID for the ingestor. Multiple replicas "
            "share this group; Kafka rebalances partitions across them."
        ),
        json_schema_extra={
            "category": "clickhouse_ingest",
            "impact": (
                "Renaming forces all consumers to re-read from the "
                "configured offset (typically earliest)."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    clickhouse_ingestor_batch_size: int = Field(
        default=500,
        ge=1,
        le=10_000,
        description="Flush threshold in rows.",
        json_schema_extra={
            "category": "clickhouse_ingest",
            "impact": (
                "Lower → smaller batches, more INSERT round-trips, lower "
                "tail latency. Higher → opposite. ADR-0020 § 'Batch size "
                "vs latency'."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    clickhouse_ingestor_batch_max_seconds: float = Field(
        default=5.0,
        ge=0.1,
        le=60.0,
        description="Flush threshold in seconds (whichever-comes-first with `BATCH_SIZE`).",
        json_schema_extra={
            "category": "clickhouse_ingest",
            "impact": (
                "Lower → less worst-case ingestion delay; ClickHouse "
                "handles many small batches less efficiently than a few "
                "large ones."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    clickhouse_ingestor_max_flush_failures: int = Field(
        default=10,
        ge=1,
        le=10_000,
        description=(
            "Consecutive INSERT failures before the ingestor exits "
            "non-zero so the supervisor (docker compose, kubernetes) "
            "restarts it. Without this the process loops forever on a "
            "wedged pipeline (wrong CH instance, missing schema, "
            "type mismatch) and silently drops fresh events while the "
            "Kafka consumer-group offset never advances."
        ),
        json_schema_extra={
            "category": "clickhouse_ingest",
            "impact": (
                "Lower → faster CrashLoopBackOff signal at the cost of "
                "tolerating fewer transient blips. Higher → more "
                "patience for a flaky CH at the cost of a longer dead "
                "window before the operator finds out."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- Backend integration (ADR-0023, E3-2..E3-6) ---------------------
    backend_base_url: str = Field(
        default="",
        description=(
            "Base URL the gateway calls into. **Empty disables the backend "
            "client entirely** — handlers fall back to their offline "
            "policies (Authorize → Accepted, etc.)."
        ),
        json_schema_extra={
            "category": "backend_integration",
            "impact": (
                "Empty in dev. Production must set this; an empty value "
                "silently degrades to offline mode."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    backend_token: SecretStr = Field(
        default=SecretStr(""),
        description=(
            "Token used in `Authorization: Bearer ...` against the backend. "
            "Stored as `SecretStr` (E5-7) — call `.get_secret_value()` at "
            "the HTTP client boundary; never log."
        ),
        json_schema_extra={
            "category": "backend_integration",
            "impact": (
                "Vault provisioning lands with the Helm chart in E5-1; "
                "until then the operator handles `EVEYS_OCPP_BACKEND_TOKEN` "
                "via the platform's existing secret-management story (k8s "
                "Secret, AWS Secrets Manager, etc.)."
            ),
            "secret": True,
            "stability": "tunable",
        },
    )
    outbound_tls_verify: bool = Field(
        default=True,
        description=(
            "Whether to verify TLS certificates on every outbound "
            "connection the gateway makes — both the backend HTTP "
            "client (Authorize / sessions/open / sessions/close / "
            "charge-points/register) and the webhook dispatcher. "
            "Default True for production. Local dev with a self-signed "
            "cert (e.g. https://toger.test) sets this to False so the "
            "gateway doesn't slam the circuit breaker on every "
            "Authorize and the webhook delivery doesn't fail every "
            "attempt. Setting False in production silently disables a "
            "real security control — boot logs a loud warning to make "
            "that obvious in case it ever ships by accident."
        ),
        json_schema_extra={
            "category": "backend_integration",
            "impact": (
                "False allows MITM against the backend AND webhook "
                "legs. Acceptable for local dev; never in production. "
                "Phase 5 vault work (E5-7) will swap this for proper "
                "CA-bundle config per leg."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    backend_timeout_authorize_seconds: float = Field(
        default=5.0,
        ge=0.1,
        le=30.0,
        description="HTTP timeout for the Authorize call (seconds).",
        json_schema_extra={
            "category": "backend_integration",
            "impact": (
                "Tighter values trip the gateway's offline fallback faster. "
                "The 30 s OCPP outer timeout is the hard ceiling."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    backend_timeout_sessions_open_seconds: float = Field(
        default=8.0,
        ge=0.1,
        le=30.0,
        description="HTTP timeout for `POST /api/eveys/sessions/open` (StartTransaction).",
        json_schema_extra={
            "category": "backend_integration",
            "impact": "Same shape as Authorize.",
            "secret": False,
            "stability": "tunable",
        },
    )
    backend_timeout_sessions_close_seconds: float = Field(
        default=10.0,
        ge=0.1,
        le=30.0,
        description="HTTP timeout for `POST /api/eveys/sessions/close` (StopTransaction).",
        json_schema_extra={
            "category": "backend_integration",
            "impact": (
                "StopTransaction tolerates a longer wait — closing a "
                "session is less time-critical than opening one."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    backend_timeout_default_seconds: float = Field(
        default=5.0,
        ge=0.1,
        le=30.0,
        description=(
            "Fallback timeout for any backend call without an explicit per-endpoint setting."
        ),
        json_schema_extra={
            "category": "backend_integration",
            "impact": ("Used by `charge-points/register` and any future endpoint."),
            "secret": False,
            "stability": "tunable",
        },
    )
    backend_retry_attempts_authorize: int = Field(
        default=1,
        ge=0,
        le=5,
        description="Retry attempts (excluding the first try) for Authorize.",
        json_schema_extra={
            "category": "backend_integration",
            "impact": (
                "Higher → resilience to transient blips, more latency on "
                "persistent outages. Authorize is on the OCPP hot path — "
                "keep low."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    backend_retry_attempts_sessions_open: int = Field(
        default=2,
        ge=0,
        le=5,
        description="Retry attempts for sessions/open.",
        json_schema_extra={
            "category": "backend_integration",
            "impact": (
                "StartTransaction is billing-critical; spending more "
                "retries here is the right trade."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    backend_retry_attempts_sessions_close: int = Field(
        default=3,
        ge=0,
        le=5,
        description="Retry attempts for sessions/close.",
        json_schema_extra={
            "category": "backend_integration",
            "impact": ("The most important: a missed Close = a session that never billed."),
            "secret": False,
            "stability": "tunable",
        },
    )
    backend_circuit_breaker_threshold: int = Field(
        default=5,
        ge=1,
        le=100,
        description="Open the breaker after this many consecutive failures.",
        json_schema_extra={
            "category": "backend_integration",
            "impact": (
                "Lower → quicker degradation to offline mode under outage, "
                "more flapping during transient incidents."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    backend_circuit_breaker_cooldown_seconds: float = Field(
        default=30.0,
        ge=1.0,
        le=600.0,
        description=(
            "How long the breaker stays open before letting one probe through (half-open)."
        ),
        json_schema_extra={
            "category": "backend_integration",
            "impact": ("Lower → faster recovery test but more load on a still-broken backend."),
            "secret": False,
            "stability": "tunable",
        },
    )
    backend_authorize_fallback: Literal["reject", "accept_offline"] = Field(
        default="reject",
        description=(
            "What the Authorize handler returns when the backend is "
            "unreachable past the retry budget. `reject` → `Invalid` "
            "(safe). `accept_offline` → `Accepted` with a 5-min expiry "
            "(operator opt-in to un-billable risk)."
        ),
        json_schema_extra={
            "category": "backend_integration",
            "impact": (
                "ADR-0023 § 'Fallback policy'. Default `reject` is the "
                "safe billing-relevant choice."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    backend_register_fallback: Literal["reject", "accept_offline"] = Field(
        default="accept_offline",
        description=(
            "What BootNotification returns when the backend's "
            "`/charge-points/register` endpoint is unreachable past the "
            "retry budget. `accept_offline` → `Accepted` with the "
            "configured heartbeat interval (the local DB row anchors "
            "reconciliation when the backend recovers). `reject` → "
            "`Rejected`, charger stops calling."
        ),
        json_schema_extra={
            "category": "backend_integration",
            "impact": (
                "Default `accept_offline` matches the contract's "
                "fail-soft model: a backend outage must not prevent "
                "chargers from booting and serving Authorize-cached "
                "sessions. Flip to `reject` only if the operator "
                "wants chargers offline during a backend incident."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- Authorize cache (E3-4) --------------------------------------------
    backend_authorize_cache_enabled: bool = Field(
        default=True,
        description=(
            "Enable Redis caching of the Authorize result keyed on "
            "`(cp_id, id_tag)`. Cache hits short-circuit the backend "
            "round-trip on the OCPP hot path."
        ),
        json_schema_extra={
            "category": "authorize_cache",
            "impact": (
                "Disabling pushes every Authorize through the backend — "
                "useful for ops debugging when a stale cached "
                "`Blocked` is suspected. Re-enable as soon as the "
                "issue is understood."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    backend_authorize_cache_ttl_seconds: int = Field(
        default=30,
        ge=1,
        le=3600,
        description="TTL on cached Authorize entries.",
        json_schema_extra={
            "category": "authorize_cache",
            "impact": (
                "Short enough that `Blocked`/`Expired` decisions "
                "propagate within ~30 s; long enough to absorb depot-"
                "shift bursts (a fleet returning at once = same-tag "
                "taps within a minute). Drop toward 1 s for ops "
                "debugging."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- E3-9: outbound webhooks --------------------------------------------
    #
    # Per docs/integration/03-webhooks.md the gateway pushes signed events
    # at backend-configured URLs. One URL per event type, each independently
    # toggleable. Empty `webhook_base_url` disables the whole subsystem
    # (sidecars and dev runs without a backend skip the dispatcher entirely).
    webhook_base_url: str = Field(
        default="",
        description=(
            "Base URL the gateway POSTs webhook deliveries to. Empty "
            "disables the dispatcher entirely. Per-event URLs default "
            "to `<base>/<event-name>` and can be overridden individually."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": (
                "Empty string = no webhook subsystem. Set to the "
                "backend's webhook receiver root URL to enable."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_secret: SecretStr = Field(
        default=SecretStr(""),
        description=(
            "Shared HMAC-SHA-256 secret used to sign every webhook "
            "delivery. The backend's receiver verifies the "
            "`X-Eveys-Signature` header against the same secret. "
            "Stored as `SecretStr` (E5-7); the dispatcher calls "
            "`.get_secret_value()` at the signing boundary."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": (
                "Holds in vault. A leak lets an attacker forge "
                "delivery requests against the backend. Rotate via "
                "coordinated update (gateway + backend in lockstep)."
            ),
            "secret": True,
            "stability": "tunable",
        },
    )

    webhook_url_cp_boot: str = Field(
        default="",
        description=(
            "Override the URL for `cp.boot` events. Empty falls back "
            "to `<webhook_base_url>/cp-boot` when the base URL is set."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": "Per-event routing override.",
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_url_cp_firmware_status: str = Field(
        default="",
        description=(
            "Override the URL for `cp.firmware_status_changed` events. "
            "Empty falls back to `<webhook_base_url>/cp-firmware-status-changed`."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": "Per-event routing override.",
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_url_cp_diagnostics_status: str = Field(
        default="",
        description=(
            "Override the URL for `cp.diagnostics_status_changed` events. "
            "Empty falls back to `<webhook_base_url>/cp-diagnostics-status-changed`."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": "Per-event routing override.",
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_url_cp_offline: str = Field(
        default="",
        description=(
            "Override the URL for `cp.offline` events. Empty falls "
            "back to `<webhook_base_url>/cp-offline`."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": "Per-event routing override.",
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_url_cp_online: str = Field(
        default="",
        description=(
            "Override the URL for `cp.online` events. Empty falls "
            "back to `<webhook_base_url>/cp-online`."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": "Per-event routing override.",
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_url_cp_status: str = Field(
        default="",
        description=(
            "Override the URL for `cp.status_changed` events. Empty "
            "falls back to `<webhook_base_url>/cp-status-changed`."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": "Per-event routing override.",
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_url_cp_meter: str = Field(
        default="",
        description=(
            "Override the URL for `cp.meter` MeterValues events. "
            "Empty falls back to `<webhook_base_url>/cp-meter`."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": (
                "MeterValues are high-volume — at 10k chargers this "
                "is ~333 webhooks/second. Off by default (see "
                "`webhook_enable_cp_meter`); prefer Kafka."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_url_tx_started: str = Field(
        default="",
        description=(
            "Override the URL for `tx.started` events. Empty falls "
            "back to `<webhook_base_url>/tx-started`."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": "Per-event routing override.",
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_url_tx_stopped: str = Field(
        default="",
        description=(
            "Override the URL for `tx.stopped` events. Empty falls "
            "back to `<webhook_base_url>/tx-stopped`."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": "Per-event routing override.",
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_enable_cp_boot: bool = Field(
        default=True,
        description="Enable webhook delivery for `cp.boot` events.",
        json_schema_extra={
            "category": "webhooks",
            "impact": "Disable to silence boot-event pushes.",
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_enable_cp_online: bool = Field(
        default=True,
        description=("Enable webhook delivery for `cp.online` events (charger WebSocket connect)."),
        json_schema_extra={
            "category": "webhooks",
            "impact": ("Pairs with `cp.offline` for backend-side online-state tracking."),
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_enable_cp_firmware_status: bool = Field(
        default=True,
        description=(
            "Enable webhook delivery for `cp.firmware_status_changed` "
            "events (charger-reported firmware-update state-machine "
            "transitions). Low volume — a few events per charger per "
            "firmware-update lifecycle."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": "Disable to silence firmware-status pushes.",
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_enable_cp_diagnostics_status: bool = Field(
        default=True,
        description=(
            "Enable webhook delivery for `cp.diagnostics_status_changed` "
            "events (charger-reported diagnostics-upload state-machine "
            "transitions). Low volume — a few events per charger per "
            "diagnostics-upload lifecycle."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": "Disable to silence diagnostics-status pushes.",
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_enable_cp_offline: bool = Field(
        default=True,
        description=(
            "Enable webhook delivery for `cp.offline` events (charger "
            "WebSocket disconnect — only fired when this pod still "
            "owned the registry key, so a reconnect-to-different-pod "
            "race never produces a spurious offline event)."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": ("Pairs with `cp.online` for backend-side online-state tracking."),
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_enable_cp_status: bool = Field(
        default=True,
        description="Enable webhook delivery for `cp.status_changed` events.",
        json_schema_extra={
            "category": "webhooks",
            "impact": "Disable to silence status-change pushes.",
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_enable_cp_meter: bool = Field(
        default=False,
        description=(
            "Enable webhook delivery for `cp.meter` MeterValues "
            "events. **Off by default** — high volume."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": (
                "Enabling this on a fleet >100 chargers will saturate "
                "the dispatcher's HTTP pool. Subscribe to Kafka instead."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_enable_tx_started: bool = Field(
        default=True,
        description="Enable webhook delivery for `tx.started` events.",
        json_schema_extra={
            "category": "webhooks",
            "impact": "Disable to silence transaction-start pushes.",
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_enable_tx_stopped: bool = Field(
        default=True,
        description=(
            "Enable webhook delivery for `tx.stopped` events. Belt-and-"
            "braces signal alongside the synchronous `/sessions/close` "
            "REST call — see `docs/integration/03-webhooks.md`."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": (
                "Disable to silence transaction-stop pushes; the "
                "synchronous `/sessions/close` is still made by the "
                "handler regardless."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_consumer_group: str = Field(
        default="eveys-ocpp-webhook-dispatcher",
        description=(
            "Kafka consumer group the webhook dispatcher uses to "
            "tail the four event topics. Distinct from the ClickHouse "
            "ingestor's group so the two pipelines run independently."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": (
                "Changing this resets webhook consumer offsets. Keep "
                "stable unless intentionally replaying from earliest."
            ),
            "secret": False,
            "stability": "structural",
        },
    )

    webhook_request_timeout_seconds: float = Field(
        default=10.0,
        ge=1.0,
        le=120.0,
        description=(
            "HTTP timeout for a single webhook delivery attempt. "
            "Backend must respond within this window or the gateway "
            "treats the attempt as a transient failure and retries."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": (
                "Lower = faster failure detection; higher = tolerates "
                "slower backends. 10 s matches the backend's documented "
                "response budget per `docs/integration/03-webhooks.md`."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    webhook_max_attempts: int = Field(
        default=5,
        ge=1,
        le=20,
        description=(
            "Total delivery attempts before the gateway gives up and "
            "logs `webhook.delivery_failed`. Includes the first attempt."
        ),
        json_schema_extra={
            "category": "webhooks",
            "impact": (
                "Retries follow exponential backoff: 1 s, 5 s, 30 s, "
                "2 min, 10 min for the default of 5. Lowering reduces "
                "the longest in-flight tail; raising tolerates longer "
                "backend outages."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- Metrics (Phase 4 / E4-1) ---------------------------------------
    metrics_enabled: bool = Field(
        default=True,
        description=(
            "Master switch for the Prometheus metrics server. True in "
            "production and local dev. Unit tests flip this to False "
            "via an autouse fixture so a `pytest -k something` invocation "
            "doesn't try to bind 9100 once per test process."
        ),
        json_schema_extra={
            "category": "metrics",
            "impact": (
                "False removes the /metrics endpoint entirely. "
                "Counters/histograms still increment in-process — they "
                "just become unscrapeable, so no operational signal."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    metrics_host: str = Field(
        default="0.0.0.0",
        description=(
            "Bind address for the Prometheus scrape server. Default "
            "`0.0.0.0` so a sidecar Prometheus scraper inside the same "
            "k8s namespace can reach it."
        ),
        json_schema_extra={
            "category": "metrics",
            "impact": (
                "Restrict to `127.0.0.1` if scraping happens inside the "
                "same pod (sidecar pattern). In production we expose to "
                "the cluster network; the port is not in the public "
                "ingress."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    metrics_port: int = Field(
        default=9100,
        ge=1,
        le=65535,
        description=(
            "Scrape port. 9100 by convention (canonically `node_exporter`'s "
            "port — we own the gateway port in our deployment). Compose "
            "publishes this and Prometheus' ServiceMonitor in k8s targets "
            "the same."
        ),
        json_schema_extra={
            "category": "metrics",
            "impact": (
                "Changing this requires updating compose `ports:`, the "
                "Helm chart's Service, and the ServiceMonitor selector. "
                "The default is fine in 99% of environments."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    metrics_path: str = Field(
        default="/metrics",
        description=(
            "Path the scrape endpoint serves on. Lets ops mount it at "
            "`/internal/metrics` behind a path-based proxy without a "
            "code change. Path is matched exactly; trailing slashes are "
            "not normalised."
        ),
        json_schema_extra={
            "category": "metrics",
            "impact": "Cosmetic + access-control via reverse-proxy rules.",
            "secret": False,
            "stability": "tunable",
        },
    )
    metrics_include_python_collectors: bool = Field(
        default=True,
        description=(
            "Whether prometheus_client's default GC / process / platform "
            "collectors stay registered. They emit ~12 series an operator "
            "rarely needs (`python_gc_objects_collected_total`, "
            "`process_resident_memory_bytes`, etc.). Set False to trim "
            "them in resource-tight environments. Most fleets keep True "
            "— they're free and they catch GC stalls."
        ),
        json_schema_extra={
            "category": "metrics",
            "impact": (
                "False removes `python_*` and `process_*` series from "
                "the scrape output. No instrumentation we own depends on "
                "them; Grafana dashboards built off our `eveys_ocpp_*` "
                "namespace are unaffected."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- Tracing (Phase 4 / E4-3) ---------------------------------------
    tracing_enabled: bool = Field(
        default=False,
        description=(
            "Master switch for OpenTelemetry tracing. Default False — "
            "tracing is opt-in because most environments don't have an "
            "OTLP collector listening, and a tracer with a misconfigured "
            "exporter quietly buffers spans until OOM. Flip to True only "
            "when `tracing_otlp_endpoint` points at a real collector."
        ),
        json_schema_extra={
            "category": "tracing",
            "impact": (
                "False keeps the global `NoOpTracerProvider` — every "
                "`tracer.start_as_current_span(...)` is a few-ns no-op. "
                "True activates the SDK; spans flow to the configured "
                "OTLP endpoint."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    tracing_otlp_endpoint: str = Field(
        default="http://localhost:4317",
        description=(
            "OTLP/gRPC endpoint for span export. Standard collector port "
            "is 4317 (gRPC) and 4318 (HTTP); we use gRPC. Honoured only "
            "when `tracing_enabled=True`. The dotted-default makes it "
            "obvious this isn't yet pointing at a real collector."
        ),
        json_schema_extra={
            "category": "tracing",
            "impact": (
                "Pointing at an unreachable endpoint silently buffers "
                "spans (the OTLP exporter retries with backoff). The SDK "
                "logs export failures at WARNING — watch for "
                "`Failed to export batch` in stderr at boot."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    tracing_sample_rate: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description=(
            "Head-based sample rate in `[0.0, 1.0]`. 1.0 traces every "
            "request — fine in dev. In production set this to "
            "0.01-0.1 unless your collector is sized for full-rate."
        ),
        json_schema_extra={
            "category": "tracing",
            "impact": (
                "Lowering this drops spans uniformly across all "
                "operations. Errors are *not* preferentially kept (no "
                "tail-based sampling at the SDK layer); use a collector-"
                "side tail sampler if you need that."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    tracing_service_name: str = Field(
        default="eveys-ocpp",
        description=(
            "`service.name` resource attribute attached to every span. "
            "Identifies this service in the trace UI; default matches "
            "the python package name. Multiple replicas of the same "
            "service share this — `service.instance.id` (auto-set from "
            "`pod_id`) discriminates between replicas."
        ),
        json_schema_extra={
            "category": "tracing",
            "impact": (
                "Changing this re-bins all spans under a new service in "
                "your trace backend; existing saved searches break. "
                "Treat as fixed once a deployment is live."
            ),
            "secret": False,
            "stability": "structural",
        },
    )

    # ---- Sentry (Phase 4 / E4-4) ----------------------------------------
    sentry_dsn: SecretStr = Field(
        default=SecretStr(""),
        description=(
            "Sentry DSN for error tracking. Empty disables the SDK "
            "entirely (no init, no transport, no monkey-patches) so "
            "the gateway behaves identically to a Sentry-free build. "
            "Set in production to capture unhandled exceptions and "
            "structured `error`-level logs. Stored as `SecretStr` "
            "(E5-7) — the public key embedded in the DSN is enough "
            "to ingest events on a project's behalf, so treat it as "
            "secret regardless of Sentry's own threat model."
        ),
        json_schema_extra={
            "category": "sentry",
            "impact": (
                "Empty → Sentry is a hard no-op. Non-empty → SDK boots "
                "at startup; any later DSN typo surfaces as a "
                "`Bad DSN` log line on stderr (the SDK refuses to "
                "init silently)."
            ),
            "secret": True,
            "stability": "structural",
        },
    )
    sentry_environment: str = Field(
        default="development",
        description=(
            "`environment` tag attached to every Sentry event. "
            "Conventionally `production`, `staging`, `development`. "
            "Sentry alert rules and saved searches typically pivot "
            "on this — keep it stable per deployment."
        ),
        json_schema_extra={
            "category": "sentry",
            "impact": (
                "Changing splits the Sentry issue stream — the same "
                "exception on the same release line appears as two "
                "issues if `environment` differs."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    sentry_release: str = Field(
        default="",
        description=(
            "`release` tag attached to every Sentry event. Empty "
            "lets the gateway default to the package `__version__` "
            "at boot. Override only when CI injects a richer label "
            "(commit SHA, deploy id)."
        ),
        json_schema_extra={
            "category": "sentry",
            "impact": (
                "Drives Sentry's regression detection — an issue marked "
                "resolved in release X reopens automatically when "
                "release Y emits the same fingerprint."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    sentry_traces_sample_rate: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description=(
            "Sentry performance / tracing sample rate. Default 0.0 — "
            "OTel (E4-3) owns tracing; Sentry's job here is errors only. "
            "Setting > 0 doubles the tracing instrumentation cost and "
            "fragments traces across two backends; only flip if you "
            "specifically want Sentry's `Performance` view."
        ),
        json_schema_extra={
            "category": "sentry",
            "impact": (
                "Above 0 → Sentry SDK monkey-patches httpx / fastapi / "
                "asyncio to record spans. May overlap with OTel's "
                "instrumentation depending on import order."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    sentry_profiles_sample_rate: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description=(
            "Sentry profiling sample rate. Default 0.0 (off). Profiling "
            "samples Python frames at ~100 Hz per traced transaction; "
            "ignored unless `sentry_traces_sample_rate > 0` since "
            "profiling attaches to traces."
        ),
        json_schema_extra={
            "category": "sentry",
            "impact": (
                "Above 0 → SIGPROF-driven sampler runs; ~3-5% steady-"
                "state CPU overhead on traced requests. Only useful "
                "when chasing per-frame latency."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- Graceful shutdown ----------------------------------------------
    shutdown_drain_enabled: bool = Field(
        default=True,
        description=(
            "When True, SIGTERM/SIGINT trigger a drain phase before the "
            "TaskGroup is cancelled: `/api/v1/ready` flips to 503, the "
            "load balancer's readiness probe fails, and new WS upgrades "
            "stop being routed here. When False, signals cancel the "
            "TaskGroup immediately (legacy behaviour)."
        ),
        json_schema_extra={
            "category": "shutdown",
            "impact": (
                "Disable only as an emergency kill-switch. Without drain, "
                "rolling deploys cause brief connection-refused windows "
                "until the LB notices the pod is gone."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    shutdown_readiness_propagation_seconds: float = Field(
        default=10.0,
        ge=0.0,
        le=120.0,
        description=(
            "Wall time the gateway holds between flipping `/ready` to 503 "
            "and beginning real teardown. Must be >= the load balancer's "
            "readiness probe interval x failure threshold so the LB has "
            "time to remove this pod from rotation before connections "
            "actually drop."
        ),
        json_schema_extra={
            "category": "shutdown",
            "impact": (
                "Too low → LB still sends new connections to a draining "
                "pod (chargers see refusals). Too high → slow rolling "
                "deploys. 10 s suits a 3 s/2-failure k8s probe."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    shutdown_grace_period_seconds: float = Field(
        default=25.0,
        ge=1.0,
        le=300.0,
        description=(
            "Hard upper bound on the whole drain → teardown sequence. "
            "After this, the TaskGroup is cancelled even if drain hasn't "
            "fully completed. Set the k8s `terminationGracePeriodSeconds` "
            "to this value plus a small buffer (e.g. +5 s) so kubelet's "
            "SIGKILL doesn't beat the gateway's own clean exit."
        ),
        json_schema_extra={
            "category": "shutdown",
            "impact": (
                "Bounds worst-case shutdown latency. Must exceed "
                "`shutdown_readiness_propagation_seconds` with margin for "
                "TaskGroup teardown (bus stop, redis aclose, span flush)."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- Per-charger rate limit (E5-3) ----------------------------------
    ws_rate_limit_enabled: bool = Field(
        default=True,
        description=(
            "Per-charger inbound-CALL rate limiter (E5-3). When True, each "
            "charger's CALLs are checked against a Redis-backed token "
            "bucket; overrun drops the message silently and bumps "
            "`eveys_ocpp_rate_limit_throttled_total{action}`. Kill-switch "
            "for emergencies; on a Redis blip the limiter already fails "
            "open, so flipping this to False should rarely be needed."
        ),
        json_schema_extra={
            "category": "ws_server",
            "impact": (
                "Disabling removes the per-charger DoS protection — a "
                "single misbehaving charger can saturate handler / "
                "Postgres / Kafka work for the whole pod."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    ws_rate_limit_capacity: int = Field(
        default=30,
        ge=1,
        le=10_000,
        description=(
            "Token-bucket capacity per charger — the burst allowance. "
            "First N CALLs in a quiet period all pass through; refill "
            "tops the bucket up over time. Default 30 absorbs reconnect "
            "bursts (BootNotification + StatusNotifications) without "
            "throttling normal chargers."
        ),
        json_schema_extra={
            "category": "ws_server",
            "impact": (
                "Lower → tighter burst tolerance, more throttles on "
                "reconnect storms. Higher → larger spike a single "
                "charger can land on us before the cap kicks in."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )
    ws_rate_limit_refill_per_second: float = Field(
        default=1.0,
        gt=0.0,
        le=1_000.0,
        description=(
            "Token-bucket refill rate per charger (tokens per second). "
            "Default 1.0 = sustained 60 CALLs/min, well above any normal "
            "OCPP 1.6 charger's steady-state traffic. Pair with the "
            "capacity field: bucket caps at `ws_rate_limit_capacity`."
        ),
        json_schema_extra={
            "category": "ws_server",
            "impact": (
                "Lower → tighter steady-state cap. Higher → looser "
                "(closer to no-limit). Don't set above ~10 unless a "
                "specific vendor's traffic profile is documented to "
                "exceed 600 CALLs/min."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )

    # ---- mTLS for the Envoy → gateway leg (E5-5) ------------------------
    ws_mtls_enabled: bool = Field(
        default=False,
        description=(
            "When True, the WS server requires client TLS authentication "
            "(`ssl.CERT_REQUIRED`) on inbound connections. The peer must "
            "present a certificate signed by the CA at `ws_mtls_ca_path`. "
            "Used in production to authenticate the Envoy → gateway leg "
            "(E5-5, ADR-0011); off in compose dev because charger sims "
            "don't carry certs."
        ),
        json_schema_extra={
            "category": "auth",
            "impact": (
                "Enabling without setting cert / key / ca paths fails "
                "loud at boot. Disabling in production drops the in-"
                "cluster authentication boundary — the WS server then "
                "trusts whatever can reach `:9000`."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    ws_mtls_cert_path: str = Field(
        default="",
        description=(
            "Filesystem path to the gateway's server certificate (PEM). "
            "Loaded into the WS server's `SSLContext` when "
            "`ws_mtls_enabled=True`. In k8s the operator mounts this "
            "from a TLS Secret via the Helm chart."
        ),
        json_schema_extra={
            "category": "auth",
            "impact": (
                "Wrong path → boot fails with `FileNotFoundError`. The "
                "cert is the gateway's own identity to Envoy."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    ws_mtls_key_path: str = Field(
        default="",
        description=(
            "Filesystem path to the gateway's server private key (PEM). "
            "Loaded with `ws_mtls_cert_path` into the `SSLContext`."
        ),
        json_schema_extra={
            "category": "auth",
            "impact": (
                "Path leak isn't a secret leak — the file at the path "
                "is. File permissions on the mount are the operator's "
                "concern; the gateway just `open()`s it once at boot."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    ws_mtls_ca_path: str = Field(
        default="",
        description=(
            "Filesystem path to the CA bundle (PEM) used to verify the "
            "client cert Envoy presents on each upstream connection. "
            "Anything signed by this CA is trusted; rotate the bundle "
            "to revoke."
        ),
        json_schema_extra={
            "category": "auth",
            "impact": (
                "Trust anchor for the Envoy-side identity. A widened "
                "CA (e.g. a public root) effectively disables the "
                "auth boundary. Mount it as a tightly-scoped private "
                "CA, not a public one."
            ),
            "secret": False,
            "stability": "structural",
        },
    )
    ws_basic_auth_required: bool = Field(
        default=False,
        description=(
            "WS-edge Basic Auth (E5-6) is always *attempted* — every "
            "upgrade is checked against `charge_point_credentials`. "
            "This flag controls behaviour for chargers that have **no "
            "credential row** yet: when False (default) those chargers "
            "are accepted, which lets a fleet migrate gradually; when "
            "True the upgrade is rejected with 401 and the operator "
            "must provision a credential before the charger connects."
        ),
        json_schema_extra={
            "category": "auth",
            "impact": (
                "Production sets True so an unprovisioned charger can't "
                "sneak through. Dev / compose stays False so the "
                "simulator (which doesn't carry creds) keeps working."
            ),
            "secret": False,
            "stability": "tunable",
        },
    )


def get_settings() -> Settings:
    """Build a fresh `Settings` from the current environment.

    No caching — call sites that need a stable reference should hold the
    returned instance themselves. This makes per-test overrides via
    `monkeypatch.setenv` reliable.
    """
    return Settings()
