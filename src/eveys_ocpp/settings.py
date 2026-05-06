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

from pydantic import Field
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
    db_url: str = Field(
        default="postgresql+asyncpg://eveys:eveys@localhost:5432/eveys_ocpp",
        description=(
            "SQLAlchemy async DSN for the gateway's relational state "
            "(charge points, transactions, reservations, profiles)."
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
    backend_token: str = Field(
        default="",
        description="Token used in `Authorization: Bearer ...` against the backend.",
        json_schema_extra={
            "category": "backend_integration",
            "impact": (
                "Move to vault in Phase 5 (E5-7). Until then handle as a "
                "secret and never commit a real value to .env or values.yaml."
            ),
            "secret": True,
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


def get_settings() -> Settings:
    """Build a fresh `Settings` from the current environment.

    No caching — call sites that need a stable reference should hold the
    returned instance themselves. This makes per-test overrides via
    `monkeypatch.setenv` reliable.
    """
    return Settings()
