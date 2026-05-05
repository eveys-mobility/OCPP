"""Environment-driven configuration for eveys/ocpp.

All runtime configuration goes through this module. Direct `os.environ` reads
elsewhere are forbidden (see `03-coding-standards.md`).
"""

from __future__ import annotations

import socket

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
    ws_host: str = Field(default="0.0.0.0", description="WebSocket bind address")
    ws_port: int = Field(default=9000, description="WebSocket bind port")

    # ---- gRPC server ----------------------------------------------------
    grpc_host: str = Field(default="0.0.0.0", description="gRPC bind address")
    grpc_port: int = Field(default=50051, description="gRPC bind port")

    # ---- Kafka event firehose (E2-7, E2-8) ------------------------------
    # Comma-separated list (matches aiokafka's bootstrap_servers param).
    kafka_brokers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers (comma-separated host:port)",
    )
    # Topic for the MeterValues firehose. Per AGENTS rule 4 + ADR-0004,
    # MeterValues never go to Postgres — Kafka is the only persistence
    # path; ClickHouse consumes from this topic (E2-14).
    kafka_topic_cp_meter: str = Field(default="cp.meter")
    # Topics wired in E2-8. Names match the frozen proto contract
    # (proto/events/v1/events.proto, E2-3) so consumers can subscribe
    # without reading our settings.
    kafka_topic_cp_boot: str = Field(default="cp.boot")
    kafka_topic_cp_status: str = Field(default="cp.status")
    kafka_topic_tx_started: str = Field(default="tx.started")

    # ---- Kafka producer durability / latency knobs (E2-7) ---------------
    # See ADR-0019 for the trade-off discussion. Defaults pick durability
    # over throughput because `tx.started` is on the financial path —
    # losing one event = losing one billable session record.
    kafka_acks: str = Field(
        default="all",
        description=(
            "Kafka producer ack mode: 'all' (full ISR ack, durable to "
            "leader crash), '1' (leader-only), '0' (fire-and-forget). "
            "Default 'all' for durability — see ADR-0019."
        ),
    )
    kafka_enable_idempotence: bool = Field(
        default=True,
        description=(
            "aiokafka producer-side dedup on retry. Eliminates duplicate "
            "events when the producer retries a request whose ack was "
            "lost. Pairs with E2-11's inbound-replay dedup."
        ),
    )
    # Producer-wide linger. aiokafka does not support per-call linger
    # override, so this is a single compromise value across all four
    # topics. 5 ms gives `cp.meter` enough batching headroom at fleet
    # scale without putting a real latency floor on the low-volume
    # billing-relevant topics. ADR-0019 § "Per-topic linger".
    kafka_linger_ms: int = Field(default=5, ge=0, le=1000)
    kafka_request_timeout_ms: int = Field(
        default=30_000,
        ge=1_000,
        le=120_000,
        description=(
            "How long a single produce request waits for the broker. "
            "Tighter than aiokafka's 40s default so a stuck broker "
            "trips the handler's publish-failed log path quickly."
        ),
    )
    kafka_retry_backoff_ms: int = Field(
        default=200,
        ge=10,
        le=10_000,
        description="Wait between aiokafka retries on a recoverable error.",
    )

    # ---- Redis online registry (E2-9) -----------------------------------
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis DSN for the online-charger registry + pub/sub bus",
    )
    redis_online_ttl_seconds: int = Field(
        default=120,
        ge=30,
        le=600,
        description=(
            "TTL on `cp:online:{cp_id}` keys. Heartbeat refreshes the key; "
            "if the charger goes silent the key expires and the charger is "
            "considered offline. 120s aligns with OCPP 1.6 default heartbeat "
            "of 60s — gives ~2 missed heartbeats before declaring offline."
        ),
    )

    # ---- Identity -------------------------------------------------------
    # Used by the Redis registry to record which pod holds a charger's
    # WebSocket. In Kubernetes set this from the downward API:
    #     env:
    #       - name: EVEYS_OCPP_POD_ID
    #         valueFrom:
    #           fieldRef:
    #             fieldPath: metadata.name
    pod_id: str = Field(
        default_factory=lambda: socket.gethostname(),
        description="Identity of this pod for cross-pod routing",
    )

    # ---- Postgres -------------------------------------------------------
    db_url: str = Field(
        default="postgresql+asyncpg://eveys:eveys@localhost:5432/eveys_ocpp",
        description="SQLAlchemy async DSN",
    )
    db_pool_size: int = Field(default=10, ge=1, le=100)
    db_max_overflow: int = Field(default=20, ge=0, le=200)

    # ---- Logging --------------------------------------------------------
    log_level: str = Field(default="INFO")
    log_json: bool = Field(default=True, description="Emit JSON logs (False = console)")

    # ---- Heartbeat / OCPP defaults --------------------------------------
    # Sent back in BootNotification.interval. Charger pings us this often.
    # 300s is the OCPP-recommended default; tighter intervals scale poorly.
    heartbeat_interval_seconds: int = Field(default=300, ge=30, le=86400)

    # ---- Cross-pod command bus (E2-10) ----------------------------------
    # Cap on how long the requesting pod waits for a cross-pod reply.
    # Defaults to the 30s OCPP request ceiling — the bus shouldn't add
    # headroom over the underlying call. Pubs/subs share the same Redis
    # as the registry (see `redis_url`).
    bus_request_timeout_seconds: int = Field(default=30, ge=1, le=120)

    # ---- Idempotency cache (E2-11) --------------------------------------
    # Window for treating a repeat (cp_id, message_id) as a replay.
    # Real OCPP retry storms resolve within seconds; 5 minutes gives
    # ample margin. Longer windows accumulate keys without benefit;
    # OCPP message_ids are UUIDs and never reused across power cycles.
    idempotency_ttl_seconds: int = Field(default=300, ge=30, le=3600)

    # ---- ClickHouse ingestion sidecar (E2-13, E2-14, ADR-0020) ----------
    # The ingestor is a separate process (`python -m
    # eveys_ocpp.clickhouse.ingestor`); these settings configure it
    # but the gateway itself does not connect to ClickHouse.
    clickhouse_host: str = Field(default="localhost")
    # Native ClickHouse protocol port (8123 is HTTP, 9000 is native).
    # asynch uses native; the migrator (`migrate.py`) uses HTTP.
    clickhouse_port: int = Field(default=9000, ge=1, le=65535)
    clickhouse_db: str = Field(default="eveys_ocpp")
    # Kafka consumer group ID for the ingestor. Multiple replicas of
    # the ingestor share this group ID and Kafka rebalances partitions
    # across them; today we run one but the design doesn't preclude
    # scaling out.
    clickhouse_ingestor_group: str = Field(default="eveys-ocpp-clickhouse-ingestor")
    # Batch knobs (ADR-0020 § "Batch size vs latency"). 500 rows or
    # 5 seconds, whichever first. Lower the seconds threshold to cut
    # tail latency at the cost of smaller batches.
    clickhouse_ingestor_batch_size: int = Field(default=500, ge=1, le=10_000)
    clickhouse_ingestor_batch_max_seconds: float = Field(default=5.0, ge=0.1, le=60.0)

    # ---- Backend integration (E3-2, ADR-0023) ------------------------------
    # Base URL the gateway calls into. Empty string disables the
    # backend client — handlers fall back to their offline policies.
    backend_base_url: str = Field(default="")
    # Bearer token. Stored as plain str for now; Phase 5 vault work
    # (E5-7) will move it to a SecretStr fetched at boot.
    backend_token: str = Field(default="")
    # Per-endpoint timeouts (seconds). Tuned to fit inside the 30 s
    # OCPP outer timeout the charger imposes (ADR-0023).
    backend_timeout_authorize_seconds: float = Field(default=5.0, ge=0.1, le=30.0)
    backend_timeout_sessions_open_seconds: float = Field(default=8.0, ge=0.1, le=30.0)
    backend_timeout_sessions_close_seconds: float = Field(default=10.0, ge=0.1, le=30.0)
    backend_timeout_default_seconds: float = Field(default=5.0, ge=0.1, le=30.0)
    # Per-endpoint retry attempts (excluding the first try). 0 = no
    # retry. The 30 s OCPP timeout absorbs the budget; staying
    # conservative on the hot path avoids piling latency on a flaky
    # backend.
    backend_retry_attempts_authorize: int = Field(default=1, ge=0, le=5)
    backend_retry_attempts_sessions_open: int = Field(default=2, ge=0, le=5)
    backend_retry_attempts_sessions_close: int = Field(default=3, ge=0, le=5)
    # Circuit-breaker knobs. Trip after `threshold` consecutive
    # failures; open for `cooldown_seconds`; then half-open and let
    # one probe through.
    backend_circuit_breaker_threshold: int = Field(default=5, ge=1, le=100)
    backend_circuit_breaker_cooldown_seconds: float = Field(default=30.0, ge=1.0, le=600.0)
    # Authorize fallback when the backend is unreachable past the
    # retry budget. ADR-0023 §"Fallback policy".
    # - "reject"         — return Invalid to the charger; safe default.
    # - "accept_offline" — return Accepted with a 5-min expiry; only
    #                      enable if the operator accepts the risk.
    backend_authorize_fallback: str = Field(default="reject", pattern="^(reject|accept_offline)$")

    # ---- Authorize cache (E3-4) --------------------------------------------
    # Redis-cached `IdTagInfo` keyed on `(cp_id, id_tag)`. A cache hit
    # short-circuits the backend round-trip on the OCPP hot path.
    # 30 s is short enough for `Blocked`/`Expired` decisions to
    # propagate within ~30 s, long enough to absorb depot-shift
    # bursts. Operator can disable entirely via the boolean below or
    # drop the TTL toward 1 s for ops debugging.
    backend_authorize_cache_enabled: bool = Field(default=True)
    backend_authorize_cache_ttl_seconds: int = Field(default=30, ge=1, le=3600)


def get_settings() -> Settings:
    """Build a fresh `Settings` from the current environment.

    No caching — call sites that need a stable reference should hold the
    returned instance themselves. This makes per-test overrides via
    `monkeypatch.setenv` reliable.
    """
    return Settings()
