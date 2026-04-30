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


def get_settings() -> Settings:
    """Build a fresh `Settings` from the current environment.

    No caching — call sites that need a stable reference should hold the
    returned instance themselves. This makes per-test overrides via
    `monkeypatch.setenv` reliable.
    """
    return Settings()
