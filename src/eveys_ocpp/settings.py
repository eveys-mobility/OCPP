"""Environment-driven configuration for eveys/ocpp.

All runtime configuration goes through this module. Direct `os.environ` reads
elsewhere are forbidden (see `03-coding-standards.md`).
"""

from __future__ import annotations

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


def get_settings() -> Settings:
    """Build a fresh `Settings` from the current environment.

    No caching — call sites that need a stable reference should hold the
    returned instance themselves. This makes per-test overrides via
    `monkeypatch.setenv` reliable.
    """
    return Settings()
