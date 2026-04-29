"""Unit tests for settings env-driven configuration."""

from __future__ import annotations

import pytest

from eveys_ocpp.settings import Settings, get_settings


def test_defaults_are_dev_safe() -> None:
    s = Settings()
    assert s.ws_host == "0.0.0.0"
    assert s.ws_port == 9000
    assert s.heartbeat_interval_seconds == 300
    assert s.log_level == "INFO"
    assert s.log_json is True


def test_env_prefix_is_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EVEYS_OCPP_WS_PORT", "9999")
    monkeypatch.setenv("EVEYS_OCPP_HEARTBEAT_INTERVAL_SECONDS", "60")

    s = get_settings()
    assert s.ws_port == 9999
    assert s.heartbeat_interval_seconds == 60


def test_settings_are_frozen() -> None:
    from pydantic import ValidationError

    s = Settings()
    with pytest.raises(ValidationError):
        s.ws_port = 1234  # type: ignore[misc]
