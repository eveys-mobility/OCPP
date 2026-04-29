"""Unit tests for the structlog setup."""

from __future__ import annotations

from eveys_ocpp.observability import configure_logging, get_logger


def test_configure_logging_is_idempotent() -> None:
    configure_logging(level="DEBUG", json=False)
    configure_logging(level="INFO", json=True)
    log = get_logger("eveys_ocpp.test")
    log.info("ok", k="v")  # must not raise
