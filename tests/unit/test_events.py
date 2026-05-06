"""Unit tests for the Kafka event producer."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from eveys_ocpp import events as events_module
from eveys_ocpp.events import KafkaEventProducer, NullEventProducer
from eveys_ocpp.settings import Settings


@pytest.mark.asyncio
async def test_null_producer_lifecycle_is_no_op() -> None:
    p = NullEventProducer()
    await p.start()
    await p.publish(topic="cp.meter", key="CP_1", value=b"\x00\x01")
    await p.stop()


@pytest.mark.asyncio
async def test_kafka_producer_publish_before_start_raises() -> None:
    p = KafkaEventProducer(bootstrap_servers="localhost:9092")
    with pytest.raises(RuntimeError, match="before start"):
        await p.publish(topic="cp.meter", key="CP_1", value=b"\x00")


def test_kafka_producer_from_settings() -> None:
    s = Settings(kafka_brokers="kafka:9092")
    p = KafkaEventProducer.from_settings(s)
    assert p._bootstrap_servers == "kafka:9092"


def test_settings_kafka_defaults() -> None:
    s = Settings()
    assert s.kafka_brokers == "localhost:9092"
    assert s.kafka_topic_cp_meter == "cp.meter"


# ---- E2-7 producer hardening ----------------------------------------------


def test_settings_kafka_durability_defaults() -> None:
    """ADR-0019 defaults are honoured by Settings."""
    s = Settings()
    assert s.kafka_acks == "all"
    assert s.kafka_enable_idempotence is True
    assert s.kafka_linger_ms == 5
    assert s.kafka_request_timeout_ms == 30_000
    assert s.kafka_retry_backoff_ms == 200


def test_kafka_producer_from_settings_threads_durability_kwargs() -> None:
    """Settings → producer wiring carries every E2-7 knob."""
    s = Settings(
        kafka_brokers="kafka:9092",
        kafka_acks="1",  # operator override
        kafka_enable_idempotence=False,
        kafka_linger_ms=20,
        kafka_request_timeout_ms=15_000,
        kafka_retry_backoff_ms=500,
    )
    p = KafkaEventProducer.from_settings(s)
    assert p._acks == "1"
    assert p._enable_idempotence is False
    assert p._linger_ms == 20
    assert p._request_timeout_ms == 15_000
    assert p._retry_backoff_ms == 500


@pytest.mark.asyncio
async def test_kafka_producer_start_passes_durability_kwargs_to_aiokafka(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The kwargs configured on `KafkaEventProducer` reach AIOKafkaProducer.

    aiokafka does not support per-call linger or per-publish acks
    overrides, so the only place these knobs are exercised is the
    `AIOKafkaProducer(...)` constructor inside `start()`. Verify by
    capturing the constructor call.
    """
    fake_inner = MagicMock()
    fake_inner.start = AsyncMock()
    fake_inner.stop = AsyncMock()
    fake_ctor = MagicMock(return_value=fake_inner)
    monkeypatch.setattr(events_module, "AIOKafkaProducer", fake_ctor)

    p = KafkaEventProducer(
        bootstrap_servers="kafka:9092",
        acks="all",
        enable_idempotence=True,
        linger_ms=5,
        request_timeout_ms=30_000,
        retry_backoff_ms=200,
    )
    await p.start()

    fake_ctor.assert_called_once()
    kwargs = fake_ctor.call_args.kwargs
    assert kwargs["bootstrap_servers"] == "kafka:9092"
    assert kwargs["client_id"] == "eveys-ocpp"
    assert kwargs["acks"] == "all"
    assert kwargs["enable_idempotence"] is True
    assert kwargs["linger_ms"] == 5
    assert kwargs["request_timeout_ms"] == 30_000
    assert kwargs["retry_backoff_ms"] == 200
    fake_inner.start.assert_awaited_once()


@pytest.mark.asyncio
async def test_kafka_producer_start_uses_overridden_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operator-tuned values reach the underlying producer."""
    fake_inner = MagicMock()
    fake_inner.start = AsyncMock()
    fake_inner.stop = AsyncMock()
    fake_ctor = MagicMock(return_value=fake_inner)
    monkeypatch.setattr(events_module, "AIOKafkaProducer", fake_ctor)

    p = KafkaEventProducer(
        bootstrap_servers="kafka:9092",
        acks="1",
        enable_idempotence=False,
        linger_ms=0,
        request_timeout_ms=10_000,
        retry_backoff_ms=50,
    )
    await p.start()

    kwargs = fake_ctor.call_args.kwargs
    assert kwargs["acks"] == "1"
    assert kwargs["enable_idempotence"] is False
    assert kwargs["linger_ms"] == 0
    assert kwargs["request_timeout_ms"] == 10_000
    assert kwargs["retry_backoff_ms"] == 50
