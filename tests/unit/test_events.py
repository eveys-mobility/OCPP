"""Unit tests for the Kafka event producer."""

from __future__ import annotations

import pytest

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
