"""Kafka event producer.

Publishes serialized `EventEnvelope` protos (proto/events/v1/events.proto)
to the platform's event topics. Each record is keyed by `cp_id` so a
single Kafka partition holds the full ordered stream for a given charger
(per AGENTS rule "message ordering is preserved per charger").

Scope of THIS module: a working producer good enough for MeterValues to
flow end-to-end and be observable via `kcat`. Reconnect-on-broker-drop
tuning, batching defaults, and idempotent-producer hardening live in
task E2-7 (a separate, dedicated MR).

Two implementations:

* `KafkaEventProducer` — real `aiokafka` producer. Used in production
  and in the e2e suite that has Kafka up.
* `NullEventProducer` — drops everything. Used in unit tests + dev
  configurations that don't need Kafka. Lets handlers stay shaped the
  same regardless of whether Kafka is reachable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from aiokafka import AIOKafkaProducer

from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


class EventProducer(Protocol):
    """Minimal contract handlers depend on. Implementations may batch,
    retry, or drop — the contract is just "publish bytes to a topic
    keyed by cp_id, fire-and-forget at this layer."
    """

    async def publish(self, *, topic: str, key: str, value: bytes) -> None: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


class KafkaEventProducer:
    """Real Kafka producer.

    `start()` connects; `stop()` flushes + closes. Between them, every
    `publish()` returns once the broker has acknowledged the record.
    The handler `await`s publish — slow brokers will surface as slow
    handlers, which we'd rather know about than silently drop.
    """

    def __init__(self, *, bootstrap_servers: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None

    @classmethod
    def from_settings(cls, settings: Settings) -> KafkaEventProducer:
        return cls(bootstrap_servers=settings.kafka_brokers)

    async def start(self) -> None:
        producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            # `acks=1` (leader only) is the aiokafka default. Phase-2
            # default is fine; E2-7 may bump to `all` for durability.
            client_id="eveys-ocpp",
        )
        await producer.start()
        self._producer = producer
        log.info("kafka.producer.started", bootstrap_servers=self._bootstrap_servers)

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            log.info("kafka.producer.stopped")
            self._producer = None

    async def publish(self, *, topic: str, key: str, value: bytes) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaEventProducer.publish called before start()")
        await self._producer.send_and_wait(topic, value=value, key=key.encode("utf-8"))


class NullEventProducer:
    """Drop-everything producer.

    Used by unit tests and by configurations where Kafka is unavailable.
    Lets handlers run identically — they `await producer.publish(...)`
    either way; this just no-ops.
    """

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def publish(self, *, topic: str, key: str, value: bytes) -> None:
        log.debug(
            "kafka.null.publish",
            topic=topic,
            key=key,
            bytes=len(value),
        )
