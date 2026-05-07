"""Kafka event producer.

Publishes serialized `EventEnvelope` protos (proto/events/v1/events.proto)
to the platform's event topics. Each record is keyed by `cp_id` so a
single Kafka partition holds the full ordered stream for a given charger
(per AGENTS rule "message ordering is preserved per charger").

Producer durability and latency knobs follow ADR-0019: `acks=all`,
`enable_idempotence=True`, modest `linger_ms` for batching headroom on
the high-volume `cp.meter` topic without putting a real latency floor on
the low-volume billing-relevant topics. Every knob is exposed via
`Settings` so an operator can re-tune without a code change.

Two implementations:

* `KafkaEventProducer` — real `aiokafka` producer. Used in production
  and in the e2e suite that has Kafka up.
* `NullEventProducer` — drops everything. Used in unit tests + dev
  configurations that don't need Kafka. Lets handlers stay shaped the
  same regardless of whether Kafka is reachable.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Protocol

from aiokafka import AIOKafkaProducer

from eveys_ocpp.metrics import registry as metrics_registry
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

    Durability config follows ADR-0019: `acks=all` + idempotent
    producer + tightened request timeout. aiokafka enforces the
    Kafka-protocol requirement that idempotent producers cap
    in-flight requests at 5 — we don't pass that explicitly because
    aiokafka doesn't expose the kwarg.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        acks: str = "all",
        enable_idempotence: bool = True,
        linger_ms: int = 5,
        request_timeout_ms: int = 30_000,
        retry_backoff_ms: int = 200,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._acks = acks
        self._enable_idempotence = enable_idempotence
        self._linger_ms = linger_ms
        self._request_timeout_ms = request_timeout_ms
        self._retry_backoff_ms = retry_backoff_ms
        self._producer: AIOKafkaProducer | None = None

    @classmethod
    def from_settings(cls, settings: Settings) -> KafkaEventProducer:
        return cls(
            bootstrap_servers=settings.kafka_brokers,
            acks=settings.kafka_acks,
            enable_idempotence=settings.kafka_enable_idempotence,
            linger_ms=settings.kafka_linger_ms,
            request_timeout_ms=settings.kafka_request_timeout_ms,
            retry_backoff_ms=settings.kafka_retry_backoff_ms,
        )

    async def start(self) -> None:
        producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id="eveys-ocpp",
            acks=self._acks,
            enable_idempotence=self._enable_idempotence,
            linger_ms=self._linger_ms,
            request_timeout_ms=self._request_timeout_ms,
            retry_backoff_ms=self._retry_backoff_ms,
        )
        await producer.start()
        self._producer = producer
        log.info(
            "kafka.producer.started",
            bootstrap_servers=self._bootstrap_servers,
            acks=self._acks,
            enable_idempotence=self._enable_idempotence,
            linger_ms=self._linger_ms,
            request_timeout_ms=self._request_timeout_ms,
            retry_backoff_ms=self._retry_backoff_ms,
        )

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            log.info("kafka.producer.stopped")
            self._producer = None

    async def publish(self, *, topic: str, key: str, value: bytes) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaEventProducer.publish called before start()")
        start = time.perf_counter()
        try:
            await self._producer.send_and_wait(topic, value=value, key=key.encode("utf-8"))
        except Exception:
            metrics_registry.KAFKA_PUBLISH_TOTAL.labels(topic=topic, outcome="error").inc()
            raise
        else:
            metrics_registry.KAFKA_PUBLISH_TOTAL.labels(topic=topic, outcome="ok").inc()
            metrics_registry.KAFKA_PUBLISH_BYTES_TOTAL.labels(topic=topic).inc(len(value))
        finally:
            metrics_registry.KAFKA_PUBLISH_LATENCY_SECONDS.labels(topic=topic).observe(
                time.perf_counter() - start
            )


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
