import logging
from functools import lru_cache
from typing import Protocol

from confluent_kafka import KafkaError, Message, Producer

from app.config import settings
from app.events.envelope import Event

logger = logging.getLogger(__name__)


class EventPublisher(Protocol):
    # Anything with this method is a publisher: Kafka in production, a list in tests.
    def publish(self, topic: str, key: str, event: Event) -> None: ...

    def flush(self, timeout: float = 10.0) -> int: ...


class KafkaEventPublisher:
    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "queue-service",
            "acks": "all",                 # wait until all in-sync replicas have the message
            "enable.idempotence": True,    # producer retries never create duplicates in the log
            "linger.ms": 5,                # wait up to 5 ms to batch messages together
        })

    def publish(self, topic: str, key: str, event: Event) -> None:
        # produce() is asynchronous: it only puts the message in a local buffer.
        # A background thread sends it; the result arrives later in _on_delivery.
        self._producer.produce(
            topic,
            key=key.encode(),
            value=event.model_dump_json().encode(),
            headers={"event_type": event.event_type.value},
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)  # serve delivery callbacks of earlier messages, don't block

    @staticmethod
    def _on_delivery(err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            logger.error("Event delivery failed (key=%s): %s", msg.key(), err)
        else:
            logger.info(
                "Event delivered to %s [partition %s] @ offset %s",
                msg.topic(), msg.partition(), msg.offset(),
            )

    def flush(self, timeout: float = 10.0) -> int:
        # Block until the buffer is sent; returns how many messages are still undelivered.
        return self._producer.flush(timeout)


@lru_cache
def get_publisher() -> EventPublisher:
    # One producer per process: it is thread-safe and expensive to create.
    return KafkaEventPublisher(settings.kafka_bootstrap_servers)
