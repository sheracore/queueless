import threading
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

    def close(self, timeout: float = 10.0) -> int: ...


class KafkaEventPublisher:
    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "queue-service",
            "acks": "all",                 # wait until all in-sync replicas have the message
            "enable.idempotence": True,    # producer retries never create duplicates in the log
            "linger.ms": 5,                # wait up to 5 ms to batch messages together
            "partitioner": "murmur2_random",  # same key -> same partition as Java clients
        })

        # Delivery callbacks only run inside poll()/flush(). This thread calls poll()
        # all the time, so success/failure is reported within ~0.5 s, not at the next request.
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._poll_loop, name="kafka-delivery-reports", daemon=True)
        self._poller.start()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.5)

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

    @staticmethod
    def _on_delivery(err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            logger.error("Event delivery failed (key=%s): %s", msg.key(), err)
        else:
            logger.info(
                "Event delivered to %s [partition %s] @ offset %s",
                msg.topic(), msg.partition(), msg.offset(),
            )

    def close(self, timeout: float = 10.0) -> int:
        # Stop the poll thread, then send what is still buffered.
        # Returns how many messages are still undelivered (0 = all good).
        self._stop.set()
        self._poller.join()
        return self._producer.flush(timeout)


@lru_cache
def get_publisher() -> EventPublisher:
    # One producer per process: it is thread-safe and expensive to create.
    return KafkaEventPublisher(settings.kafka_bootstrap_servers)
