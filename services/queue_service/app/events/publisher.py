import logging
from dataclasses import dataclass
from typing import Protocol

from confluent_kafka import KafkaError, Message, Producer

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OutgoingMessage:
    id: int                    # the outbox row id, so we know WHICH rows were delivered
    topic: str
    key: str
    value: bytes
    headers: dict[str, str]


class MessagePublisher(Protocol):
    # Send a batch and return the ids of the messages that Kafka CONFIRMED.
    def publish_batch(self, messages: list[OutgoingMessage]) -> set[int]: ...


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, delivery_timeout_ms: int = 10_000):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "queue-outbox-relay",
            "acks": "all",                    # wait until all in-sync replicas have the message
            "enable.idempotence": True,       # producer retries never create duplicates in the log
            "linger.ms": 5,                   # wait up to 5 ms to batch messages together
            "partitioner": "murmur2_random",  # same key -> same partition as Java clients
            # Give up on a message after 10 s (default: 5 min). The outbox row stays
            # unpublished, so the relay simply tries again later. The database is now
            # our durable buffer, not the producer's memory.
            "message.timeout.ms": delivery_timeout_ms,
        })
        self._flush_timeout = delivery_timeout_ms / 1000 + 5

    def publish_batch(self, messages: list[OutgoingMessage]) -> set[int]:
        delivered: set[int] = set()

        def on_delivery(message_id: int):
            def callback(err: KafkaError | None, msg: Message) -> None:
                if err is None:
                    delivered.add(message_id)
                else:
                    logger.warning("Outbox message %s not delivered: %s", message_id, err)
            return callback

        for m in messages:
            self._producer.produce(
                m.topic,
                key=m.key.encode(),
                value=m.value,
                headers=m.headers,
                on_delivery=on_delivery(m.id),
            )
        # flush() waits until every message is confirmed or failed, and runs the callbacks.
        remaining = self._producer.flush(self._flush_timeout)
        if remaining:
            # Some messages are still waiting inside the producer. Drop them: their outbox
            # rows stay unpublished and are sent again next round. If we kept them, the
            # producer could ALSO send them later, and Kafka would get a duplicate.
            self._producer.purge(in_queue=True, in_flight=False)
            self._producer.poll(0)  # run the callbacks of the purged messages
        return delivered

    def close(self) -> None:
        self._producer.flush(10)