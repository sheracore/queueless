import time

from confluent_kafka import KafkaError, Message, Producer

from app.config import settings

# Headers we add when a message leaves the main topic. They travel with it.
ATTEMPT = "x-attempt"                    # how many retries have been done (0 = original)
RETRY_AT = "x-retry-at-ms"               # do not process before this time (epoch ms)
ORIGINAL_TOPIC = "x-original-topic"
ORIGINAL_PARTITION = "x-original-partition"
ORIGINAL_OFFSET = "x-original-offset"
ERROR = "x-error"                        # the last error, for humans reading the DLQ


class PermanentError(Exception):
    """Retrying will never help: broken JSON, unsupported event version, ... -> straight to the DLQ."""


def retry_topic(delay_seconds: int) -> str:
    # One topic per delay. Example: queue.events.notification-service.retry-10s
    return f"{settings.queue_events_topic}.{settings.consumer_group_id}.retry-{delay_seconds}s"


def dlq_topic() -> str:
    return f"{settings.queue_events_topic}.{settings.consumer_group_id}.dlq"


def retry_topics() -> list[str]:
    return [retry_topic(d) for d in settings.retry_delays_seconds]


def headers_of(msg: Message) -> dict[str, str]:
    return {key: value.decode() for key, value in (msg.headers() or []) if value is not None}


def attempt_of(headers: dict[str, str]) -> int:
    return int(headers.get(ATTEMPT, "0"))


def due_at_ms(headers: dict[str, str]) -> int:
    return int(headers.get(RETRY_AT, "0"))


def next_destination(attempt: int, error: Exception, delays: list[int]) -> tuple[str, int | None]:
    """Where does a failed message go? Returns (topic, delay_seconds or None for the DLQ)."""
    if isinstance(error, PermanentError) or attempt >= len(delays):
        return dlq_topic(), None
    delay = delays[attempt]
    return retry_topic(delay), delay


def failure_headers(
    headers: dict[str, str], topic: str, partition: int, offset: int,
    error: Exception, delay: int | None, now_ms: int,
) -> dict[str, str]:
    new = dict(headers)
    # Remember where the event FIRST came from (setdefault keeps the first value).
    new.setdefault(ORIGINAL_TOPIC, topic)
    new.setdefault(ORIGINAL_PARTITION, str(partition))
    new.setdefault(ORIGINAL_OFFSET, str(offset))
    new[ERROR] = f"{type(error).__name__}: {error}"[:500]
    if delay is not None:
        new[ATTEMPT] = str(attempt_of(headers) + 1)
        new[RETRY_AT] = str(now_ms + delay * 1000)
    return new


class FailureRouter:
    """Moves a failed message to the next retry topic, or to the DLQ."""

    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "notification-failure-router",
            "acks": "all",
            "enable.idempotence": True,
            "partitioner": "murmur2_random",
        })

    def route(self, msg: Message, error: Exception) -> str:
        headers = headers_of(msg)
        topic, delay = next_destination(attempt_of(headers), error, settings.retry_delays_seconds)
        new_headers = failure_headers(
            headers, msg.topic(), msg.partition(), msg.offset(), error, delay, int(time.time() * 1000)
        )

        errors: list[KafkaError] = []
        self._producer.produce(
            topic, key=msg.key(), value=msg.value(), headers=new_headers,
            on_delivery=lambda err, _msg: errors.append(err) if err else None,
        )
        # Wait for Kafka's confirmation. We may commit the original offset ONLY after the
        # message is safely stored in the retry/DLQ topic, otherwise it could be lost.
        # The 10 means wait for up to 10 seconds for outstanding messages to finish.
        remaining = self._producer.flush(10)
        if remaining or errors:
            raise RuntimeError(f"Could not write to {topic}: {errors or 'timeout'}")
        return topic