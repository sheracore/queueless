"""Inspect and replay the dead-letter topic.

    python -m app.dlq list      show every message in the DLQ (read-only)
    python -m app.dlq replay    send every NEW DLQ message back to the first retry topic
"""
import json
import sys
import time
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition

from app.config import settings
from app.retry import ATTEMPT, ERROR, ORIGINAL_OFFSET, ORIGINAL_PARTITION, RETRY_AT, dlq_topic, headers_of, retry_topic


def read_all(group_id: str, commit: bool):
    """Yield the messages that are in the DLQ NOW, then stop.

    Messages that arrive while we run are not yielded. Without this rule, a replayed message
    that fails again comes back into the DLQ, gets replayed again, and the loop never ends.
    """
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    topic = dlq_topic()
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    # The "end" of each partition at start time: the offset the NEXT message will get.
    end = {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in partitions}

    consumer.subscribe([topic])
    idle = 0
    try:
        while idle < 3:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                continue
            if msg.error():
                continue
            idle = 0
            if msg.offset() >= end[msg.partition()]:
                continue  # arrived after we started: leave it for the next run
            yield msg
            if commit:
                consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()


def list_messages() -> None:
    # A random group id: we read everything and commit nothing (read-only).
    count = 0
    for msg in read_all(f"dlq-inspect-{uuid.uuid4()}", commit=False):
        h = headers_of(msg)
        try:
            event_type = json.loads(msg.value()).get("event_type", "?")
        except (ValueError, AttributeError):
            event_type = "<not JSON>"
        print(f"dlq@{msg.offset()}  key={msg.key()!r}  {event_type}  attempts={h.get(ATTEMPT, '0')}  "
              f"from={h.get(ORIGINAL_PARTITION)}@{h.get(ORIGINAL_OFFSET)}  error={h.get(ERROR)}")
        count += 1
    print(f"{count} message(s) in {dlq_topic()}")


def replay() -> None:
    # A FIXED group id with commits: each DLQ message is replayed only once.
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "enable.idempotence": True,
                         "partitioner": "murmur2_random"})
    target = retry_topic(settings.retry_delays_seconds[0])
    count = 0
    for msg in read_all(f"{settings.consumer_group_id}-dlq-replay", commit=True):
        h = headers_of(msg)
        h[ATTEMPT] = "0"                               # a fresh set of retries
        h[RETRY_AT] = str(int(time.time() * 1000))     # due now
        producer.produce(target, key=msg.key(), value=msg.value(), headers=h)
        producer.flush(10)
        count += 1
    print(f"Replayed {count} message(s) to {target}")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "list"
    {"list": list_messages, "replay": replay}[command]()