import logging
import signal
import socket
import sys
import time

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError

from app.channels import FakeSmsProvider, SmsSender
from app.config import settings
from app.database import SessionLocal
from app.events import IncomingEvent
from app.handlers import handle_event
from app.retry import FailureRouter, PermanentError, due_at_ms, headers_of, retry_topics

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("notification.worker")

SUPPORTED_EVENT_VERSIONS = {1}

running = True
# Retry mode: partitions we paused because their next message is not due yet -> resume time (ms).
paused_until: dict[tuple[str, int], int] = {}


def stop(signum, frame):
    # docker stop sends SIGTERM. We finish the current message, then leave the loop.
    global running
    running = False


# With cooperative-sticky, these callbacks receive only the CHANGE, not the full assignment.
def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    if partitions:
        logger.info("Partitions added: %s", [f"{p.topic}[{p.partition}]" for p in partitions])


def on_revoke(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    for p in partitions:
        paused_until.pop((p.topic, p.partition), None)
    if partitions:
        logger.info("Partitions removed: %s", [f"{p.topic}[{p.partition}]" for p in partitions])


def create_consumer(group_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": group_id,
        "client.id": f"{group_id}-{socket.gethostname()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "partition.assignment.strategy": "cooperative-sticky",
    })


def process(msg: Message, sender: SmsSender) -> None:
    try:
        event = IncomingEvent.model_validate_json(msg.value())
    except ValidationError as exc:
        # A "poison message": it will never parse. Retrying is useless.
        raise PermanentError(f"invalid event: {exc.error_count()} validation error(s)") from exc
    if event.event_version not in SUPPORTED_EVENT_VERSIONS:
        raise PermanentError(f"unsupported event_version {event.event_version}")

    with SessionLocal() as db:
        handle_event(db, event, sender)


def resume_due_partitions(consumer: Consumer) -> None:
    now = int(time.time() * 1000)
    for (topic, partition), until in list(paused_until.items()):
        if until <= now:
            consumer.resume([TopicPartition(topic, partition)])
            del paused_until[(topic, partition)]


def wait_if_not_due(consumer: Consumer, msg: Message) -> bool:
    """Retry mode: if this message's retry time is in the future, pause its partition."""
    due = due_at_ms(headers_of(msg))
    if due <= int(time.time() * 1000):
        return False

    # pause() is entirely client-side. The broker doesn’t know or care that you paused anything.
    # The consumer has a background thread that keeps sending fetch requests to the partition leaders and buffering the
    # results locally. pause([tp]) simply tells that thread: “don’t include this partition in your next fetch requests”.
    # That’s all.
    tp = TopicPartition(msg.topic(), msg.partition(), msg.offset())
    consumer.pause([tp])
    consumer.seek(tp)  # rewind: the next fetch after resume() returns this same message
    paused_until[(msg.topic(), msg.partition())] = due
    return True


def commit(consumer: Consumer, msg: Message) -> None:
    try:
        consumer.commit(message=msg, asynchronous=False)
    except KafkaException as exc:
        # Normal during a rebalance: this partition may now belong to another worker
        # (errors like ILLEGAL_GENERATION or REBALANCE_IN_PROGRESS). Not fatal: the
        # event will be delivered again, and our handler ignores duplicates.
        logger.warning("Commit failed, the event will be redelivered: %s", exc)


def run(retry_mode: bool) -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    if retry_mode:
        group_id, topics = f"{settings.consumer_group_id}-retry", retry_topics()
    else:
        group_id, topics = settings.consumer_group_id, [settings.queue_events_topic]

    consumer = create_consumer(group_id)
    consumer.subscribe(topics, on_assign=on_assign, on_revoke=on_revoke)
    router = FailureRouter(settings.kafka_bootstrap_servers)
    sender = FakeSmsProvider(failure_rate=settings.sms_failure_rate)
    logger.info("Consuming %s as group %s", topics, group_id)

    try:
        while running:
            if retry_mode:
                resume_due_partitions(consumer)

            # poll() simply skips the paused partitions.
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue
            if retry_mode and wait_if_not_due(consumer, msg):
                continue

            try:
                process(msg, sender)
            except OperationalError as exc:
                # Our OWN database is down. Every message would fail, so moving them to the
                # retry topic is pointless. Block and retry this same message (no commit).
                logger.warning("Database unavailable, retrying offset %s in 5s: %s", msg.offset(), exc)
                consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                time.sleep(5)
                continue
            except Exception as exc:
                # This ONE message failed (provider error, bad data, a bug...).
                # Park it in a retry topic or the DLQ, and keep the partition moving.
                try:
                    destination = router.route(msg, exc)
                except Exception as route_exc:
                    # Could not even park it: do NOT commit, or the event would be lost.
                    logger.error("Could not route failed message, retrying in 5s: %s", route_exc)
                    consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    time.sleep(5)
                    continue
                logger.warning("Offset %s failed (%s: %s) -> sent to %s",
                               msg.offset(), type(exc).__name__, exc, destination)

            # Commit only AFTER the message is handled or safely parked: at-least-once.
            commit(consumer, msg)
    except KafkaException as exc:
        logger.exception("Fatal Kafka error: %s", exc)
        raise
    finally:
        # Leave the group cleanly: partitions move to other members immediately,
        # instead of after session.timeout.ms (45 s).
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    run(retry_mode="--retry" in sys.argv[1:])
