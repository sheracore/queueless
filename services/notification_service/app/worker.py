import logging
import signal
import socket
import time

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError

from app.config import settings
from app.database import SessionLocal
from app.events import IncomingEvent
from app.handlers import handle_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("notification.worker")

running = True


def stop(signum, frame):
    # docker stop sends SIGTERM. We finish the current message, then leave the loop.
    global running
    running = False


# With cooperative-sticky, these callbacks receive only the CHANGE, not the full assignment.
def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    if partitions:
        logger.info("Partitions added: %s", [p.partition for p in partitions])


def on_revoke(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    if partitions:
        logger.info("Partitions removed: %s", [p.partition for p in partitions])


def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.consumer_group_id,
        "client.id": f"notification-{socket.gethostname()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "partition.assignment.strategy": "cooperative-sticky",
    })


def process(msg: Message) -> None:
    try:
        event = IncomingEvent.model_validate_json(msg.value())
    except ValidationError as exc:
        # A "poison message": it will never parse, retrying is useless. Log it and skip it.
        logger.error("Skipping invalid message at %s[%s]@%s: %s",
                     msg.topic(), msg.partition(), msg.offset(), exc)
        return

    with SessionLocal() as db:
        handle_event(db, event)


def run() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer = create_consumer()
    consumer.subscribe([settings.queue_events_topic], on_assign=on_assign, on_revoke=on_revoke)
    logger.info("Consuming %s as group %s", settings.queue_events_topic, settings.consumer_group_id)

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue

            try:
                process(msg)
            except OperationalError as exc:
                # Temporary problem (database down). Do NOT commit.
                # Move the position back to this message so the next poll() returns it again.
                logger.warning("Database unavailable, retrying offset %s in 5s: %s", msg.offset(), exc)
                consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                time.sleep(5)
                continue

            # Commit only AFTER the work is done: at-least-once delivery.
            consumer.commit(message=msg, asynchronous=False)
    except KafkaException as exc:
        logger.exception("Fatal Kafka error: %s", exc)
        raise
    finally:
        # Leave the group cleanly: partitions move to other members immediately,
        # instead of after session.timeout.ms (45 s).
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    run()