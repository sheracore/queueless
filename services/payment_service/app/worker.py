"""Payment Service worker: reacts to the deposit saga events on queue.events.

    python -m app.worker
"""
import logging
import signal
import socket
import time

from confluent_kafka import Consumer, KafkaException, TopicPartition
from pydantic import ValidationError

from app.config import settings
from app.database import SessionLocal
from app.events.contracts import IncomingEvent
from app.gateway import FakePaymentGateway
from app.handlers import handle_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("payment.worker")

SUPPORTED_EVENT_VERSIONS = {1}

running = True


def stop(signum, frame):
    global running
    running = False


def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.consumer_group_id,
        "client.id": f"{settings.consumer_group_id}-{socket.gethostname()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "partition.assignment.strategy": "cooperative-sticky",
    })


def run() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    gateway = FakePaymentGateway(settings.payment_decline_rate, settings.payment_failure_rate)
    consumer = create_consumer()
    consumer.subscribe([settings.queue_events_topic])
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
                event = IncomingEvent.model_validate_json(msg.value())
            except ValidationError:
                event = None   # not a queue-entry event we understand: not ours
            if event is not None and event.event_version in SUPPORTED_EVENT_VERSIONS:
                try:
                    with SessionLocal() as db:
                        result = handle_event(db, event, gateway)
                    if result != "ignored":
                        logger.info("%s for entry %s: %s", event.event_type, event.data.queue_entry_id, result)
                except Exception as exc:
                    # BLOCKING retry on purpose: for the same queue, "TicketReserved" must be handled
                    # before "TicketReservationCancelled", or we could miss a refund.
                    logger.warning("Retrying offset %s in 5s: %s: %s", msg.offset(), type(exc).__name__, exc)
                    consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    time.sleep(5)
                    continue

            try:
                consumer.commit(message=msg, asynchronous=False)
            except KafkaException as exc:
                logger.warning("Commit failed, the event will be redelivered: %s", exc)
    finally:
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    run()