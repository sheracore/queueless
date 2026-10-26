"""Queue Service side of the deposit saga.

    python -m app.saga_worker

1. consumes payment.events:   PaymentSucceeded -> RESERVED becomes WAITING
                              PaymentFailed    -> RESERVED becomes CANCELLED (compensation)
2. every few seconds:         RESERVED past its deadline -> CANCELLED (saga timeout)
"""
import logging
import signal
import socket
import time

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from pydantic import BaseModel, ValidationError
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.services.queue_service import QueueService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("queue.saga_worker")

EXPIRE_EVERY_SECONDS = 5

running = True


def stop(signum, frame):
    global running
    running = False


# The CONTRACT we expect from the Payment Service (consumer side, no shared code).
class PaymentData(BaseModel):
    queue_entry_id: int
    queue_id: int
    amount: int
    reason: str | None = None


class PaymentEvent(BaseModel):
    event_id: str
    event_type: str
    event_version: int
    data: PaymentData


def handle_payment_event(db: Session, service: QueueService, event: PaymentEvent) -> str:
    entry_id = event.data.queue_entry_id
    if event.event_type == "PaymentSucceeded":
        changed = service.confirm_reservation(db, entry_id)
        return "confirmed" if changed else "ignored (not RESERVED any more)"
    if event.event_type == "PaymentFailed":
        changed = service.cancel_reservation(db, entry_id, reason="payment_declined")
        return "cancelled" if changed else "ignored (not RESERVED any more)"
    return "ignored (not part of this saga step)"  # e.g. PaymentRefunded


def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.saga_consumer_group_id,
        "client.id": f"{settings.saga_consumer_group_id}-{socket.gethostname()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "partition.assignment.strategy": "cooperative-sticky",
    })


def retry_same_message(consumer: Consumer, msg: Message, reason: Exception) -> None:
    # Blocking retry: saga steps must stay in order per queue, so we never skip ahead.
    logger.warning("Retrying offset %s in 5s: %s", msg.offset(), reason)
    consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
    time.sleep(5)


def run() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    service = QueueService()
    consumer = create_consumer()
    consumer.subscribe([settings.payment_events_topic])
    logger.info("Saga worker: consuming %s, reservation timeout %ss",
                settings.payment_events_topic, settings.reservation_timeout_seconds)
    last_expire = 0.0

    try:
        while running:
            # --- the timeout part of the saga ---
            if time.monotonic() - last_expire >= EXPIRE_EVERY_SECONDS:
                last_expire = time.monotonic()
                try:
                    with SessionLocal() as db:
                        expired = service.expire_reservations(db)
                    if expired:
                        logger.info("Saga timeout: cancelled %s expired reservation(s)", expired)
                except OperationalError as exc:
                    logger.warning("Database unavailable during expiry check: %s", exc)

            # --- the payment-result part of the saga ---
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue

            try:
                event = PaymentEvent.model_validate_json(msg.value())
            except ValidationError as exc:
                logger.error("Skipping invalid payment event at offset %s: %s", msg.offset(), exc)
            else:
                try:
                    with SessionLocal() as db:
                        result = handle_payment_event(db, service, event)
                    logger.info("%s for entry %s: %s", event.event_type, event.data.queue_entry_id, result)
                except Exception as exc:
                    retry_same_message(consumer, msg, exc)
                    continue

            try:
                consumer.commit(message=msg, asynchronous=False)
            except KafkaException as exc:
                logger.warning("Commit failed, the event will be redelivered: %s", exc)
    finally:
        consumer.close()
        logger.info("Saga worker stopped")


if __name__ == "__main__":
    run()