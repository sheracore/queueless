import json
import logging
import signal
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.events.publisher import KafkaPublisher, MessagePublisher, OutgoingMessage
from app.models.outbox_event import OutboxEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("queue.outbox_relay")

BATCH_SIZE = 100
IDLE_SLEEP_SECONDS = 0.5
CLEANUP_EVERY_SECONDS = 3600
KEEP_PUBLISHED_FOR = timedelta(days=7)

running = True


def stop(signum, frame):
    global running
    running = False


def to_message(row: OutboxEvent) -> OutgoingMessage:
    return OutgoingMessage(
        id=row.id,
        topic=row.topic,
        key=row.message_key,
        value=json.dumps(row.payload).encode(),
        headers={"event_type": row.event_type},
    )


def relay_once(db: Session, publisher: MessagePublisher, batch_size: int = BATCH_SIZE) -> tuple[int, int]:
    """Send one batch of unpublished events. Returns (found, published)."""
    rows = db.scalars(
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.id)          # oldest first: keeps the original order
        .limit(batch_size)
        .with_for_update(skip_locked=True)  # lock these rows while we work on them
    ).all()
    if not rows:
        db.rollback()  # end the (empty) transaction
        return 0, 0

    delivered = publisher.publish_batch([to_message(row) for row in rows])

    now = datetime.now(timezone.utc)
    for row in rows:
        if row.id in delivered:
            row.published_at = now
    # Rows that failed keep published_at = NULL and are retried in the next round.
    db.commit()
    return len(rows), len(delivered)


def cleanup(db: Session, older_than: timedelta = KEEP_PUBLISHED_FOR) -> int:
    """Delete old, already-published rows so the table does not grow forever."""
    result = db.execute(
        delete(OutboxEvent).where(OutboxEvent.published_at < datetime.now(timezone.utc) - older_than)
    )
    db.commit()
    return result.rowcount


def run() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    publisher = KafkaPublisher(settings.kafka_bootstrap_servers)
    last_cleanup = 0.0
    logger.info("Outbox relay started (batch size %s)", BATCH_SIZE)

    try:
        while running:
            try:
                with SessionLocal() as db:
                    found, published = relay_once(db, publisher)
                    if time.monotonic() - last_cleanup > CLEANUP_EVERY_SECONDS:
                        removed = cleanup(db)
                        last_cleanup = time.monotonic()
                        if removed:
                            logger.info("Cleanup: deleted %s old published events", removed)
            except OperationalError as exc:
                logger.warning("Database unavailable, retrying in 5s: %s", exc)
                time.sleep(5)
                continue

            if found:
                logger.info("Published %s/%s outbox events", published, found)
            if published < found:
                time.sleep(2)                    # Kafka has problems: back off a little
            elif found < BATCH_SIZE:
                time.sleep(IDLE_SLEEP_SECONDS)   # no more waiting events: wait for new ones
            # A full batch was published: loop at once, there may be more waiting.
    finally:
        publisher.close()
        logger.info("Outbox relay stopped")


if __name__ == "__main__":
    run()