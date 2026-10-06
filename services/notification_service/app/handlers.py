import logging
from datetime import datetime, timezone

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.events import IncomingEvent
from app.models import Notification

logger = logging.getLogger(__name__)

# Which events we notify about, and what we say. Other event types are ignored.
MESSAGES: dict[str, str] = {
    "CustomerJoinedQueue": "You joined the queue. Your ticket number is #{ticket_number}.",
    "CustomerCalled": "It's your turn! Ticket #{ticket_number}, please go to the counter.",
}


def handle_event(db: Session, event: IncomingEvent) -> Notification | None:
    """Turn one event into one notification. Safe to call twice with the same event."""
    template = MESSAGES.get(event.event_type)
    if template is None:
        return None  # not interesting for this service

    notification = Notification(
        event_id=event.event_id,
        event_type=event.event_type,
        user_id=event.data.user_id,
        queue_id=event.data.queue_id,
        queue_entry_id=event.data.queue_entry_id,
        ticket_number=event.data.ticket_number,
        message=template.format(ticket_number=event.data.ticket_number),
        created_at=datetime.now(timezone.utc),
    )
    db.add(notification)
    try:
        db.commit()
    except IntegrityError:
        # The UNIQUE(event_id) constraint fired: we already handled this event
        # (Kafka delivered it again after a crash or rebalance). Do nothing.
        db.rollback()
        logger.info("Duplicate event %s ignored", event.event_id)
        return None

    logger.info("Notify user %s: %s", notification.user_id, notification.message)
    return notification