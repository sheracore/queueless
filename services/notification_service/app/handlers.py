import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.channels import SmsSender
from app.events import IncomingEvent
from app.models import Notification, NotificationStatus

logger = logging.getLogger(__name__)

# Which events we notify about, and what we say. Other event types are ignored.
MESSAGES: dict[str, str] = {
    "CustomerJoinedQueue": "You joined the queue. Your ticket number is #{ticket_number}.",
    "CustomerCalled": "It's your turn! Ticket #{ticket_number}, please go to the counter.",
    "TicketReservationCancelled": "Your reservation for ticket #{ticket_number} was cancelled: {reason}.",
}
# Turns the machine reason in the event into words for the customer.
REASONS: dict[str, str] = {
    "payment_declined": "the deposit payment was declined",
    "payment_timeout": "the deposit was not paid in time",
}


def _find(db: Session, event: IncomingEvent) -> Notification | None:
    return db.scalar(select(Notification).where(Notification.event_id == event.event_id))


def handle_event(db: Session, event: IncomingEvent, sender: SmsSender) -> Notification | None:
    """Turn one event into one SMS. Safe to call again with the same event.

    1. save the notification as PENDING (UNIQUE event_id: at most one row per event)
    2. send the SMS  -> may raise SmsProviderError; the row stays PENDING
    3. mark it SENT
    Called again for the same event: SENT -> do nothing; PENDING -> try to send again.
    """
    template = MESSAGES.get(event.event_type)
    if template is None:
        return None  # not interesting for this service

    notification = _find(db, event)
    if notification is None:
        notification = Notification(
            event_id=event.event_id,
            event_type=event.event_type,
            user_id=event.data.user_id,
            queue_id=event.data.queue_id,
            queue_entry_id=event.data.queue_entry_id,
            ticket_number=event.data.ticket_number,
            message=template.format(
                ticket_number=event.data.ticket_number,
                reason=REASONS.get(event.data.reason or "", "it could not be confirmed"),
            ),
            created_at=datetime.now(timezone.utc),
            status=NotificationStatus.PENDING,
        )
        db.add(notification)
        try:
            db.commit()
        except IntegrityError:
            # Another worker inserted the same event a moment ago. Use its row.
            db.rollback()
            notification = _find(db, event)

    if notification.status == NotificationStatus.SENT:
        logger.info("Duplicate event %s ignored (already sent)", event.event_id)
        return None

    sender.send(notification.user_id, notification.message, idempotency_key=str(event.event_id))

    notification.status = NotificationStatus.SENT
    notification.sent_at = datetime.now(timezone.utc)
    db.commit()
    return notification