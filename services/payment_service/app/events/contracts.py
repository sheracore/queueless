# services/payment_service/app/events/contracts.py
import uuid

from pydantic import BaseModel

from app.config import settings
from app.events.catalog import EVENT_MODELS, EventType
from app.models import OutboxEvent, Payment


# ---- what we CONSUME from queue.events (consumer-side contract, no shared code) ----

class QueueEntryData(BaseModel):
    queue_entry_id: int
    queue_id: int
    user_id: int
    ticket_number: int
    deposit_amount: int | None = None   # only in TicketReserved
    reason: str | None = None           # only in TicketReservationCancelled


class IncomingEvent(BaseModel):
    event_id: uuid.UUID
    event_type: str
    event_version: int
    data: QueueEntryData


# ---- what we PUBLISH to payment.events (through the outbox) ----

def payment_outbox_event(event_type: EventType, payment: Payment, reason: str | None = None) -> OutboxEvent:
    """Build the typed event, then store it in the outbox. A payload that does not match
    the catalog fails HERE, inside our transaction, never on a consumer."""
    model = EVENT_MODELS[event_type]
    data_model = model.model_fields["data"].annotation
    data = {
        "payment_id": payment.id,
        "queue_entry_id": payment.queue_entry_id,
        "queue_id": payment.queue_id,
        "user_id": payment.user_id,
        "amount": payment.amount,
        "status": payment.status,
    }
    if reason is not None:
        data["reason"] = reason
    event = model(data=data_model(**data))
    return OutboxEvent(
        event_id=event.event_id,
        event_type=event.event_type.value,
        topic=settings.payment_events_topic,
        message_key=str(payment.queue_id),  # same key as queue.events: per-queue ordering
        payload=event.model_dump(mode="json"),
        created_at=event.occurred_at,
    )