import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel

from app.config import settings
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

def payment_outbox_event(event_type: str, payment: Payment, **extra: Any) -> OutboxEvent:
    event_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    envelope = {
        "event_id": str(event_id),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": now.isoformat(),
        "producer": "payment-service",
        "data": {
            "payment_id": payment.id,
            "queue_entry_id": payment.queue_entry_id,
            "queue_id": payment.queue_id,
            "user_id": payment.user_id,
            "amount": payment.amount,
            "status": payment.status,
            **extra,
        },
    }
    return OutboxEvent(
        event_id=event_id,
        event_type=event_type,
        topic=settings.payment_events_topic,
        message_key=str(payment.queue_id),  # same key as queue.events: per-queue ordering
        payload=envelope,
        created_at=now,
    )