from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

from app.models.queue_entry import QueueEntry, QueueEntryStatus


class EventType(StrEnum):
    CUSTOMER_JOINED_QUEUE = "CustomerJoinedQueue"
    CUSTOMER_CALLED = "CustomerCalled"
    CUSTOMER_SERVED = "CustomerServed"
    CUSTOMER_NO_SHOW = "CustomerMarkedNoShow"
    CUSTOMER_LEFT_QUEUE = "CustomerLeftQueue"
    # Deposit saga
    TICKET_RESERVED = "TicketReserved"
    TICKET_RESERVATION_CANCELLED = "TicketReservationCancelled"


class Event(BaseModel):
    # Envelope: the same fields on every event, whatever its type.
    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    event_version: int = 1
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    producer: str = "queue-service"
    # Payload: the facts specific to this event type.
    data: dict[str, Any]


def entry_event(
    event_type: EventType,
    queue_entry: QueueEntry,
    deposit_amount: Decimal | None = None,
    reserved_until: datetime | None = None,
    reason: str | None = None,
) -> Event:
    data={
        "queue_entry_id": queue_entry.id,
        "queue_id": queue_entry.queue_id,
        "user_id": queue_entry.user_id,
        "ticket_number": queue_entry.ticket_number,
        "status": QueueEntryStatus(queue_entry.status).name,
        "joined_at": queue_entry.joined_at,
        "called_at": queue_entry.called_at,
        "served_at": queue_entry.served_at,
    }
    # event-specific fields: only included when set
    if deposit_amount is not None:
        data["deposit_amount"] = deposit_amount
    if reserved_until is not None:
        data["reserved_until"] = reserved_until
    if reason is not None:
        data["reason"] = reason
    return Event(event_type=event_type, data=data)