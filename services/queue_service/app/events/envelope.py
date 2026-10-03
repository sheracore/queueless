from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from app.models.queue_entry import QueueEntry, QueueEntryStatus


class EventType(StrEnum):
    CUSTOMER_JOINED_QUEUE = "CustomerJoinedQueue"
    CUSTOMER_CALLED = "CustomerCalled"
    CUSTOMER_SERVED = "CustomerServed"
    CUSTOMER_NO_SHOW = "CustomerMarkedNoShow"
    CUSTOMER_LEFT_QUEUE = "CustomerLeftQueue"


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
) -> Event:
    return Event(
        event_type=event_type,
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
    )