# services/queue_service/app/events/catalog.py
"""The catalog of events the Queue Service PUBLISHES on queue.events.

This file IS the contract. Each event type is one closed Pydantic model. From these models
tools/schemas.py generates the JSON Schemas in contracts/queue.events/ and registers them
in the Schema Registry.

Rules (enforced by the registry with BACKWARD compatibility):
  - adding an OPTIONAL field (with a default)  -> allowed
  - adding a required field, removing, renaming, changing a type -> NOT allowed
Pure Pydantic on purpose: no database or settings imports, so tools can load it anywhere.
"""
from datetime import datetime, timezone
from enum import StrEnum
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

TOPIC = "queue.events"


class EventType(StrEnum):
    CUSTOMER_JOINED_QUEUE = "CustomerJoinedQueue"
    CUSTOMER_CALLED = "CustomerCalled"
    CUSTOMER_SERVED = "CustomerServed"
    CUSTOMER_NO_SHOW = "CustomerMarkedNoShow"
    CUSTOMER_LEFT_QUEUE = "CustomerLeftQueue"
    # Deposit saga
    TICKET_RESERVED = "TicketReserved"
    TICKET_RESERVATION_CANCELLED = "TicketReservationCancelled"


class Closed(BaseModel):
    # extra="forbid" -> "additionalProperties": false in the JSON Schema.
    # A CLOSED producer schema is what lets us add optional fields later (see contracts/README.md).
    model_config = ConfigDict(extra="forbid")


# ---------- payloads ("data") ----------

class QueueEntryData(Closed):
    queue_entry_id: int
    queue_id: int
    user_id: int
    ticket_number: int
    status: str
    joined_at: datetime
    called_at: datetime | None = None
    served_at: datetime | None = None
    # v2 (non-breaking): added later, OPTIONAL with a default, so old events stay valid.
    queue_name: str | None = None


class TicketReservedData(QueueEntryData):
    deposit_amount: int = Field(ge=0, description="Smallest currency unit. An integer, never a decimal string.")


class TicketReservationCancelledData(QueueEntryData):
    reason: Literal["payment_declined", "payment_timeout"]


# ---------- envelopes: one per event type ----------

def _all_fields_required(schema: dict, model: type) -> None:
    # Envelope fields have Python defaults (event_id, occurred_at, ...) for convenience, but they are
    # ALWAYS on the wire and consumers rely on them, so the published schema marks them all required.
    schema["required"] = sorted(schema["properties"])


class EventEnvelope(Closed):
    model_config = ConfigDict(extra="forbid", json_schema_extra=_all_fields_required)

    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    event_version: Literal[1] = 1  # MAJOR version: changes only for a breaking change
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    producer: Literal["queue-service"] = "queue-service"
    data: QueueEntryData


class CustomerJoinedQueue(EventEnvelope):
    event_type: Literal[EventType.CUSTOMER_JOINED_QUEUE] = EventType.CUSTOMER_JOINED_QUEUE


class CustomerCalled(EventEnvelope):
    event_type: Literal[EventType.CUSTOMER_CALLED] = EventType.CUSTOMER_CALLED


class CustomerServed(EventEnvelope):
    event_type: Literal[EventType.CUSTOMER_SERVED] = EventType.CUSTOMER_SERVED


class CustomerMarkedNoShow(EventEnvelope):
    event_type: Literal[EventType.CUSTOMER_NO_SHOW] = EventType.CUSTOMER_NO_SHOW


class CustomerLeftQueue(EventEnvelope):
    event_type: Literal[EventType.CUSTOMER_LEFT_QUEUE] = EventType.CUSTOMER_LEFT_QUEUE


class TicketReserved(EventEnvelope):
    event_type: Literal[EventType.TICKET_RESERVED] = EventType.TICKET_RESERVED
    data: TicketReservedData


class TicketReservationCancelled(EventEnvelope):
    event_type: Literal[EventType.TICKET_RESERVATION_CANCELLED] = EventType.TICKET_RESERVATION_CANCELLED
    data: TicketReservationCancelledData


EVENT_MODELS: dict[EventType, type[EventEnvelope]] = {
    EventType.CUSTOMER_JOINED_QUEUE: CustomerJoinedQueue,
    EventType.CUSTOMER_CALLED: CustomerCalled,
    EventType.CUSTOMER_SERVED: CustomerServed,
    EventType.CUSTOMER_NO_SHOW: CustomerMarkedNoShow,
    EventType.CUSTOMER_LEFT_QUEUE: CustomerLeftQueue,
    EventType.TICKET_RESERVED: TicketReserved,
    EventType.TICKET_RESERVATION_CANCELLED: TicketReservationCancelled,
}


# ---------- fixed examples: committed next to the schemas, used by consumer contract tests ----------

def examples() -> dict[str, EventEnvelope]:
    at = datetime(2026, 11, 6, 9, 0, tzinfo=timezone.utc)
    base = dict(queue_entry_id=12, queue_id=3, user_id=42, ticket_number=7, joined_at=at, queue_name="Dentist")

    def envelope(model: type[EventEnvelope], n: int, data: BaseModel) -> EventEnvelope:
        return model(event_id=UUID(int=n), occurred_at=at, data=data)

    return {
        "CustomerJoinedQueue": envelope(CustomerJoinedQueue, 1, QueueEntryData(**base, status="WAITING")),
        "CustomerCalled": envelope(CustomerCalled, 2, QueueEntryData(**base, status="CALLED", called_at=at)),
        "CustomerServed": envelope(CustomerServed, 3, QueueEntryData(**base, status="SERVED", called_at=at, served_at=at)),
        "CustomerMarkedNoShow": envelope(CustomerMarkedNoShow, 4, QueueEntryData(**base, status="NO_SHOW", called_at=at)),
        "CustomerLeftQueue": envelope(CustomerLeftQueue, 5, QueueEntryData(**base, status="LEFT")),
        "TicketReserved": envelope(TicketReserved, 6, TicketReservedData(**base, status="RESERVED", deposit_amount=50_000)),
        "TicketReservationCancelled": envelope(
            TicketReservationCancelled, 7, TicketReservationCancelledData(**base, status="CANCELLED", reason="payment_timeout")),
    }