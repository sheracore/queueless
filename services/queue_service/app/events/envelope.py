# services/queue_service/app/events/envelope.py
from datetime import datetime

from app.events.catalog import EVENT_MODELS, EventEnvelope, EventType
from app.models.queue_entry import QueueEntry, QueueEntryStatus

__all__ = ["EventType", "entry_event"]


def entry_event(
    event_type: EventType,
    queue_entry: QueueEntry,
    deposit_amount: int | None = None,
    reason: str | None = None,
) -> EventEnvelope:
    """Build the typed event for a queue entry. Validation happens HERE, in the producer:
    a wrong type or a missing field fails inside the API transaction, before anything is published."""
    model = EVENT_MODELS[event_type]
    data_model = model.model_fields["data"].annotation
    data: dict = {
        "queue_entry_id": queue_entry.id,
        "queue_id": queue_entry.queue_id,
        "user_id": queue_entry.user_id,
        "ticket_number": queue_entry.ticket_number,
        "status": QueueEntryStatus(queue_entry.status).name,
        "joined_at": queue_entry.joined_at,
        "called_at": queue_entry.called_at,
        "served_at": queue_entry.served_at,
        "queue_name": queue_entry.queue.name,
    }
    # event-specific fields: only included when set
    if deposit_amount is not None:
        data["deposit_amount"] = deposit_amount
    if reason is not None:
        data["reason"] = reason
    return model(data=data_model(**data))