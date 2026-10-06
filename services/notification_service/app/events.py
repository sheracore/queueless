from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


# The CONTRACT we expect from the queue service, written on the consumer side.
# We do not import the queue service's code: services share contracts, not code.

class QueueEntryData(BaseModel):
    queue_entry_id: int
    queue_id: int
    user_id: int
    ticket_number: int
    status: str


class IncomingEvent(BaseModel):
    event_id: UUID
    event_type: str
    event_version: int
    occurred_at: datetime
    producer: str
    data: QueueEntryData