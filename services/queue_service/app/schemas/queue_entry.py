from datetime import datetime

from pydantic import BaseModel

from app.models.queue_entry import QueueEntryStatus


class QueueJoinResponse(BaseModel):
    id: int
    queue_id: int
    ticket_number: int
    status: QueueEntryStatus
    joined_at: datetime

    model_config = {
        "from_attributes": True,
    }


class QueueJoinRequest(BaseModel):
    # Temporary: once the Identity Service exists, this comes from the auth token.
    user_id: int


class QueueEntryResponse(BaseModel):
    id: int
    queue_id: int
    user_id: int
    ticket_number: int
    status: QueueEntryStatus
    joined_at: datetime
    called_at: datetime | None = None
    served_at: datetime | None = None

    model_config = {
        "from_attributes": True
    }


class QueueLeaveRequest(BaseModel):
    # Temporary: once the Identity Service exists, this comes from the auth token.
    user_id: int