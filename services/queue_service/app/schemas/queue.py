from pydantic import BaseModel

from app.models.queue import QueueStatus


class QueueCreate(BaseModel):
    name: str


class QueueResponse(BaseModel):
    id: int
    business_id: int
    name: str
    status: QueueStatus

    model_config = {
        "from_attributes": True,
    }
