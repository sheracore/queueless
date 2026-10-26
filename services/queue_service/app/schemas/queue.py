from pydantic import BaseModel, Field

from app.models.queue import QueueStatus


class QueueCreate(BaseModel):
    name: str
    deposit_amount: int = Field(default=0, ge=0)  # 0 = free; > 0 = joining needs a payment


class QueueResponse(BaseModel):
    id: int
    business_id: int
    name: str
    status: QueueStatus
    deposit_amount: int

    model_config = {
        "from_attributes": True,
    }
