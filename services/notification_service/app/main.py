from datetime import datetime

from fastapi import Depends, FastAPI
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Notification

app = FastAPI(title="QueueLess Notification Service", version="0.1.0")


class NotificationResponse(BaseModel):
    id: int
    event_type: str
    queue_id: int
    ticket_number: int
    message: str
    created_at: datetime

    model_config = {"from_attributes": True}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/users/{user_id}/notifications", response_model=list[NotificationResponse])
def list_notifications(user_id: int, db: Session = Depends(get_db)):
    stmt = (
        select(Notification)
        .where(Notification.user_id == user_id)
        .order_by(Notification.created_at.desc(), Notification.id.desc())
    )
    return db.scalars(stmt).all()