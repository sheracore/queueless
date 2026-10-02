from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.business import Business
from app.models.queue import Queue, QueueStatus
from app.schemas.queue import QueueCreate, QueueResponse

router = APIRouter(
    prefix="/businesses/{business_id}/queues",
    tags=["queues"],
)

@router.post(
    "",
    response_model=QueueResponse,
)
def create_queue(
        business_id: int,
        data: QueueCreate,
        db: Session = Depends(get_db),
):
    if db.get(Business, business_id) is None:
        raise HTTPException(status_code=404, detail="Business not found")

    queue = Queue(
        name=data.name,
        business_id=business_id,
        status=QueueStatus.OPEN,
        created_at=datetime.now(timezone.utc),
    )

    db.add(queue)
    db.commit()
    db.refresh(queue)

    return queue