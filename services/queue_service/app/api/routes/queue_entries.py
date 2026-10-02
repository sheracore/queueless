from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.queue_entry import QueueJoinRequest, QueueJoinResponse, QueueEntryResponse
from app.services.queue_service import QueueService

router = APIRouter(
    prefix="/queues/{queue_id}",
    tags=["queue entries"]
)

queue_service = QueueService()


@router.post(
    "/join",
    response_model=QueueJoinResponse,
    status_code=status.HTTP_201_CREATED
)
def join_queue(
        queue_id: int,
        data: QueueJoinRequest,
        db: Session = Depends(get_db)
):
    return queue_service.join_queue(db, queue_id=queue_id, user_id=data.user_id)

@router.post(
    "/call-next",
    response_model=QueueEntryResponse,
    status_code=status.HTTP_201_CREATED
)
def call_next(
        queue_id: int,
        db: Session = Depends(get_db)
):
    return queue_service.call_next(db, queue_id=queue_id)
