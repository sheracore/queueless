from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.queue_entry import QueueJoinRequest, QueueJoinResponse, QueueEntryResponse, QueueLeaveRequest
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
    status_code=status.HTTP_200_OK
)
def call_next(
        queue_id: int,
        db: Session = Depends(get_db)
):
    return queue_service.call_next(db, queue_id=queue_id)


"""
Why action endpoints instead of PATCH /entries/{id} {"status": 3}? Each action is a business event with its own rules 
and who's allowed to do it. When Kafka arrives, each of these routes becomes one event: 
CustomerServed, CustomerNoShow, CustomerLeft. A generic PATCH would hide that.
"""


@router.post(
    "/entries/{entry_id}/serve",
    response_model=QueueEntryResponse,
)
def serve_entry(
        queue_id: int,
        entry_id: int,
        db: Session = Depends(get_db)
):
    return queue_service.serve(db, queue_id=queue_id, entry_id=entry_id)


@router.post(
    "/entries/{entry_id}/no-show",
    response_model=QueueEntryResponse
)
def mark_no_show(
        queue_id: int,
        entry_id: int,
        db: Session = Depends(get_db)
):
    return queue_service.mark_no_show(db, queue_id=queue_id, entry_id=entry_id)

@router.post(
    "/entries/{entry_id}/leave",
    response_model=QueueEntryResponse,
)
def serve_entry(
        queue_id: int,
        entry_id: int,
        data: QueueLeaveRequest,
        db: Session = Depends(get_db)
):
    return queue_service.leave(db, queue_id=queue_id, entry_id=entry_id, user_id=data.user_id)