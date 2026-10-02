from datetime import datetime, timezone

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models.queue import Queue, QueueStatus
from app.models.queue_entry import QueueEntry, QueueEntryStatus


class QueueService:

    def join_queue(
        self,
        db: Session,
        queue_id: int,
        user_id: int,
    ) -> QueueEntry:

        queue = (
            db.query(Queue)
            .filter(Queue.id == queue_id)
            .with_for_update()
            .first()
        )

        if queue is None:
            raise HTTPException(
                status_code=404,
                detail="Queue not found",
            )

        if queue.status != QueueStatus.OPEN:
            raise HTTPException(
                status_code=409,
                detail="Queue is not open",
            )

        existing_entry = (
            db.query(QueueEntry)
            .filter(
                QueueEntry.queue_id == queue_id,
                QueueEntry.user_id == user_id,
                QueueEntry.status.in_([QueueEntryStatus.WAITING, QueueEntryStatus.CALLED]),
            )
            .first()
        )

        if existing_entry:
            raise HTTPException(
                status_code=409,
                detail="User is already in this queue",
            )

        ticket_number = queue.next_ticket_number

        queue.next_ticket_number += 1

        entry = QueueEntry(
            queue_id=queue_id,
            user_id=user_id,
            ticket_number=ticket_number,
            status=QueueEntryStatus.WAITING,
            joined_at=datetime.now(timezone.utc),
        )

        db.add(entry)
        db.commit() # Queue changes will be commited automatically
        db.refresh(entry)

        return entry

    def call_next(
            self,
            db: Session,
            queue_id: int
    ) -> QueueEntry:
        # At the moment of locking, we don't yet know which entry we'll pick.
        # The queue row is the natural single point of serialization for everything that changes the queue's state.
        # Row lock: concurrent "call next" requests run one after the other.
        # TODO: It isn't efficient way for high traffic queues, change to  QueueEntry lock with skip_locked=True next
        queue = (
            db.query(Queue)
            .filter(Queue.id == queue_id)
            .with_for_update()
            .first()
        )
        if queue is None:
            raise HTTPException(status_code=404, detail="Queue not found")

        entry = (
            db.query(QueueEntry)
            .filter(
                QueueEntry.queue_id == queue_id,
                QueueEntry.status == QueueEntryStatus.WAITING,
            )
            .order_by(QueueEntry.ticket_number)
            .first()
        )
        if entry is None:
            raise HTTPException(status_code=409, detail="No customers are waiting")

        entry.status = QueueEntryStatus.CALLED
        entry.called_at = datetime.now(timezone.utc)

        db.commit()
        db.refresh(entry)
        return entry
