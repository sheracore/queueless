from datetime import datetime, timezone

from fastapi import HTTPException
from sqlalchemy import update
from sqlalchemy.orm import Session

from app.config import settings
from app.models.queue import Queue, QueueStatus
from app.models.queue_entry import QueueEntry, QueueEntryStatus
from app.events.envelope import EventType, entry_event
from app.events.publisher import EventPublisher


# The state machine of a ticket: current status -> statuses it may move to.
ALLOWED_TRANSITIONS: dict[QueueEntryStatus, set[QueueEntryStatus]] = {
    QueueEntryStatus.WAITING: {QueueEntryStatus.CALLED, QueueEntryStatus.LEFT},
    QueueEntryStatus.CALLED: {QueueEntryStatus.SERVED, QueueEntryStatus.NO_SHOW},
    QueueEntryStatus.SERVED: set(),
    QueueEntryStatus.LEFT: set(),
    QueueEntryStatus.NO_SHOW: set(),
}


def allowed_sources(target: QueueEntryStatus) -> list[QueueEntryStatus]:
    # The table inverted: "from which statuses can I reach `target`?"
    return [src for src, targets in ALLOWED_TRANSITIONS.items() if target in targets]


# Which fact happened when an entry reaches each status.
TRANSITION_EVENTS: dict[QueueEntryStatus, EventType] = {
    QueueEntryStatus.SERVED: EventType.CUSTOMER_SERVED,
    QueueEntryStatus.NO_SHOW: EventType.CUSTOMER_NO_SHOW,
    QueueEntryStatus.LEFT: EventType.CUSTOMER_LEFT_QUEUE,
}


class QueueService:

    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher

    def _publish(self, event_type: EventType, entry: QueueEntry) -> None:
        # Key = queue_id: all events of one queue land in the same partition, in order.
        self.publisher.publish(
            topic=settings.queue_events_topic,
            key=str(entry.queue_id),
            event=entry_event(event_type, entry),
        )

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

        self._publish(EventType.CUSTOMER_JOINED_QUEUE, entry)
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

        self._publish(EventType.CUSTOMER_CALLED, entry)
        return entry

    def _transition(
            self,
            db: Session,
            queue_id: int,
            entry_id: int,
            target: QueueEntryStatus,
            user_id: int | None = None,
    ) -> QueueEntry:
        values: dict = {"status": target}

        if target == QueueEntryStatus.SERVED:
            values["served_at"] = datetime.now(timezone.utc)

        # Compare-and-set: check + change in ONE atomic statement.
        statement = (
            update(QueueEntry)
            .where(
                QueueEntry.id == entry_id,
                QueueEntry.queue_id == queue_id,
                QueueEntry.status.in_(allowed_sources(target)),
            )
            .values(**values)
            .returning(QueueEntry)
        )
        if user_id is not None:  # customers may only touch their own ticket
            statement = statement.where(QueueEntry.user_id == user_id)

        entry = db.execute(statement).scalar_one_or_none()

        if entry is None:
            # 0 rows matched. Now (and only now) find out WHY, for a good error.
            db.rollback()
            current = db.get(QueueEntry, entry_id)
            if current is None or current.queue_id != queue_id or (
                    user_id is not None and current.user_id != user_id
            ):
                raise HTTPException(status_code=404, detail="Queue entry not found")
            raise HTTPException(
                status_code=409,
                detail=f"Cannot change entry from {QueueEntryStatus(current.status).name} to {target.name}",
            )

        db.commit()
        db.refresh(entry)

        self._publish(TRANSITION_EVENTS[target], entry)
        return entry

    def serve(self, db: Session, queue_id: int, entry_id: int) -> QueueEntry:
        return self._transition(db, queue_id, entry_id, QueueEntryStatus.SERVED)

    def mark_no_show(self, db: Session, queue_id: int, entry_id: int) -> QueueEntry:
        return self._transition(db, queue_id, entry_id, QueueEntryStatus.NO_SHOW)

    def leave(self, db: Session, queue_id: int, entry_id: int, user_id: int) -> QueueEntry:
        return self._transition(db, queue_id, entry_id, QueueEntryStatus.LEFT, user_id=user_id)
