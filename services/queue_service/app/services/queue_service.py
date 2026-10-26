from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fastapi import HTTPException
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.config import settings
from app.models.queue import Queue, QueueStatus
from app.models.queue_entry import QueueEntry, QueueEntryStatus
from app.events.envelope import EventType, entry_event
from app.models.outbox_event import OutboxEvent


# The state machine of a ticket: current status -> statuses it may move to.
ALLOWED_TRANSITIONS: dict[QueueEntryStatus, set[QueueEntryStatus]] = {
    QueueEntryStatus.WAITING: {QueueEntryStatus.CALLED, QueueEntryStatus.LEFT},
    QueueEntryStatus.CALLED: {QueueEntryStatus.SERVED, QueueEntryStatus.NO_SHOW},
# Deposit saga: RESERVED waits for the payment result.
    QueueEntryStatus.RESERVED: {QueueEntryStatus.WAITING, QueueEntryStatus.CANCELLED},
    QueueEntryStatus.CANCELLED: set(),
    QueueEntryStatus.SERVED: set(),
    QueueEntryStatus.LEFT: set(),
    QueueEntryStatus.NO_SHOW: set(),
}
ACTIVE_STATUSES = [QueueEntryStatus.WAITING, QueueEntryStatus.CALLED, QueueEntryStatus.RESERVED]

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

    def _record_event(
            self,
            db: Session,
            event_type: EventType,
            entry: QueueEntry,
            deposit_amount: Decimal | None = None,
            reserved_until: datetime | None = None,
            reason: str | None = None,
    ) -> None:
        # Transactional outbox: we do NOT talk to Kafka here. We add the event to the
        # outbox table in the SAME transaction as the state change. The next db.commit()
        # saves both together, or neither. The outbox relay sends it to Kafka later.
        event = entry_event(event_type, entry, deposit_amount, reserved_until, reason)
        db.add(OutboxEvent(
            event_id=event.event_id,
            event_type=event.event_type.value,
            topic=settings.queue_events_topic,
            message_key=str(entry.queue_id),  # key = queue_id: per-queue ordering on Kafka
            payload=event.model_dump(mode="json"),
            created_at=event.occurred_at,
        ))

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
                QueueEntry.status.in_(ACTIVE_STATUSES),
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

        now = datetime.now(timezone.utc)
        needs_deposit = queue.deposit_amount > 0
        entry = QueueEntry(
            queue_id=queue_id,
            user_id=user_id,
            ticket_number=ticket_number,
            # A deposit queue does not admit the customer yet: it STARTS the saga.
            status=QueueEntryStatus.RESERVED if needs_deposit else QueueEntryStatus.WAITING,
            joined_at=now,
            reserved_until=now + timedelta(seconds=settings.reservation_timeout_seconds) if needs_deposit else None,
        )

        db.add(entry)
        db.flush()

        if needs_deposit:
            # Saga step 1. The Payment Service reacts to this event.
            self._record_event(db, EventType.TICKET_RESERVED, entry, deposit_amount=queue.deposit_amount)
        else:
            self._record_event(db, EventType.CUSTOMER_JOINED_QUEUE, entry)
        db.commit()  # ONE commit: queue counter + new entry + outbox event
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

        self._record_event(db, EventType.CUSTOMER_CALLED, entry)
        db.commit()
        db.refresh(entry)
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

        self._record_event(db, TRANSITION_EVENTS[target], entry)
        db.commit()
        db.refresh(entry)
        return entry

    def serve(self, db: Session, queue_id: int, entry_id: int) -> QueueEntry:
        return self._transition(db, queue_id, entry_id, QueueEntryStatus.SERVED)

    def mark_no_show(self, db: Session, queue_id: int, entry_id: int) -> QueueEntry:
        return self._transition(db, queue_id, entry_id, QueueEntryStatus.NO_SHOW)

    def leave(self, db: Session, queue_id: int, entry_id: int, user_id: int) -> QueueEntry:
        return self._transition(db, queue_id, entry_id, QueueEntryStatus.LEFT, user_id=user_id)

    # ------------------------------------------------------------------
    # Deposit saga: called by the saga worker, never by HTTP.
    # Each method is idempotent: it uses compare-and-set, so a duplicate or late
    # message finds the entry in another status and changes nothing.
    # ------------------------------------------------------------------

    def _move(self, db: Session, entry_id: int, target: QueueEntryStatus) -> QueueEntry | None:
        return db.execute(
            update(QueueEntry)
            .where(QueueEntry.id == entry_id, QueueEntry.status.in_(allowed_sources(target)))
            .values(status=target, reserved_until=None)
            .returning(QueueEntry)
        ).scalar_one_or_none()

    def confirm_reservation(self, db: Session, entry_id: int) -> bool:
        """Payment succeeded: RESERVED -> WAITING. The customer is now really in the queue."""
        entry = self._move(db, entry_id, QueueEntryStatus.WAITING)
        if entry is None:
            db.rollback()  # not RESERVED any more (duplicate message, or already cancelled)
            return False
        self._record_event(db, EventType.CUSTOMER_JOINED_QUEUE, entry)
        db.commit()
        return True

    def cancel_reservation(self, db: Session, entry_id: int, reason: str) -> bool:
        """Compensation: RESERVED -> CANCELLED, and tell the other services why."""
        entry = self._move(db, entry_id, QueueEntryStatus.CANCELLED)
        if entry is None:
            db.rollback()
            return False
        self._record_event(db, EventType.TICKET_RESERVATION_CANCELLED, entry, reason=reason)
        db.commit()
        return True

    def expire_reservations(self, db: Session, now: datetime | None = None, limit: int = 100) -> int:
        """Saga timeout: cancel RESERVED entries whose deadline has passed."""
        now = now or datetime.now(timezone.utc)
        expired_ids = db.scalars(
            select(QueueEntry.id)
            .where(QueueEntry.status == QueueEntryStatus.RESERVED, QueueEntry.reserved_until < now)
            .order_by(QueueEntry.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        ).all()
        cancelled = 0
        for entry_id in expired_ids:
            entry = self._move(db, entry_id, QueueEntryStatus.CANCELLED)
            if entry is not None:
                self._record_event(db, EventType.TICKET_RESERVATION_CANCELLED, entry, reason="payment_timeout")
                cancelled += 1
        db.commit()
        return cancelled