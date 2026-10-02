from datetime import datetime
from enum import IntEnum

from sqlalchemy import DateTime, ForeignKey, Index, Integer, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class QueueEntryStatus(IntEnum):
    WAITING = 1
    CALLED = 2
    SERVED = 3
    LEFT = 4
    NO_SHOW = 5


class QueueEntry(Base):
    __tablename__ = "queue_entries"

    __table_args__ = (
        # A ticket number is unique inside one queue.
        UniqueConstraint("queue_id", "ticket_number", name="uq_queue_entries_queue_ticket"),
        # One ACTIVE entry (WAITING=1 / CALLED=2) per user per queue.
        Index(
            "uq_queue_entries_active_user",
            "queue_id", "user_id",
            unique=True,
            postgresql_where=text("status IN (1, 2)"),
        ),
    )
    id: Mapped[int] = mapped_column(primary_key=True)

    queue_id: Mapped[int] = mapped_column(
        ForeignKey("queues.id"),
        nullable=False,
    )

    user_id: Mapped[int] = mapped_column(
        nullable=False,
    )

    ticket_number: Mapped[int] = mapped_column(
        nullable=False,
    )

    status: Mapped[QueueEntryStatus] = mapped_column(
        Integer,
        default=QueueEntryStatus.WAITING,
        nullable=False,
    )

    joined_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    called_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
    )

    served_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
    )

    queue = relationship(
        "Queue",
        back_populates="entries",
    )
