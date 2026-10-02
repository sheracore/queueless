from datetime import datetime
from enum import IntEnum

from sqlalchemy import DateTime, ForeignKey, String, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class QueueStatus(IntEnum):
    OPEN = 0
    PAUSED = 1
    CLOSED = 2


class Queue(Base):
    __tablename__ = "queues"

    id: Mapped[int] = mapped_column(primary_key=True)

    business_id: Mapped[int] = mapped_column(
        ForeignKey("businesses.id"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[QueueStatus] = mapped_column(
        Integer,
        default=QueueStatus.CLOSED,
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    next_ticket_number: Mapped[int] = mapped_column(
        nullable=False,
        default=1,
    )

    business = relationship(
        "Business",
        back_populates="queues",
    )

    entries = relationship(
        "QueueEntry",
        back_populates="queue",
    )
