import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import DateTime, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class NotificationStatus(StrEnum):
    PENDING = "PENDING"  # saved, but the SMS is not confirmed yet
    SENT = "SENT"        # the SMS provider accepted it


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(primary_key=True)
    # The id of the Kafka event that caused this notification.
    # UNIQUE = the idempotency guarantee: the same event can never create two notifications.
    event_id: Mapped[uuid.UUID] = mapped_column(Uuid, unique=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    user_id: Mapped[int] = mapped_column(nullable=False, index=True)
    queue_id: Mapped[int] = mapped_column(nullable=False)
    queue_entry_id: Mapped[int] = mapped_column(nullable=False)
    ticket_number: Mapped[int] = mapped_column(nullable=False)
    message: Mapped[str] = mapped_column(String(500), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=NotificationStatus.PENDING)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))