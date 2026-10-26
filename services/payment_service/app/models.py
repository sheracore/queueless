import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import JSON, BigInteger, DateTime, Index, Integer, String, Uuid, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class PaymentStatus(StrEnum):
    PENDING = "PENDING"        # row saved, gateway result not known yet
    SUCCEEDED = "SUCCEEDED"    # deposit charged
    FAILED = "FAILED"          # gateway declined the card
    REFUNDED = "REFUNDED"      # charged, then given back (saga compensation)


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[int] = mapped_column(primary_key=True)
    # One deposit per ticket. UNIQUE = the idempotency guarantee of this service.
    queue_entry_id: Mapped[int] = mapped_column(unique=True, nullable=False)
    queue_id: Mapped[int] = mapped_column(nullable=False)
    user_id: Mapped[int] = mapped_column(nullable=False)
    amount: Mapped[int] = mapped_column(nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=PaymentStatus.PENDING)
    failure_reason: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class OutboxEvent(Base):
    """Same pattern as the Queue Service: events are saved with the change, sent later."""

    __tablename__ = "outbox_events"

    __table_args__ = (
        Index(
            "ix_outbox_events_unpublished",
            "id",
            postgresql_where=text("published_at IS NULL"),
            sqlite_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(Uuid, unique=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    message_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON().with_variant(JSONB, "postgresql"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))