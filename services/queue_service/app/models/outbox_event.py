import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, BigInteger, DateTime, Index, Integer, String, Uuid, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class OutboxEvent(Base):
    """An event waiting to be sent to Kafka.

    It is written in the SAME database transaction as the business change,
    so the change and the event are saved together, or not at all.
    """

    __tablename__ = "outbox_events"

    __table_args__ = (
        # The relay only looks for unpublished rows. A partial index keeps that query
        # fast even when the table holds millions of already-published rows.
        Index(
            "ix_outbox_events_unpublished",
            "id",
            postgresql_where=text("published_at IS NULL"),
            sqlite_where=text("published_at IS NULL"),
        ),
    )

    # BIGINT, always increasing: the relay sends events in this order.
    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(Uuid, unique=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    message_key: Mapped[str] = mapped_column(String(255), nullable=False)
    # The full event envelope, exactly as it will appear on Kafka.
    payload: Mapped[dict[str, Any]] = mapped_column(JSON().with_variant(JSONB, "postgresql"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # NULL = still waiting. Set by the relay after Kafka confirms the message.
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))