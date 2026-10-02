from datetime import datetime
from app.database import Base

from sqlalchemy import String, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship

class Business(Base):
    __tablename__ = "businesses"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    queues = relationship(
        "Queue",
        back_populates="business",
    )
