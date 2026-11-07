# services/payment_service/app/events/catalog.py
"""The catalog of events the Payment Service PUBLISHES on payment.events.

This file IS the contract (see the Queue Service's catalog.py for the rules).
Pure Pydantic on purpose: no database or settings imports.
"""
from datetime import datetime, timezone
from enum import StrEnum
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

TOPIC = "payment.events"


class EventType(StrEnum):
    PAYMENT_SUCCEEDED = "PaymentSucceeded"
    PAYMENT_FAILED = "PaymentFailed"
    PAYMENT_REFUNDED = "PaymentRefunded"


class Closed(BaseModel):
    model_config = ConfigDict(extra="forbid")   # -> "additionalProperties": false


class PaymentData(Closed):
    payment_id: int
    queue_entry_id: int
    queue_id: int
    user_id: int
    amount: int = Field(ge=0, description="Smallest currency unit.")
    status: Literal["SUCCEEDED", "FAILED", "REFUNDED"]


class PaymentFailedData(PaymentData):
    reason: str                  # e.g. "card_declined"


class PaymentRefundedData(PaymentData):
    reason: str | None = None    # why the reservation was cancelled, e.g. "payment_timeout"


def _all_fields_required(schema: dict, model: type) -> None:
    # Envelope fields have Python defaults (event_id, occurred_at, ...) for convenience, but they are
    # ALWAYS on the wire and consumers rely on them, so the published schema marks them all required.
    schema["required"] = sorted(schema["properties"])


class EventEnvelope(Closed):
    model_config = ConfigDict(extra="forbid", json_schema_extra=_all_fields_required)

    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    event_version: Literal[1] = 1
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    producer: Literal["payment-service"] = "payment-service"
    data: PaymentData


class PaymentSucceeded(EventEnvelope):
    event_type: Literal[EventType.PAYMENT_SUCCEEDED] = EventType.PAYMENT_SUCCEEDED


class PaymentFailed(EventEnvelope):
    event_type: Literal[EventType.PAYMENT_FAILED] = EventType.PAYMENT_FAILED
    data: PaymentFailedData


class PaymentRefunded(EventEnvelope):
    event_type: Literal[EventType.PAYMENT_REFUNDED] = EventType.PAYMENT_REFUNDED
    data: PaymentRefundedData


EVENT_MODELS: dict[EventType, type[EventEnvelope]] = {
    EventType.PAYMENT_SUCCEEDED: PaymentSucceeded,
    EventType.PAYMENT_FAILED: PaymentFailed,
    EventType.PAYMENT_REFUNDED: PaymentRefunded,
}


def examples() -> dict[str, EventEnvelope]:
    at = datetime(2026, 11, 6, 9, 0, tzinfo=timezone.utc)
    base = dict(payment_id=5, queue_entry_id=12, queue_id=3, user_id=42, amount=50_000)
    return {
        "PaymentSucceeded": PaymentSucceeded(event_id=UUID(int=101), occurred_at=at,
                                             data=PaymentData(**base, status="SUCCEEDED")),
        "PaymentFailed": PaymentFailed(event_id=UUID(int=102), occurred_at=at,
                                       data=PaymentFailedData(**base, status="FAILED", reason="card_declined")),
        "PaymentRefunded": PaymentRefunded(event_id=UUID(int=103), occurred_at=at,
                                           data=PaymentRefundedData(**base, status="REFUNDED", reason="payment_timeout")),
    }