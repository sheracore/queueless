import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.events.contracts import IncomingEvent, payment_outbox_event
from app.gateway import PaymentGateway
from app.models import Payment, PaymentStatus

logger = logging.getLogger(__name__)


def _find(db: Session, queue_entry_id: int) -> Payment | None:
    return db.scalar(select(Payment).where(Payment.queue_entry_id == queue_entry_id))


def charge_deposit(db: Session, event: IncomingEvent, gateway: PaymentGateway) -> str:
    """Saga step 2 (TicketReserved): charge the deposit, then publish the result."""
    data = event.data
    payment = _find(db, data.queue_entry_id)
    if payment is None:
        now = datetime.now(timezone.utc)
        payment = Payment(queue_entry_id=data.queue_entry_id, queue_id=data.queue_id, user_id=data.user_id,
                          amount=data.deposit_amount or 0, status=PaymentStatus.PENDING,
                          created_at=now, updated_at=now)
        db.add(payment)
        try:
            db.commit()   # save PENDING BEFORE calling the gateway: we never charge without a record
        except IntegrityError:
            db.rollback()
            payment = _find(db, data.queue_entry_id)

    if payment.status != PaymentStatus.PENDING:
        return f"already {payment.status}"   # duplicate TicketReserved

    # May raise GatewayUnavailableError -> the worker retries this same message later.
    # The idempotency key makes that retry safe: the gateway never charges twice.
    result = gateway.charge(payment.amount, payment.user_id, idempotency_key=f"deposit-{payment.queue_entry_id}")

    payment.status = PaymentStatus.SUCCEEDED if result.approved else PaymentStatus.FAILED
    payment.failure_reason = result.reason
    payment.updated_at = datetime.now(timezone.utc)
    if result.approved:
        db.add(payment_outbox_event("PaymentSucceeded", payment))
    else:
        db.add(payment_outbox_event("PaymentFailed", payment, reason=result.reason))
    db.commit()   # ONE commit: new status + outbox event
    return payment.status


def refund_if_charged(db: Session, event: IncomingEvent, gateway: PaymentGateway) -> str:
    """Compensation (TicketReservationCancelled): give the money back if we took it."""
    payment = _find(db, event.data.queue_entry_id)
    if payment is None or payment.status != PaymentStatus.SUCCEEDED:
        return "nothing to refund"   # never charged, declined, or already refunded

    gateway.refund(payment.amount, idempotency_key=f"refund-{payment.queue_entry_id}")

    payment.status = PaymentStatus.REFUNDED
    payment.updated_at = datetime.now(timezone.utc)
    db.add(payment_outbox_event("PaymentRefunded", payment, reason=event.data.reason))
    db.commit()
    return payment.status


HANDLERS = {
    "TicketReserved": charge_deposit,
    "TicketReservationCancelled": refund_if_charged,
}


def handle_event(db: Session, event: IncomingEvent, gateway: PaymentGateway) -> str:
    handler = HANDLERS.get(event.event_type)
    if handler is None:
        return "ignored"   # most queue.events are not about payments
    return handler(db, event, gateway)