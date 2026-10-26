import uuid

import pytest

from app.events.contracts import IncomingEvent
from app.gateway import FakePaymentGateway, GatewayUnavailableError
from app.handlers import handle_event
from app.models import Payment, PaymentStatus


def queue_event(event_type: str, entry_id: int = 12, **data) -> IncomingEvent:
    return IncomingEvent.model_validate({
        "event_id": str(uuid.uuid4()), "event_type": event_type, "event_version": 1,
        "data": {"queue_entry_id": entry_id, "queue_id": 3, "user_id": 42, "ticket_number": 7, **data},
    })


def reserved(entry_id: int = 12) -> IncomingEvent:
    return queue_event("TicketReserved", entry_id, deposit_amount=50_000)


def cancelled(entry_id: int = 12, reason: str = "payment_timeout") -> IncomingEvent:
    return queue_event("TicketReservationCancelled", entry_id, reason=reason)


def test_approved_charge_publishes_payment_succeeded(db, gateway, outbox):
    assert handle_event(db, reserved(), gateway) == PaymentStatus.SUCCEEDED
    [row] = outbox()
    assert row.event_type == "PaymentSucceeded"
    assert row.topic == "payment.events"
    assert row.message_key == "3"                       # queue_id, like queue.events
    assert row.payload["data"]["amount"] == 50_000
    assert gateway.charges == {"deposit-12": gateway.charges["deposit-12"]}


def test_declined_charge_publishes_payment_failed(db, outbox):
    declining = FakePaymentGateway(decline_rate=1.0)
    assert handle_event(db, reserved(), declining) == PaymentStatus.FAILED
    [row] = outbox()
    assert (row.event_type, row.payload["data"]["reason"]) == ("PaymentFailed", "card_declined")


def test_duplicate_reserved_event_charges_once(db, gateway, outbox):
    handle_event(db, reserved(), gateway)
    assert handle_event(db, reserved(), gateway) == "already SUCCEEDED"
    assert len(gateway.charges) == 1
    assert len(outbox()) == 1


def test_gateway_outage_keeps_pending_and_retry_charges_once(db, outbox):
    flaky = FakePaymentGateway(failure_rate=1.0)
    with pytest.raises(GatewayUnavailableError):
        handle_event(db, reserved(), flaky)
    assert db.query(Payment).one().status == PaymentStatus.PENDING
    assert outbox() == []

    flaky.failure_rate = 0.0            # gateway is back; the worker retries the same message
    assert handle_event(db, reserved(), flaky) == PaymentStatus.SUCCEEDED
    assert len(flaky.charges) == 1


def test_cancel_after_success_refunds_once(db, gateway, outbox):
    handle_event(db, reserved(), gateway)
    assert handle_event(db, cancelled(), gateway) == PaymentStatus.REFUNDED
    assert handle_event(db, cancelled(), gateway) == "nothing to refund"   # duplicate
    assert gateway.refunds == {"refund-12"}
    assert [r.event_type for r in outbox()] == ["PaymentSucceeded", "PaymentRefunded"]


def test_cancel_after_decline_does_not_refund(db, outbox):
    declining = FakePaymentGateway(decline_rate=1.0)
    handle_event(db, reserved(), declining)
    assert handle_event(db, cancelled(reason="payment_declined"), declining) == "nothing to refund"
    assert declining.refunds == set()
    assert [r.event_type for r in outbox()] == ["PaymentFailed"]


def test_other_queue_events_are_ignored(db, gateway, outbox):
    assert handle_event(db, queue_event("CustomerCalled"), gateway) == "ignored"
    assert outbox() == []