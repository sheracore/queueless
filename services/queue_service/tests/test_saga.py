from datetime import datetime, timedelta, timezone

from app.events.envelope import EventType
from app.models.queue_entry import QueueEntry, QueueEntryStatus
from app.saga_worker import PaymentEvent, handle_payment_event
from app.services.queue_service import QueueService

service = QueueService()


def _deposit_queue(client, amount=50_000):
    business = client.post("/businesses", json={"name": "City Clinic"}).json()
    return client.post(f"/businesses/{business['id']}/queues",
                       json={"name": "Dentist", "deposit_amount": amount}).json()


def _payment_event(event_type, entry_id, queue_id=1, amount=50_000):
    return PaymentEvent.model_validate({
        "event_id": "e-1", "event_type": event_type, "event_version": 1,
        "data": {"queue_entry_id": entry_id, "queue_id": queue_id, "amount": amount},
    })


def test_free_queue_still_joins_directly(client, outbox):
    business = client.post("/businesses", json={"name": "Joe"}).json()
    q = client.post(f"/businesses/{business['id']}/queues", json={"name": "Haircut"}).json()
    assert q["deposit_amount"] == 0
    entry = client.post(f"/queues/{q['id']}/join", json={"user_id": 1}).json()
    assert entry["status"] == QueueEntryStatus.WAITING
    assert [r.event_type for r in outbox()] == [EventType.CUSTOMER_JOINED_QUEUE]


def test_deposit_queue_reserves_and_starts_the_saga(client, db, outbox):
    q = _deposit_queue(client)
    entry = client.post(f"/queues/{q['id']}/join", json={"user_id": 1}).json()

    assert entry["status"] == QueueEntryStatus.RESERVED
    [row] = outbox()
    assert row.event_type == EventType.TICKET_RESERVED
    assert row.payload["data"]["deposit_amount"] == 50_000
    stored = db.get(QueueEntry, entry["id"])
    assert stored.reserved_until is not None


def test_reserved_customer_cannot_join_twice_and_is_not_called(client):
    q = _deposit_queue(client)
    client.post(f"/queues/{q['id']}/join", json={"user_id": 1})
    assert client.post(f"/queues/{q['id']}/join", json={"user_id": 1}).status_code == 409
    assert client.post(f"/queues/{q['id']}/call-next").status_code == 409  # nobody WAITING yet


def test_payment_succeeded_confirms_once(client, db, outbox):
    q = _deposit_queue(client)
    entry = client.post(f"/queues/{q['id']}/join", json={"user_id": 1}).json()
    event = _payment_event("PaymentSucceeded", entry["id"], q["id"])

    assert handle_payment_event(db, service, event) == "confirmed"
    assert handle_payment_event(db, service, event).startswith("ignored")  # duplicate message
    db.expire_all()  # forget cached objects: always read fresh rows
    assert db.get(QueueEntry, entry["id"]).status == QueueEntryStatus.WAITING
    assert [r.event_type for r in outbox()] == [EventType.TICKET_RESERVED, EventType.CUSTOMER_JOINED_QUEUE]


def test_payment_failed_compensates(client, db, outbox):
    q = _deposit_queue(client)
    entry = client.post(f"/queues/{q['id']}/join", json={"user_id": 1}).json()

    assert handle_payment_event(db, service, _payment_event("PaymentFailed", entry["id"], q["id"])) == "cancelled"
    db.expire_all()
    assert db.get(QueueEntry, entry["id"]).status == QueueEntryStatus.CANCELLED
    last = outbox()[-1]
    assert last.event_type == EventType.TICKET_RESERVATION_CANCELLED
    assert last.payload["data"]["reason"] == "payment_declined"
    # The user may try again: CANCELLED is not an active status.
    assert client.post(f"/queues/{q['id']}/join", json={"user_id": 1}).status_code == 201


def test_late_success_after_cancel_changes_nothing(client, db, outbox):
    q = _deposit_queue(client)
    entry = client.post(f"/queues/{q['id']}/join", json={"user_id": 1}).json()
    service.cancel_reservation(db, entry["id"], reason="payment_timeout")

    result = handle_payment_event(db, service, _payment_event("PaymentSucceeded", entry["id"], q["id"]))
    assert result.startswith("ignored")
    db.expire_all()
    assert db.get(QueueEntry, entry["id"]).status == QueueEntryStatus.CANCELLED
    # No CustomerJoinedQueue: the Payment Service refunds when it sees the cancellation.
    assert EventType.CUSTOMER_JOINED_QUEUE not in [r.event_type for r in outbox()]


def test_expire_cancels_only_overdue_reservations(client, db, outbox):
    q = _deposit_queue(client)
    old = client.post(f"/queues/{q['id']}/join", json={"user_id": 1}).json()
    fresh = client.post(f"/queues/{q['id']}/join", json={"user_id": 2}).json()
    db.get(QueueEntry, old["id"]).reserved_until = datetime.now(timezone.utc) - timedelta(seconds=1)
    db.commit()

    assert service.expire_reservations(db) == 1
    db.expire_all()
    assert db.get(QueueEntry, old["id"]).status == QueueEntryStatus.CANCELLED
    assert db.get(QueueEntry, fresh["id"]).status == QueueEntryStatus.RESERVED
    last = outbox()[-1]
    assert (last.event_type, last.payload["data"]["reason"]) == (EventType.TICKET_RESERVATION_CANCELLED, "payment_timeout")
    assert service.expire_reservations(db) == 0  # already cancelled: nothing to do


def test_refund_event_is_not_a_queue_step(client, db):
    q = _deposit_queue(client)
    entry = client.post(f"/queues/{q['id']}/join", json={"user_id": 1}).json()
    assert handle_payment_event(db, service, _payment_event("PaymentRefunded", entry["id"])).startswith("ignored")