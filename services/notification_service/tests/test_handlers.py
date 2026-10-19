import json
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from app.events import IncomingEvent
from app.handlers import handle_event
from app.models import Notification
from app.channels import SmsProviderError
from app.models import Notification, NotificationStatus


def make_event(event_type="CustomerCalled", user_id=42, ticket_number=43, event_id=None) -> IncomingEvent:
    # The same JSON shape the queue service puts on Kafka.
    raw = {
        "event_id": str(event_id or uuid4()),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": "2026-10-04T09:02:00Z",
        "producer": "queue-service",
        "data": {
            "queue_entry_id": 12,
            "queue_id": 1,
            "user_id": user_id,
            "ticket_number": ticket_number,
            "status": "CALLED",
            "joined_at": "2026-10-04T08:40:00Z",
            "called_at": "2026-10-04T09:02:00Z",
            "served_at": None,
        },
    }
    return IncomingEvent.model_validate_json(json.dumps(raw))


def count(db) -> int:
    return db.scalar(select(func.count()).select_from(Notification))


def test_called_event_creates_notification(db, sender):
    n = handle_event(db, make_event("CustomerCalled", user_id=42, ticket_number=43), sender)
    assert n is not None
    assert n.user_id == 42
    assert "#43" in n.message
    assert count(db) == 1


def test_same_event_twice_creates_one_notification(db, sender):
    event = make_event()
    handle_event(db, event, sender)
    assert handle_event(db, event, sender) is None  # duplicate delivery from Kafka
    assert count(db) == 1


def test_events_we_do_not_care_about_are_ignored(db, sender):
    for event_type in ("CustomerServed", "CustomerLeftQueue", "CustomerMarkedNoShow"):
        assert handle_event(db, make_event(event_type), sender) is None
    assert count(db) == 0


def test_unknown_extra_fields_are_accepted():
    # The producer may add new fields later; old consumers must not break.
    event = make_event()
    raw = json.loads(event.model_dump_json())
    raw["data"]["new_field_added_later"] = "x"
    IncomingEvent.model_validate_json(json.dumps(raw))


def test_message_missing_required_field_is_rejected():
    with pytest.raises(ValidationError):
        IncomingEvent.model_validate_json('{"event_type": "CustomerCalled"}')


def test_api_lists_notifications_for_a_user(client, db, sender):
    handle_event(db, make_event("CustomerJoinedQueue", user_id=7, ticket_number=5), sender)
    handle_event(db, make_event("CustomerCalled", user_id=7, ticket_number=5), sender)
    handle_event(db, make_event("CustomerCalled", user_id=8, ticket_number=6), sender)
    body = client.get("/users/7/notifications").json()
    assert [n["event_type"] for n in body] == ["CustomerCalled", "CustomerJoinedQueue"]


def test_sms_is_sent_once_and_marked_sent(db, sender):
    event = make_event("CustomerCalled", user_id=42, ticket_number=43)
    n = handle_event(db, event, sender)
    assert n.status == NotificationStatus.SENT
    assert n.sent_at is not None
    assert sender.sent == [(42, n.message, str(event.event_id))]  # event_id = idempotency key


def test_provider_failure_keeps_notification_pending(db, sender):
    sender.fail = True
    with pytest.raises(SmsProviderError):
        handle_event(db, make_event(), sender)
    [n] = db.scalars(select(Notification)).all()
    assert n.status == NotificationStatus.PENDING
    assert sender.sent == []


def test_retry_after_failure_sends_exactly_once(db, sender):
    event = make_event()
    sender.fail = True
    with pytest.raises(SmsProviderError):
        handle_event(db, event, sender)

    sender.fail = False                      # the provider is back (the retry)
    n = handle_event(db, event, sender)
    assert n.status == NotificationStatus.SENT
    assert len(sender.sent) == 1
    assert count(db) == 1                    # still one row, not two

    assert handle_event(db, event, sender) is None   # a later duplicate: nothing sent
    assert len(sender.sent) == 1