import json
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from app.events import IncomingEvent
from app.handlers import handle_event
from app.models import Notification


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


def test_called_event_creates_notification(db):
    n = handle_event(db, make_event("CustomerCalled", user_id=42, ticket_number=43))
    assert n is not None
    assert n.user_id == 42
    assert "#43" in n.message
    assert count(db) == 1


def test_same_event_twice_creates_one_notification(db):
    event = make_event()
    handle_event(db, event)
    assert handle_event(db, event) is None  # duplicate delivery from Kafka
    assert count(db) == 1


def test_events_we_do_not_care_about_are_ignored(db):
    for event_type in ("CustomerServed", "CustomerLeftQueue", "CustomerMarkedNoShow"):
        assert handle_event(db, make_event(event_type)) is None
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


def test_api_lists_notifications_for_a_user(client, db):
    handle_event(db, make_event("CustomerJoinedQueue", user_id=7, ticket_number=5))
    handle_event(db, make_event("CustomerCalled", user_id=7, ticket_number=5))
    handle_event(db, make_event("CustomerCalled", user_id=8, ticket_number=6))
    body = client.get("/users/7/notifications").json()
    assert [n["event_type"] for n in body] == ["CustomerCalled", "CustomerJoinedQueue"]