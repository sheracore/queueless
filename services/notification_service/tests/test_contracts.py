# services/notification_service/tests/test_contracts.py
"""Consumer contract tests: the Queue Service's published examples must work with OUR contract."""
import json
from pathlib import Path

import pytest

from app.events import IncomingEvent
from app.handlers import MESSAGES, handle_event

REPO = next(p for p in Path(__file__).resolve().parents if (p / "contracts").is_dir())
QUEUE_EVENTS = REPO / "contracts" / "queue.events"


@pytest.mark.parametrize("event_type", sorted(MESSAGES))
def test_every_event_we_notify_about_is_published(event_type):
    assert (QUEUE_EVENTS / f"{event_type}.schema.json").exists()


@pytest.mark.parametrize("event_type", sorted(MESSAGES))
def test_we_can_read_the_published_examples(event_type, db, sender):
    example = json.loads((QUEUE_EVENTS / f"{event_type}.example.json").read_text())
    notification = handle_event(db, IncomingEvent.model_validate(example), sender)
    assert notification is not None and notification.status == "SENT"