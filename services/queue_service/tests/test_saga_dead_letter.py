# services/queue_service/tests/test_saga_dead_letter.py
import json

import pytest

from app.events.dead_letter import PermanentError, parse_or_ignore
from app.saga_worker import SAGA_EVENT_TYPES, SUPPORTED_EVENT_VERSIONS, PaymentEvent


def parse(envelope: dict):
    return parse_or_ignore(json.dumps(envelope).encode(), SAGA_EVENT_TYPES, PaymentEvent, SUPPORTED_EVENT_VERSIONS)


def test_refunds_are_not_a_saga_step_and_are_ignored():
    assert parse({"event_type": "PaymentRefunded"}) is None


def test_a_broken_payment_result_is_parked_not_skipped():
    with pytest.raises(PermanentError, match="PaymentSucceeded does not match our contract"):
        parse({"event_id": "x", "event_type": "PaymentSucceeded", "event_version": 1, "data": {"queue_entry_id": 12}})