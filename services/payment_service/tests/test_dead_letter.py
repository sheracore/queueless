# services/payment_service/tests/test_dead_letter.py
import json

import pytest

from app.dead_letter import PermanentError, parse_or_ignore
from app.events.contracts import IncomingEvent
from app.handlers import HANDLERS

GOOD = {"event_id": "00000000-0000-0000-0000-000000000006", "event_type": "TicketReserved", "event_version": 1,
        "data": {"queue_entry_id": 12, "queue_id": 3, "user_id": 42, "ticket_number": 7, "deposit_amount": 50_000}}


def parse(envelope) -> IncomingEvent | None:
    raw = envelope if isinstance(envelope, bytes) else json.dumps(envelope).encode()
    return parse_or_ignore(raw, set(HANDLERS), IncomingEvent, {1})


def test_a_valid_event_we_handle_is_parsed():
    assert parse(GOOD).data.deposit_amount == 50_000


def test_an_event_type_we_do_not_handle_is_ignored_even_if_its_shape_is_unknown():
    assert parse({"event_type": "CustomerCalled", "something": "else"}) is None


def test_our_event_with_a_renamed_field_is_a_permanent_error_not_ignored():
    # The old worker treated this as "not ours" and committed it: the deposit was never charged.
    broken = {**GOOD, "data": {k: v for k, v in GOOD["data"].items() if k != "ticket_number"} | {"ticket": 7}}
    with pytest.raises(PermanentError, match=r"TicketReserved does not match our contract \(data.ticket_number\)"):
        parse(broken)


def test_unsupported_version_is_a_permanent_error():
    with pytest.raises(PermanentError, match="unsupported event_version 2"):
        parse({**GOOD, "event_version": 2})


def test_not_json_is_a_permanent_error():
    with pytest.raises(PermanentError, match="not valid JSON"):
        parse(b"this is not json")