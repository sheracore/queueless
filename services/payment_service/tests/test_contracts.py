# services/payment_service/tests/test_contracts.py
"""Contract tests for the Payment Service: as a producer (payment.events) and a consumer (queue.events)."""
import json
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from app.events.catalog import EVENT_MODELS
from app.events.contracts import IncomingEvent
from app.gateway import FakePaymentGateway
from app.handlers import HANDLERS, handle_event

REPO = next(p for p in Path(__file__).resolve().parents if (p / "contracts").is_dir())
CONTRACTS = REPO / "contracts"


def load(topic: str, name: str, kind: str) -> dict:
    return json.loads((CONTRACTS / topic / f"{name}.{kind}.json").read_text())


# ---------- producer: payment.events ----------

def test_contracts_folder_matches_the_catalog():
    result = subprocess.run([sys.executable, str(REPO / "tools" / "schemas.py"), "check", "--service", "."],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout


@pytest.mark.parametrize("event_type", [t.value for t in EVENT_MODELS])
def test_examples_match_their_schema(event_type):
    schema = load("payment.events", event_type, "schema")
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(load("payment.events", event_type, "example"))


def test_real_events_match_the_published_schemas(db, outbox):
    def queue_event(event_type, entry_id, **data):
        return IncomingEvent.model_validate({
            "event_id": str(uuid.uuid4()), "event_type": event_type, "event_version": 1,
            "data": {"queue_entry_id": entry_id, "queue_id": 3, "user_id": 42, "ticket_number": 7, **data}})

    handle_event(db, queue_event("TicketReserved", 1, deposit_amount=50_000), FakePaymentGateway())
    handle_event(db, queue_event("TicketReservationCancelled", 1, reason="payment_timeout"), FakePaymentGateway())
    handle_event(db, queue_event("TicketReserved", 2, deposit_amount=50_000), FakePaymentGateway(decline_rate=1.0))

    rows = outbox()
    assert {r.event_type for r in rows} == {t.value for t in EVENT_MODELS}
    for row in rows:
        schema = load("payment.events", row.event_type, "schema")
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(row.payload)


# ---------- consumer: queue.events ----------

@pytest.mark.parametrize("event_type", sorted(HANDLERS))
def test_every_event_we_handle_is_published_by_the_queue_service(event_type):
    # If the Queue Service renames or removes an event we depend on, this fails in OUR CI.
    assert (CONTRACTS / "queue.events" / f"{event_type}.schema.json").exists()


@pytest.mark.parametrize("event_type", sorted(HANDLERS))
def test_we_can_read_the_published_examples(event_type, db, gateway):
    event = IncomingEvent.model_validate(load("queue.events", event_type, "example"))
    handle_event(db, event, gateway)   # must not raise