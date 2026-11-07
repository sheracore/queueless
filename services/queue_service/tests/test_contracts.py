import json
import subprocess
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from app.events.catalog import EVENT_MODELS
from app.saga_worker import PaymentEvent, handle_payment_event
from app.services.queue_service import QueueService

REPO = next(p for p in Path(__file__).resolve().parents if (p / "contracts").is_dir())
CONTRACTS = REPO / "contracts"

def validator(topic: str, event_type: str) -> Draft202012Validator:
    schema = json.loads((CONTRACTS / topic / f"{event_type}.schema.json").read_text())
    return Draft202012Validator(schema, format_checker=FormatChecker())

def test_contracts_folder_matches_the_catalog():
    # Fails when someone changes catalog.py but forgets to run `tools/schemas.py export`.
    result = subprocess.run([sys.executable, str(REPO / "tools" / "schemas.py"), "check", "--service", "."],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout

@pytest.mark.parametrize("event_type", [t.value for t in EVENT_MODELS])
def test_examples_match_their_schema(event_type):
    example = json.loads((CONTRACTS / "queue.events" / f"{event_type}.example.json").read_text())
    validator("queue.events", event_type).validate(example)

def test_real_events_match_the_published_schemas(client, db, outbox):
    """Run the whole lifecycle through the API and the saga, then validate every outbox payload."""
    business = client.post("/businesses", json={"name": "Clinic"}).json()
    free = client.post(f"/businesses/{business['id']}/queues", json={"name": "Walk-in"}).json()
    paid = client.post(f"/businesses/{business['id']}/queues", json={"name": "Dentist", "deposit_amount": 50_000}).json()

    a = client.post(f"/queues/{free['id']}/join", json={"user_id": 1}).json()
    b = client.post(f"/queues/{free['id']}/join", json={"user_id": 2}).json()
    client.post(f"/queues/{free['id']}/call-next")
    client.post(f"/queues/{free['id']}/entries/{a['id']}/serve")
    client.post(f"/queues/{free['id']}/entries/{b['id']}/leave", json={"user_id": 2})
    c = client.post(f"/queues/{free['id']}/join", json={"user_id": 3}).json()
    client.post(f"/queues/{free['id']}/call-next")
    client.post(f"/queues/{free['id']}/entries/{c['id']}/no-show")
    r1 = client.post(f"/queues/{paid['id']}/join", json={"user_id": 4}).json()
    r2 = client.post(f"/queues/{paid['id']}/join", json={"user_id": 5}).json()
    service = QueueService()
    for event_type, entry in (("PaymentSucceeded", r1), ("PaymentFailed", r2)):
        handle_payment_event(db, service, PaymentEvent.model_validate({
            "event_id": "x", "event_type": event_type, "event_version": 1,
            "data": {"queue_entry_id": entry["id"], "queue_id": paid["id"], "amount": 50_000}}))

    rows = outbox()
    assert {row.event_type for row in rows} == {t.value for t in EVENT_MODELS}  # every event type was produced
    for row in rows:
        validator("queue.events", row.event_type).validate(row.payload)

def test_saga_worker_reads_every_payment_event_it_handles():
    """Consumer side: the Payment Service's published examples must parse with OUR contract."""
    for event_type in ("PaymentSucceeded", "PaymentFailed", "PaymentRefunded"):
        example = json.loads((CONTRACTS / "payment.events" / f"{event_type}.example.json").read_text())
        PaymentEvent.model_validate(example)
