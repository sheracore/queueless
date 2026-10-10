import json
from datetime import datetime, timedelta, timezone

from app.outbox_relay import cleanup, relay_once
from tests.conftest import FakePublisher


def _make_events(client, count: int) -> None:
    business = client.post("/businesses", json={"name": "Joe"}).json()
    qid = client.post(f"/businesses/{business['id']}/queues", json={"name": "Haircut"}).json()["id"]
    for user_id in range(1, count + 1):
        client.post(f"/queues/{qid}/join", json={"user_id": user_id})


def test_relay_sends_pending_events_in_order_and_marks_them(client, db, outbox, publisher):
    _make_events(client, 3)

    found, published = relay_once(db, publisher)

    assert (found, published) == (3, 3)
    assert [m.id for m in publisher.sent] == [row.id for row in outbox()]  # oldest first
    assert all(row.published_at is not None for row in outbox())
    # The Kafka message is exactly the stored envelope, keyed by queue id.
    first = publisher.sent[0]
    assert json.loads(first.value)["event_type"] == "CustomerJoinedQueue"
    assert first.headers == {"event_type": "CustomerJoinedQueue"}


def test_published_events_are_not_sent_again(client, db, publisher):
    _make_events(client, 2)
    relay_once(db, publisher)
    assert relay_once(db, publisher) == (0, 0)
    assert len(publisher.sent) == 2


def test_failed_events_stay_in_outbox_and_are_retried(client, db, outbox):
    _make_events(client, 3)
    failing_id = outbox()[1].id

    assert relay_once(db, FakePublisher(fail_ids={failing_id})) == (3, 2)
    assert [row.published_at is None for row in outbox()] == [False, True, False]

    # Next round (Kafka is healthy again): only the failed event is sent.
    retry = FakePublisher()
    assert relay_once(db, retry) == (1, 1)
    assert [m.id for m in retry.sent] == [failing_id]


def test_batch_size_limits_one_round(client, db, publisher):
    _make_events(client, 5)
    assert relay_once(db, publisher, batch_size=2) == (2, 2)
    assert relay_once(db, publisher, batch_size=2) == (2, 2)
    assert relay_once(db, publisher, batch_size=2) == (1, 1)


def test_cleanup_deletes_only_old_published_events(client, db, outbox, publisher):
    _make_events(client, 3)
    relay_once(db, publisher, batch_size=2)                # 2 published, 1 still pending
    rows = outbox()
    rows[0].published_at = datetime.now(timezone.utc) - timedelta(days=8)  # old
    db.commit()

    assert cleanup(db, older_than=timedelta(days=7)) == 1
    remaining = outbox()
    assert [row.id for row in remaining] == [rows[1].id, rows[2].id]
    assert remaining[1].published_at is None               # pending events are never deleted