from app.events.envelope import EventType


def _queue(client):
    business = client.post("/businesses", json={"name": "Joe's Barber Shop"}).json()
    return client.post(f"/businesses/{business['id']}/queues", json={"name": "Haircut"}).json()["id"]


def test_join_writes_customer_joined_queue_to_outbox(client, outbox):
    qid = _queue(client)
    entry = client.post(f"/queues/{qid}/join", json={"user_id": 7}).json()

    [row] = outbox()
    assert row.topic == "queue.events"
    assert row.message_key == str(qid)  # partition key = queue id
    assert row.event_type == EventType.CUSTOMER_JOINED_QUEUE
    assert row.published_at is None     # not sent yet: that is the relay's job
    assert row.payload["data"]["queue_entry_id"] == entry["id"]
    assert row.payload["data"]["user_id"] == 7
    assert row.payload["data"]["status"] == "WAITING"
    assert row.payload["event_id"] == str(row.event_id)


def test_full_lifecycle_writes_events_in_order(client, outbox):
    qid = _queue(client)
    a = client.post(f"/queues/{qid}/join", json={"user_id": 1}).json()
    b = client.post(f"/queues/{qid}/join", json={"user_id": 2}).json()
    client.post(f"/queues/{qid}/call-next")
    client.post(f"/queues/{qid}/entries/{a['id']}/serve")
    client.post(f"/queues/{qid}/entries/{b['id']}/leave", json={"user_id": 2})

    assert [row.event_type for row in outbox()] == [
        EventType.CUSTOMER_JOINED_QUEUE,
        EventType.CUSTOMER_JOINED_QUEUE,
        EventType.CUSTOMER_CALLED,
        EventType.CUSTOMER_SERVED,
        EventType.CUSTOMER_LEFT_QUEUE,
    ]


def test_no_show_writes_event(client, outbox):
    qid = _queue(client)
    e = client.post(f"/queues/{qid}/join", json={"user_id": 1}).json()
    client.post(f"/queues/{qid}/call-next")
    client.post(f"/queues/{qid}/entries/{e['id']}/no-show")
    assert outbox()[-1].event_type == EventType.CUSTOMER_NO_SHOW


def test_failed_actions_write_nothing(client, outbox):
    qid = _queue(client)
    client.post(f"/queues/{qid}/call-next")                       # 409: nobody waiting
    e = client.post(f"/queues/{qid}/join", json={"user_id": 1}).json()
    client.post(f"/queues/{qid}/join", json={"user_id": 1})       # 409: already in queue
    client.post(f"/queues/{qid}/entries/{e['id']}/serve")         # 409: still WAITING
    assert [row.event_type for row in outbox()] == [EventType.CUSTOMER_JOINED_QUEUE]


def test_every_event_has_a_unique_id(client, outbox):
    qid = _queue(client)
    for user_id in (1, 2, 3):
        client.post(f"/queues/{qid}/join", json={"user_id": user_id})
    assert len({row.event_id for row in outbox()}) == 3
