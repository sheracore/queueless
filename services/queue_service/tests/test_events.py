from app.events.envelope import EventType


def _queue(client):
    business = client.post("/businesses", json={"name": "Joe's Barber Shop"}).json()
    return client.post(f"/businesses/{business['id']}/queues", json={"name": "Haircut"}).json()["id"]


def test_join_publishes_customer_joined_queue(client, publisher):
    qid = _queue(client)
    client.post(f"/queues/{qid}/join", json={"user_id": 7})

    [(topic, key, event)] = publisher.messages
    assert topic == "queue.events"
    assert key == str(qid)  # partition key = queue id
    assert event.event_type == EventType.CUSTOMER_JOINED_QUEUE
    assert event.data["user_id"] == 7
    assert event.data["ticket_number"] == 1
    assert event.data["status"] == "WAITING"


def test_full_lifecycle_publishes_events_in_order(client, publisher):
    qid = _queue(client)
    a = client.post(f"/queues/{qid}/join", json={"user_id": 1}).json()
    b = client.post(f"/queues/{qid}/join", json={"user_id": 2}).json()
    client.post(f"/queues/{qid}/call-next")
    client.post(f"/queues/{qid}/entries/{a['id']}/serve")
    client.post(f"/queues/{qid}/entries/{b['id']}/leave", json={"user_id": 2})

    assert publisher.event_types == [
        EventType.CUSTOMER_JOINED_QUEUE,
        EventType.CUSTOMER_JOINED_QUEUE,
        EventType.CUSTOMER_CALLED,
        EventType.CUSTOMER_SERVED,
        EventType.CUSTOMER_LEFT_QUEUE,
    ]


def test_no_show_publishes_event(client, publisher):
    qid = _queue(client)
    e = client.post(f"/queues/{qid}/join", json={"user_id": 1}).json()
    client.post(f"/queues/{qid}/call-next")
    client.post(f"/queues/{qid}/entries/{e['id']}/no-show")
    assert publisher.event_types[-1] == EventType.CUSTOMER_NO_SHOW


def test_failed_actions_publish_nothing(client, publisher):
    qid = _queue(client)
    client.post(f"/queues/{qid}/call-next")                       # 409: nobody waiting
    e = client.post(f"/queues/{qid}/join", json={"user_id": 1}).json()
    client.post(f"/queues/{qid}/join", json={"user_id": 1})       # 409: already in queue
    client.post(f"/queues/{qid}/entries/{e['id']}/serve")         # 409: still WAITING
    assert publisher.event_types == [EventType.CUSTOMER_JOINED_QUEUE]


def test_every_event_has_a_unique_id(client, publisher):
    qid = _queue(client)
    for user_id in (1, 2, 3):
        client.post(f"/queues/{qid}/join", json={"user_id": user_id})
    ids = {event.event_id for _, _, event in publisher.messages}
    assert len(ids) == 3