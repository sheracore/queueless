
def _make_queue(client):
    business = client.post("/businesses", json={"name": "Joe's Barber Shop"}).json()
    return client.post(f"/businesses/{business['id']}/queues", json={"name": "Haircut"}).json()


def test_join_assigns_incrementing_tickets(client):
    q = _make_queue(client)
    a = client.post(f"/queues/{q['id']}/join", json={"user_id": 1})
    b = client.post(f"/queues/{q['id']}/join", json={"user_id": 2})
    assert a.status_code == 201
    assert (a.json()["ticket_number"], b.json()["ticket_number"]) == (1, 2)


def test_same_user_cannot_join_twice(client):
    q = _make_queue(client)
    client.post(f"/queues/{q['id']}/join", json={"user_id": 1})
    assert client.post(f"/queues/{q['id']}/join", json={"user_id": 1}).status_code == 409


def test_unknown_queue_and_business_return_404(client):
    assert client.post("/queues/999/join", json={"user_id": 1}).status_code == 404
    assert client.post("/businesses/999/queues", json={"name": "x"}).status_code == 404


def test_call_next_calls_lowest_waiting_ticket_in_order(client):
    q = _make_queue(client)
    for user_id in (1, 2):
        client.post(f"/queues/{q['id']}/join", json={"user_id": user_id})
    first = client.post(f"/queues/{q['id']}/call-next").json()
    second = client.post(f"/queues/{q['id']}/call-next").json()
    assert (first["ticket_number"], first["status"]) == (1, 2)
    assert first["called_at"] is not None
    assert second["ticket_number"] == 2


def test_call_next_on_empty_queue_returns_409(client):
    q = _make_queue(client)
    assert client.post(f"/queues/{q['id']}/call-next").status_code == 409


def test_called_user_cannot_rejoin(client):
    q = _make_queue(client)
    client.post(f"/queues/{q['id']}/join", json={"user_id": 1})
    client.post(f"/queues/{q['id']}/call-next")
    assert client.post(f"/queues/{q['id']}/join", json={"user_id": 1}).status_code == 409