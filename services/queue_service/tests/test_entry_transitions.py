from app.models.queue_entry import QueueEntryStatus


def _queue_with_users(client, *user_ids):
    business = client.post("/businesses", json={"name": "Joe's Barber Shop"}).json()
    q = client.post(f"/businesses/{business['id']}/queues", json={"name": "Haircut"}).json()
    entries = [client.post(f"/queues/{q['id']}/join", json={"user_id": u}).json() for u in user_ids]
    return q["id"], entries


def test_called_entry_can_be_served(client):
    qid, (e,) = _queue_with_users(client, 1)
    client.post(f"/queues/{qid}/call-next")
    r = client.post(f"/queues/{qid}/entries/{e['id']}/serve")
    assert r.status_code == 200
    assert r.json()["status"] == QueueEntryStatus.SERVED
    assert r.json()["served_at"] is not None


def test_waiting_entry_cannot_be_served(client):
    qid, (e,) = _queue_with_users(client, 1)
    r = client.post(f"/queues/{qid}/entries/{e['id']}/serve")
    assert r.status_code == 409
    assert "WAITING" in r.json()["detail"]


def test_served_entry_is_final(client):
    qid, (e,) = _queue_with_users(client, 1)
    client.post(f"/queues/{qid}/call-next")
    client.post(f"/queues/{qid}/entries/{e['id']}/serve")
    assert client.post(f"/queues/{qid}/entries/{e['id']}/no-show").status_code == 409
    assert client.post(f"/queues/{qid}/entries/{e['id']}/serve").status_code == 409


def test_called_entry_can_be_marked_no_show(client):
    qid, (e,) = _queue_with_users(client, 1)
    client.post(f"/queues/{qid}/call-next")
    r = client.post(f"/queues/{qid}/entries/{e['id']}/no-show")
    assert r.status_code == 200
    assert r.json()["status"] == QueueEntryStatus.NO_SHOW


def test_waiting_user_can_leave_and_rejoin(client):
    qid, (e,) = _queue_with_users(client, 1)
    r = client.post(f"/queues/{qid}/entries/{e['id']}/leave", json={"user_id": 1})
    assert r.json()["status"] == QueueEntryStatus.LEFT
    assert client.post(f"/queues/{qid}/join", json={"user_id": 1}).status_code == 201


def test_user_cannot_leave_someone_elses_entry(client):
    qid, (e,) = _queue_with_users(client, 1)
    r = client.post(f"/queues/{qid}/entries/{e['id']}/leave", json={"user_id": 2})
    assert r.status_code == 404


def test_left_entry_is_skipped_by_call_next(client):
    qid, (first, second) = _queue_with_users(client, 1, 2)
    client.post(f"/queues/{qid}/entries/{first['id']}/leave", json={"user_id": 1})
    called = client.post(f"/queues/{qid}/call-next").json()
    assert called["ticket_number"] == second["ticket_number"]


def test_entry_in_other_queue_returns_404(client):
    qid, (e,) = _queue_with_users(client, 1)
    assert client.post(f"/queues/{qid + 1}/entries/{e['id']}/serve").status_code == 404
    assert client.post(f"/queues/{qid}/entries/999/serve").status_code == 404