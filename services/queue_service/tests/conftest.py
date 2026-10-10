import os

os.environ["DATABASE_URL"] = "sqlite://"  # must be set before importing the app

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import app.models  # noqa: F401  (registers the tables)
from app.database import Base, get_db
from app.events.publisher import OutgoingMessage
from app.main import app
from app.models.outbox_event import OutboxEvent


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False)


@pytest.fixture
def db(session_factory):
    session = session_factory()
    yield session
    session.close()


@pytest.fixture
def client(session_factory):
    def override_get_db():
        session = session_factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_get_db
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def outbox(db):
    """Returns a function that reads all outbox rows, oldest first."""
    def read() -> list[OutboxEvent]:
        db.expire_all()  # forget cached objects: always read fresh rows
        return list(db.scalars(select(OutboxEvent).order_by(OutboxEvent.id)))
    return read


class FakePublisher:
    """Pretends to be Kafka. Messages whose id is in `fail_ids` are 'not delivered'."""

    def __init__(self, fail_ids: set[int] | None = None):
        self.fail_ids = fail_ids or set()
        self.sent: list[OutgoingMessage] = []

    def publish_batch(self, messages: list[OutgoingMessage]) -> set[int]:
        self.sent.extend(messages)
        return {m.id for m in messages if m.id not in self.fail_ids}


@pytest.fixture
def publisher():
    return FakePublisher()