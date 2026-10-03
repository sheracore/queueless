import os

os.environ["DATABASE_URL"] = "sqlite://"  # must be set before importing the app

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import app.models  # noqa: F401  (registers the tables)
from app.database import Base, get_db
from app.events.envelope import Event
from app.events.publisher import get_publisher
from app.main import app


class FakePublisher:
    """Records events in memory instead of sending them to Kafka."""

    def __init__(self):
        self.messages: list[tuple[str, str, Event]] = []

    def publish(self, topic: str, key: str, event: Event) -> None:
        self.messages.append((topic, key, event))

    @property
    def event_types(self) -> list[str]:
        return [event.event_type for _, _, event in self.messages]


@pytest.fixture
def publisher():
    return FakePublisher()


@pytest.fixture
def client(publisher):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    TestingSession = sessionmaker(bind=engine, autoflush=False)

    def override_get_db():
        db = TestingSession()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_publisher] = lambda: publisher
    yield TestClient(app)
    app.dependency_overrides.clear()