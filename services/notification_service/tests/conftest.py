import os

os.environ["NOTIFICATION_DATABASE_URL"] = "sqlite://"  # must be set before importing the app

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import app.models  # noqa: F401
from app.database import Base, get_db
from app.channels import SmsProviderError
from app.main import app


@pytest.fixture
def db():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, autoflush=False)()
    yield session
    session.close()


class RecordingSender:
    """A fake SMS provider for tests: records every SMS, and can be told to fail."""

    def __init__(self):
        self.sent: list[tuple[int, str, str]] = []
        self.fail = False

    def send(self, user_id: int, text: str, idempotency_key: str) -> None:
        if self.fail:
            raise SmsProviderError("provider down (test)")
        self.sent.append((user_id, text, idempotency_key))


@pytest.fixture
def sender():
    return RecordingSender()


@pytest.fixture
def client(db):
    app.dependency_overrides[get_db] = lambda: db
    yield TestClient(app)
    app.dependency_overrides.clear()