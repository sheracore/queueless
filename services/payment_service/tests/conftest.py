import os

os.environ["PAYMENT_DATABASE_URL"] = "sqlite://"  # must be set before importing the app

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import app.models  # noqa: F401
from app.database import Base
from app.gateway import FakePaymentGateway
from app.models import OutboxEvent


@pytest.fixture
def db():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, autoflush=False)()
    yield session
    session.close()


@pytest.fixture
def gateway():
    return FakePaymentGateway()   # approves everything unless a test changes it


@pytest.fixture
def outbox(db):
    def read():
        db.expire_all()
        return list(db.scalars(select(OutboxEvent).order_by(OutboxEvent.id)))
    return read