from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


def find_env_file() -> Path | None:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / ".env"
        if candidate.is_file():
            return candidate
    return None


class Settings(BaseSettings):
    # Own database: the payment service never touches another service's database.
    payment_database_url: str
    kafka_bootstrap_servers: str = "localhost:9094"
    queue_events_topic: str = "queue.events"       # we consume it
    payment_events_topic: str = "payment.events"   # we publish to it (via the outbox)
    consumer_group_id: str = "payment-service"
    # LAB ONLY: the fake payment gateway.
    payment_decline_rate: float = 0.0   # business result: card declined
    payment_failure_rate: float = 0.0   # technical problem: gateway timeout / HTTP 503

    model_config = SettingsConfigDict(
        env_file=find_env_file(),
        env_file_encoding="utf-8",
        extra="ignore",  # the shared .env also holds other services' settings
    )


settings = Settings()