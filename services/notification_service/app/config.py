from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


def find_env_file() -> Path | None:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / ".env"
        if candidate.is_file():
            return candidate
    return None


class Settings(BaseSettings):
    # Own database: the notification service never touches the queue service's database.
    notification_database_url: str
    kafka_bootstrap_servers: str = "localhost:9094"
    queue_events_topic: str = "queue.events"
    consumer_group_id: str = "notification-service"

    model_config = SettingsConfigDict(
        env_file=find_env_file(),
        env_file_encoding="utf-8",
        extra="ignore",  # the shared .env also holds other services' settings
    )


settings = Settings()
