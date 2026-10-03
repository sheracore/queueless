from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


def find_env_file() -> Path | None:
    # Local dev: walk up from this file and use the first .env found (the repo root's).
    # In a container there is no .env (it's excluded by .dockerignore), so this returns None
    # and settings come purely from real environment variables.
    for parent in Path(__file__).resolve().parents:
        candidate = parent / ".env"
        if candidate.is_file():
            return candidate
    return None


class Settings(BaseSettings):
    app_name: str = "QueueLess Queue Service"
    database_url: str
    kafka_bootstrap_servers: str = "localhost:9094"
    queue_events_topic: str = "queue.events"

    model_config = SettingsConfigDict(
        env_file=find_env_file(),
        env_file_encoding="utf-8",
    )

settings = Settings()
