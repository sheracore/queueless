# services/payment_service/app/dead_letter.py
"""Tell "not my event" apart from "my event, but broken", and park broken ones in a DLQ.

Before: any event that failed validation was treated as "not ours" and committed. So a breaking change in the producer made this consumer SKIP its own events silently (no charge, no error).
"""
import json
import logging

from confluent_kafka import KafkaError, Message, Producer
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)


class PermanentError(Exception):
    """This event is meant for us, but we can never process it (bad JSON, wrong shape, unknown version)."""


def parse_or_ignore(raw: bytes, handled_types: set[str], model: type[BaseModel],
                    supported_versions: set[int]) -> BaseModel | None:
    """None = not an event type we handle (ignore it). PermanentError = ours, but broken."""
    try:
        envelope = json.loads(raw)
    except (ValueError, TypeError) as exc:
        raise PermanentError(f"not valid JSON: {exc}") from exc
    if not isinstance(envelope, dict):
        raise PermanentError("not a JSON object")
    event_type = envelope.get("event_type")
    if event_type not in handled_types:
        return None
    if envelope.get("event_version") not in supported_versions:
        raise PermanentError(f"{event_type}: unsupported event_version {envelope.get('event_version')!r}")
    try:
        return model.model_validate(envelope)
    except ValidationError as exc:
        fields = ", ".join(".".join(str(p) for p in e["loc"]) for e in exc.errors())
        raise PermanentError(f"{event_type} does not match our contract ({fields})") from exc


class DeadLetterPublisher:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "partitioner": "murmur2_random",
        })

    def send(self, msg: Message, error: Exception) -> None:
        """Copy the message to the DLQ and WAIT for Kafka. Raises if it is not stored."""
        headers = list(msg.headers() or []) + [
            ("x-error", f"{type(error).__name__}: {error}"[:500].encode()),
            ("x-original-topic", msg.topic().encode()),
            ("x-original-partition", str(msg.partition()).encode()),
            ("x-original-offset", str(msg.offset()).encode()),
        ]
        errors: list[KafkaError] = []
        self._producer.produce(self.topic, key=msg.key(), value=msg.value(), headers=headers,
                               on_delivery=lambda err, _m: errors.append(err) if err else None)
        if self._producer.flush(10) or errors:
            raise RuntimeError(f"could not write to {self.topic}: {errors or 'timeout'}")