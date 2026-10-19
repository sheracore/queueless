import logging
import random
from collections.abc import Callable
from typing import Protocol

logger = logging.getLogger(__name__)


class SmsProviderError(Exception):
    """The SMS provider failed (timeout, HTTP 503, ...). Usually temporary: worth retrying."""


class SmsSender(Protocol):
    def send(self, user_id: int, text: str, idempotency_key: str) -> None: ...


class FakeSmsProvider:
    """Stands in for a real SMS API (Twilio, Kavenegar, ...).

    `failure_rate` is a LAB switch: 0.3 means 30% of sends fail, like a flaky provider.
    """

    def __init__(self, failure_rate: float = 0.0, rng: Callable[[], float] = random.random):
        self.failure_rate = failure_rate
        self.rng = rng

    def send(self, user_id: int, text: str, idempotency_key: str) -> None:
        if self.rng() < self.failure_rate:
            raise SmsProviderError("SMS provider returned HTTP 503")
        # A real provider gets `idempotency_key` too: if we send the same key twice
        # (after a crash), it delivers the SMS only once.
        logger.info("SMS to user %s: %s", user_id, text)