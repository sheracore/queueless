import logging
import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

logger = logging.getLogger(__name__)


class GatewayUnavailableError(Exception):
    """A TECHNICAL problem (timeout, HTTP 503). We do not know the result: retry later."""


@dataclass(frozen=True)
class ChargeResult:
    approved: bool
    reason: str | None = None   # e.g. "card_declined": a BUSINESS answer, final


class PaymentGateway(Protocol):
    def charge(self, amount: int, user_id: int, idempotency_key: str) -> ChargeResult: ...
    def refund(self, amount: int, idempotency_key: str) -> None: ...


class FakePaymentGateway:
    """Stands in for a real gateway (Stripe, Zarinpal, ...).

    Like real gateways it remembers idempotency keys: charging the same key twice
    returns the first result and does NOT charge the customer again.
    `decline_rate` and `failure_rate` are LAB switches.
    """

    def __init__(self, decline_rate: float = 0.0, failure_rate: float = 0.0,
                 rng: Callable[[], float] = random.random):
        self.decline_rate = decline_rate
        self.failure_rate = failure_rate
        self.rng = rng
        self.charges: dict[str, ChargeResult] = {}
        self.refunds: set[str] = set()

    def charge(self, amount: int, user_id: int, idempotency_key: str) -> ChargeResult:
        if idempotency_key in self.charges:
            return self.charges[idempotency_key]          # a retry: same answer, no new charge
        if self.rng() < self.failure_rate:
            raise GatewayUnavailableError("payment gateway timed out")
        result = ChargeResult(False, "card_declined") if self.rng() < self.decline_rate else ChargeResult(True)
        self.charges[idempotency_key] = result
        logger.info("Gateway: charge %s for user %s -> %s", amount, user_id,
                    "approved" if result.approved else result.reason)
        return result

    def refund(self, amount: int, idempotency_key: str) -> None:
        if idempotency_key in self.refunds:
            return
        if self.rng() < self.failure_rate:
            raise GatewayUnavailableError("payment gateway timed out")
        self.refunds.add(idempotency_key)
        logger.info("Gateway: refund %s", amount)