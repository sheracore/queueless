from app.channels import FakeSmsProvider, SmsProviderError
from app.retry import (
    ATTEMPT, ERROR, ORIGINAL_OFFSET, ORIGINAL_TOPIC, RETRY_AT,
    PermanentError, dlq_topic, failure_headers, next_destination, retry_topic,
)

DELAYS = [10, 60]


def test_topic_names():
    assert retry_topic(10) == "queue.events.notification-service.retry-10s"
    assert dlq_topic() == "queue.events.notification-service.dlq"


def test_temporary_error_walks_through_the_retry_tiers_then_dlq():
    error = SmsProviderError("503")
    assert next_destination(0, error, DELAYS) == (retry_topic(10), 10)  # first failure
    assert next_destination(1, error, DELAYS) == (retry_topic(60), 60)  # failed again in retry-10s
    assert next_destination(2, error, DELAYS) == (dlq_topic(), None)    # failed in retry-60s: give up


def test_permanent_error_goes_straight_to_dlq():
    assert next_destination(0, PermanentError("bad json"), DELAYS) == (dlq_topic(), None)


def test_failure_headers_on_first_failure():
    h = failure_headers({}, "queue.events", 2, 41, SmsProviderError("503"), delay=10, now_ms=1_000)
    assert h[ATTEMPT] == "1"
    assert h[RETRY_AT] == str(1_000 + 10_000)
    assert h[ORIGINAL_TOPIC] == "queue.events"
    assert h[ORIGINAL_OFFSET] == "41"
    assert h[ERROR] == "SmsProviderError: 503"


def test_failure_headers_keep_the_original_position_on_later_failures():
    first = failure_headers({}, "queue.events", 2, 41, SmsProviderError("503"), delay=10, now_ms=0)
    second = failure_headers(first, retry_topic(10), 0, 7, SmsProviderError("timeout"), delay=60, now_ms=0)
    assert second[ATTEMPT] == "2"
    assert second[ORIGINAL_TOPIC] == "queue.events"   # still the FIRST place, not the retry topic
    assert second[ORIGINAL_OFFSET] == "41"
    assert second[ERROR] == "SmsProviderError: timeout"


def test_dlq_headers_have_no_retry_time():
    h = failure_headers({ATTEMPT: "2"}, retry_topic(60), 0, 3, SmsProviderError("503"), delay=None, now_ms=0)
    assert RETRY_AT not in h
    assert h[ATTEMPT] == "2"


def test_fake_provider_failure_rate():
    always = FakeSmsProvider(failure_rate=1.0)
    never = FakeSmsProvider(failure_rate=0.0)
    try:
        always.send(1, "hi", "k")
        raise AssertionError("expected SmsProviderError")
    except SmsProviderError:
        pass
    never.send(1, "hi", "k")  # no exception