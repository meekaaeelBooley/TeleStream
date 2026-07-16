import json

from telestream_producer.config import ProducerConfig
from telestream_producer.main import DLQ_TOPIC, ProducerRunLoop
from telestream_producer.publisher import InMemoryPublisher

TOPIC_EVENT_TYPES = {
    "subscriber-created": "subscriber_created",
    "airtime-purchases": "airtime_purchase",
    "bundle-purchases": "bundle_purchase",
    "voice-calls": "voice_call",
    "sms": "sms",
    "data-usage": "data_usage",
    "tower-events": "tower_status",
    DLQ_TOPIC: "dlq_record",
}


def run_loop(events: int = 2000, error_rate: float = 0.05) -> InMemoryPublisher:
    publisher = InMemoryPublisher()
    config = ProducerConfig(
        events_per_second=100, error_rate=error_rate, initial_subscribers=100, seed=42
    )
    loop = ProducerRunLoop(config, publisher)
    loop.seed_subscribers()
    for _ in range(events):
        loop.emit_one()
    return publisher


def test_events_route_to_matching_topics() -> None:
    publisher = run_loop()
    assert publisher.published
    for topic, key, event in publisher.published:
        assert topic in TOPIC_EVENT_TYPES, f"unexpected topic {topic}"
        assert event["event_type"] == TOPIC_EVENT_TYPES[topic]
        assert key  # every message is keyed for partition ordering


def test_all_topics_receive_traffic() -> None:
    publisher = run_loop()
    seen = {topic for topic, _, _ in publisher.published}
    assert seen == set(TOPIC_EVENT_TYPES)


def test_payment_failures_wrap_valid_purchases() -> None:
    publisher = run_loop()
    failures = [e for t, _, e in publisher.published if t == DLQ_TOPIC]
    assert failures
    for event in failures:
        assert event["rejection_reason"] == "PAYMENT_DECLINED"
        inner = json.loads(event["original_payload"])
        assert inner["event_type"] == "airtime_purchase"


def test_event_ids_are_unique() -> None:
    publisher = run_loop()
    ids = [e["event_id"] for _, _, e in publisher.published]
    assert len(ids) == len(set(ids))
