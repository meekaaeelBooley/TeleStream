import random

from telestream_producer.calls import VoiceCallGenerator
from telestream_producer.registry import SubscriberRegistry
from telestream_producer.subscribers import SubscriberCreatedGenerator


def seeded_registry(seed: int, count: int = 100) -> tuple[random.Random, SubscriberRegistry]:
    rng = random.Random(seed)
    registry = SubscriberRegistry()
    seeder = SubscriberCreatedGenerator(rng, registry)
    for _ in range(count):
        seeder.generate()
    return rng, registry


def test_same_seed_same_events() -> None:
    def run(seed: int) -> list[dict[str, object]]:
        rng, registry = seeded_registry(seed)
        gen = VoiceCallGenerator(rng, registry)
        events = [gen.generate() for _ in range(20)]
        # Timestamps are wall-clock; determinism applies to everything else.
        for e in events:
            del e["timestamp"]
        return events

    assert run(42) == run(42)
    assert run(42) != run(43)


def test_msisdns_are_unique() -> None:
    _, registry = seeded_registry(1, count=2000)
    assert len(registry) == 2000


def test_calls_reference_existing_subscribers() -> None:
    rng, registry = seeded_registry(5)
    gen = VoiceCallGenerator(rng, registry)
    known_ids = {registry.random_subscriber(rng).subscriber_id for _ in range(500)}
    for _ in range(200):
        event = gen.generate()
        assert isinstance(event["caller_id"], int)
        assert event["caller_id"] < registry.next_subscriber_id()
    assert known_ids  # registry sampling works


def test_registry_rejects_duplicate_msisdn() -> None:
    import pytest
    from telestream_producer.registry import Subscriber

    registry = SubscriberRegistry()
    sub = Subscriber(1, "27821234567", "Prepaid", "Gauteng")
    registry.add(sub)
    with pytest.raises(ValueError):
        registry.add(sub)
