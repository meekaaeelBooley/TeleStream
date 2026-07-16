"""Business rules must accept every valid generated event and flag every
corrupted one (stateless rules; referential failures are join-stage concerns)."""

import random

import pytest
from telestream_producer.base import EventGenerator
from telestream_producer.calls import SmsGenerator, VoiceCallGenerator
from telestream_producer.network import TowerStatusGenerator
from telestream_producer.recharges import AirtimePurchaseGenerator, BundlePurchaseGenerator
from telestream_producer.registry import SubscriberRegistry
from telestream_producer.subscribers import SubscriberCreatedGenerator
from telestream_producer.usage import DataUsageGenerator
from telestream_spark.rules import RULES, apply_rules

ALL_GENERATORS = [
    SubscriberCreatedGenerator,
    AirtimePurchaseGenerator,
    BundlePurchaseGenerator,
    VoiceCallGenerator,
    SmsGenerator,
    DataUsageGenerator,
    TowerStatusGenerator,
]

# Corruptions that only a stateful/referential check can catch.
REFERENTIAL_FAULTS = {"unknown_subscriber"}


def build(gen_cls: type[EventGenerator]) -> EventGenerator:
    rng = random.Random(11)
    registry = SubscriberRegistry()
    seeder = SubscriberCreatedGenerator(rng, registry)
    for _ in range(50):
        seeder.generate()
    return gen_cls(rng, registry)


def test_every_event_type_has_rules() -> None:
    assert set(RULES) == {g.event_type for g in ALL_GENERATORS}


@pytest.mark.parametrize("gen_cls", ALL_GENERATORS)
def test_valid_events_pass_all_rules(gen_cls: type[EventGenerator]) -> None:
    generator = build(gen_cls)
    for _ in range(200):
        event = generator.generate()
        assert apply_rules(generator.event_type, event) == []


@pytest.mark.parametrize("gen_cls", ALL_GENERATORS)
def test_corrupted_events_violate_a_rule_or_are_referential(
    gen_cls: type[EventGenerator],
) -> None:
    generator = build(gen_cls)
    registry = generator.registry
    flagged = 0
    for _ in range(200):
        event = generator.corrupt()
        violations = apply_rules(generator.event_type, event)
        if violations:
            flagged += 1
            continue
        # The only acceptable pass-through is a referential fault the
        # streaming job catches by joining against known subscribers.
        subscriber_field = next(
            f for f in ("subscriber_id", "caller_id", "sender_id") if f in event
        )
        assert event[subscriber_field] >= registry.next_subscriber_id(), (
            f"clean-by-rules corrupt event is not referential either: {event}"
        )
    assert flagged > 0


def test_rule_predicate_exception_counts_as_violation() -> None:
    # Missing field: predicate raises KeyError -> rule counts as violated.
    violations = apply_rules("airtime_purchase", {"event_type": "airtime_purchase"})
    assert "amount_positive" in violations
    assert "timestamp_not_in_future" in violations
