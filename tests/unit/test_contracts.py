"""Every generator's valid output must pass its contract; every corrupted
output must be rejectable — by the contract itself or by a referential
business rule (unknown subscriber, catalog price mismatch)."""

import random
from typing import Any

import pandera.errors
import pytest
from telestream_producer import contracts
from telestream_producer.base import EventGenerator
from telestream_producer.calls import SmsGenerator, VoiceCallGenerator
from telestream_producer.catalog import BUNDLE_PRICES
from telestream_producer.network import TowerStatusGenerator
from telestream_producer.recharges import AirtimePurchaseGenerator, BundlePurchaseGenerator
from telestream_producer.registry import SubscriberRegistry
from telestream_producer.subscribers import SubscriberCreatedGenerator
from telestream_producer.usage import DataUsageGenerator

ALL_GENERATORS = [
    SubscriberCreatedGenerator,
    AirtimePurchaseGenerator,
    BundlePurchaseGenerator,
    VoiceCallGenerator,
    SmsGenerator,
    DataUsageGenerator,
    TowerStatusGenerator,
]


def build(gen_cls: type[EventGenerator], seed: int = 7) -> EventGenerator:
    rng = random.Random(seed)
    registry = SubscriberRegistry()
    seeder = SubscriberCreatedGenerator(rng, registry)
    for _ in range(50):
        seeder.generate()
    return gen_cls(rng, registry)


def passes_contract(event_type: str, event: dict[str, Any]) -> bool:
    try:
        contracts.validate(event_type, [event])
        return True
    except pandera.errors.SchemaError:
        return False


def violates_referential_rule(registry: SubscriberRegistry, event: dict[str, Any]) -> bool:
    subscriber_field = next(
        (f for f in ("subscriber_id", "caller_id", "sender_id") if f in event), None
    )
    if subscriber_field is not None and event[subscriber_field] >= registry.next_subscriber_id():
        return True
    return "bundle_code" in event and event["price"] != BUNDLE_PRICES.get(event["bundle_code"])


@pytest.mark.parametrize("gen_cls", ALL_GENERATORS)
def test_valid_events_pass_contract(gen_cls: type[EventGenerator]) -> None:
    generator = build(gen_cls)
    events = [generator.generate() for _ in range(100)]
    contracts.validate(generator.event_type, events)  # raises on violation


@pytest.mark.parametrize("gen_cls", ALL_GENERATORS)
def test_corrupted_events_are_rejectable(gen_cls: type[EventGenerator]) -> None:
    generator = build(gen_cls)
    for _ in range(100):
        event = generator.corrupt()
        assert not passes_contract(generator.event_type, event) or violates_referential_rule(
            generator.registry, event
        ), f"corrupt() produced an event nothing would reject: {event}"


def test_unknown_event_type_raises() -> None:
    with pytest.raises(KeyError):
        contracts.validate("carrier_pigeon", [{}])
