import random

from telestream_producer.recharges import AirtimePurchaseGenerator
from telestream_producer.registry import SubscriberRegistry
from telestream_producer.subscribers import SubscriberCreatedGenerator
from telestream_spark.schemas import schema_violations


def make_event() -> dict[str, object]:
    rng = random.Random(3)
    registry = SubscriberRegistry()
    seeder = SubscriberCreatedGenerator(rng, registry)
    for _ in range(5):
        seeder.generate()
    return AirtimePurchaseGenerator(rng, registry).generate()


def test_valid_event_has_no_violations() -> None:
    assert schema_violations("airtime_purchase", make_event()) == []


def test_missing_field_flagged() -> None:
    event = make_event()
    del event["amount"]
    assert schema_violations("airtime_purchase", event) == ["missing:amount"]


def test_wrong_type_flagged() -> None:
    event = make_event()
    event["amount"] = "fifty"
    assert schema_violations("airtime_purchase", event) == ["type:amount"]


def test_int_accepted_for_float_field() -> None:
    event = make_event()
    event["amount"] = 50
    assert schema_violations("airtime_purchase", event) == []


def test_bool_rejected_for_numeric_field() -> None:
    event = make_event()
    event["subscriber_id"] = True
    assert schema_violations("airtime_purchase", event) == ["type:subscriber_id"]


def test_unexpected_field_flagged() -> None:
    event = make_event()
    event["discount"] = 10
    assert schema_violations("airtime_purchase", event) == ["unexpected:discount"]


def test_non_object_flagged() -> None:
    assert schema_violations("airtime_purchase", [1, 2]) == ["not_an_object"]
