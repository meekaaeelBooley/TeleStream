"""The producer's pandera contracts and the Spark job's field schemas both
implement docs/event-schemas.md — this test fails if they drift apart."""

from telestream_producer.contracts import SCHEMAS
from telestream_spark.schemas import EVENT_FIELDS, TOPIC_TO_EVENT_TYPE


def test_same_event_types() -> None:
    assert set(EVENT_FIELDS) == set(SCHEMAS)


def test_same_fields_per_event_type() -> None:
    for event_type, schema in SCHEMAS.items():
        contract_fields = set(schema.columns)
        spark_fields = set(EVENT_FIELDS[event_type])
        assert contract_fields == spark_fields, f"field drift in {event_type}"


def test_every_topic_maps_to_a_known_event_type() -> None:
    for event_type in TOPIC_TO_EVENT_TYPE.values():
        assert event_type in EVENT_FIELDS
