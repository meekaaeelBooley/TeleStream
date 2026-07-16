"""Shared generator machinery: envelope creation and corruption hooks."""

import random
import uuid
from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

from telestream_producer.registry import SubscriberRegistry

Event = dict[str, Any]

SCHEMA_VERSION = 1


class EventGenerator(ABC):
    """Base for all domain generators.

    Subclasses implement `generate` (a valid event) and `corrupt` (a
    deliberately invalid variant used to exercise the DLQ path downstream).
    """

    event_type: ClassVar[str]
    topic: ClassVar[str]

    def __init__(self, rng: random.Random, registry: SubscriberRegistry) -> None:
        self.rng = rng
        self.registry = registry

    def envelope(self) -> Event:
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "event_type": self.event_type,
            "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "schema_version": SCHEMA_VERSION,
        }

    def future_timestamp(self) -> str:
        """A timestamp beyond the allowed clock skew — rule-violating."""
        return (datetime.now(UTC) + timedelta(hours=2)).isoformat(timespec="milliseconds")

    @abstractmethod
    def generate(self) -> Event:
        """Produce one valid event conforming to the contract."""

    @abstractmethod
    def corrupt(self) -> Event:
        """Produce one event that violates the contract or a business rule."""

    def key(self, event: Event) -> str:
        """Kafka partition key for the event (per-entity ordering)."""
        raise NotImplementedError
