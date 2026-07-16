"""Kafka publishing layer shared by all generators."""

import json
import logging
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class EventPublisher(Protocol):
    """What the run loop needs — lets tests substitute an in-memory fake."""

    def publish(self, topic: str, key: str, event: dict[str, Any]) -> None: ...

    def close(self) -> None: ...


class KafkaEventPublisher:
    def __init__(self, bootstrap_servers: str) -> None:
        # Imported here so unit tests never need the kafka client installed/reachable.
        from kafka import KafkaProducer

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
            acks="all",
            linger_ms=20,
            retries=5,
        )

    def publish(self, topic: str, key: str, event: dict[str, Any]) -> None:
        self._producer.send(topic, key=key, value=event)

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()


class InMemoryPublisher:
    """Test double that records everything published."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str, dict[str, Any]]] = []

    def publish(self, topic: str, key: str, event: dict[str, Any]) -> None:
        self.published.append((topic, key, event))

    def close(self) -> None:
        pass
