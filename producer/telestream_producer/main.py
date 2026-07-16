"""Producer entrypoint: seeds the subscriber base, then emits a weighted
mix of telecom events at a configurable rate with configurable corruption."""

import json
import logging
import random
import time
import uuid
from datetime import UTC, datetime

from telestream_producer.base import SCHEMA_VERSION, Event, EventGenerator
from telestream_producer.calls import SmsGenerator, VoiceCallGenerator
from telestream_producer.config import ProducerConfig
from telestream_producer.network import TowerStatusGenerator
from telestream_producer.publisher import EventPublisher, KafkaEventPublisher
from telestream_producer.recharges import AirtimePurchaseGenerator, BundlePurchaseGenerator
from telestream_producer.registry import SubscriberRegistry
from telestream_producer.subscribers import SubscriberCreatedGenerator
from telestream_producer.usage import DataUsageGenerator

logger = logging.getLogger("telestream.producer")

DLQ_TOPIC = "failed-transactions"

# Relative volume of each event domain (see docs/event-schemas.md topic map).
GENERATOR_WEIGHTS: dict[type[EventGenerator], int] = {
    SubscriberCreatedGenerator: 2,
    AirtimePurchaseGenerator: 10,
    BundlePurchaseGenerator: 8,
    VoiceCallGenerator: 25,
    SmsGenerator: 15,
    DataUsageGenerator: 30,
    TowerStatusGenerator: 6,
}
PAYMENT_FAILURE_WEIGHT = 4


def payment_failure_event(rng: random.Random, declined: Event) -> Event:
    """A business-level failure: a declined recharge, published straight to
    the failed-transactions topic by the payment simulator."""
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "event_type": "dlq_record",
        "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
        "schema_version": SCHEMA_VERSION,
        "source_topic": "airtime-purchases",
        "rejection_reason": "PAYMENT_DECLINED",
        "original_payload": json.dumps(declined, separators=(",", ":")),
    }


class ProducerRunLoop:
    def __init__(self, config: ProducerConfig, publisher: EventPublisher) -> None:
        self.config = config
        self.publisher = publisher
        self.rng = random.Random(config.seed)
        self.registry = SubscriberRegistry()
        self.generators = [gen(self.rng, self.registry) for gen in GENERATOR_WEIGHTS]
        self.weights = list(GENERATOR_WEIGHTS.values())
        self._subscriber_gen = self.generators[0]
        self._airtime_gen = self.generators[1]
        self.emitted = 0
        self.corrupted = 0

    def seed_subscribers(self) -> None:
        for _ in range(self.config.initial_subscribers):
            event = self._subscriber_gen.generate()
            self.publisher.publish(
                self._subscriber_gen.topic, self._subscriber_gen.key(event), event
            )
            self.emitted += 1

    def emit_one(self) -> None:
        roll = self.rng.randrange(sum(self.weights) + PAYMENT_FAILURE_WEIGHT)
        if roll >= sum(self.weights):
            declined = self._airtime_gen.generate()
            event = payment_failure_event(self.rng, declined)
            self.publisher.publish(DLQ_TOPIC, str(declined["subscriber_id"]), event)
        else:
            generator = self.rng.choices(self.generators, weights=self.weights, k=1)[0]
            if self.rng.random() < self.config.error_rate:
                event = generator.corrupt()
                self.corrupted += 1
            else:
                event = generator.generate()
            self.publisher.publish(generator.topic, generator.key(event), event)
        self.emitted += 1

    def run_forever(self) -> None:
        self.seed_subscribers()
        logger.info("seeded %d subscribers", len(self.registry))
        last_report = time.monotonic()
        while True:
            tick_start = time.monotonic()
            for _ in range(self.config.events_per_second):
                self.emit_one()
            if time.monotonic() - last_report >= 10:
                logger.info("emitted=%d corrupted=%d", self.emitted, self.corrupted)
                last_report = time.monotonic()
            elapsed = time.monotonic() - tick_start
            time.sleep(max(0.0, 1.0 - elapsed))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    config = ProducerConfig.from_env()
    logger.info("starting producer: %s", config)
    publisher = KafkaEventPublisher(config.bootstrap_servers)
    loop = ProducerRunLoop(config, publisher)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("shutting down")
    finally:
        publisher.close()


if __name__ == "__main__":
    main()
