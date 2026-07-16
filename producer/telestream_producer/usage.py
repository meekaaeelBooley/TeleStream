"""Data session events."""

from telestream_producer.base import Event, EventGenerator
from telestream_producer.catalog import TOWERS
from telestream_producer.contracts import MAX_SESSION_MB


class DataUsageGenerator(EventGenerator):
    event_type = "data_usage"
    topic = "data-usage"

    def generate(self) -> Event:
        subscriber = self.registry.random_subscriber(self.rng)
        tower = self.rng.choice(TOWERS)
        return {
            **self.envelope(),
            "subscriber_id": subscriber.subscriber_id,
            "mb_used": round(min(self.rng.expovariate(1 / 80), MAX_SESSION_MB), 2),
            "session_seconds": self.rng.randint(10, 7200),
            "tower_id": tower.tower_id,
            "technology": self.rng.choice(tower.technologies),
        }

    def corrupt(self) -> Event:
        event = self.generate()
        fault = self.rng.choice(("implausible_volume", "unknown_tower", "zero_session"))
        if fault == "implausible_volume":
            event["mb_used"] = MAX_SESSION_MB * 10
        elif fault == "unknown_tower":
            event["tower_id"] = "XYZ-000-000"
        else:
            event["session_seconds"] = 0
        return event

    def key(self, event: Event) -> str:
        return str(event["subscriber_id"])
