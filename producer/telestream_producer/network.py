"""Tower telemetry events."""

from telestream_producer.base import Event, EventGenerator
from telestream_producer.catalog import TOWERS


class TowerStatusGenerator(EventGenerator):
    event_type = "tower_status"
    topic = "tower-events"

    def generate(self) -> Event:
        tower = self.rng.choice(TOWERS)
        signal = self.rng.randint(35, 100)
        if signal < 45:
            status = "DOWN"
        elif signal < 60:
            status = "DEGRADED"
        else:
            status = "HEALTHY"
        return {
            **self.envelope(),
            "tower_id": tower.tower_id,
            "technology": self.rng.choice(tower.technologies),
            "signal_strength": signal,
            "connected_devices": self.rng.randint(50, 5000),
            "status": status,
        }

    def corrupt(self) -> Event:
        event = self.generate()
        fault = self.rng.choice(("impossible_signal", "unknown_tower", "bad_status"))
        if fault == "impossible_signal":
            event["signal_strength"] = 250
        elif fault == "unknown_tower":
            event["tower_id"] = "GHOST-001"
        else:
            event["status"] = "ON_FIRE"
        return event

    def key(self, event: Event) -> str:
        return str(event["tower_id"])
