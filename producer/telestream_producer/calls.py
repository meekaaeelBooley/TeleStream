"""Communication events: voice calls and SMS."""

from telestream_producer.base import Event, EventGenerator
from telestream_producer.catalog import TOWER_IDS
from telestream_producer.contracts import MAX_CALL_SECONDS

DROP_RATE = 0.03


class VoiceCallGenerator(EventGenerator):
    event_type = "voice_call"
    topic = "voice-calls"

    def generate(self) -> Event:
        caller = self.registry.random_subscriber(self.rng)
        receiver = self.registry.random_subscriber(self.rng)
        dropped = self.rng.random() < DROP_RATE
        # Dropped calls end early; normal calls follow a short-skewed distribution.
        duration = self.rng.randrange(0, 45) if dropped else int(self.rng.expovariate(1 / 180))
        return {
            **self.envelope(),
            "caller_id": caller.subscriber_id,
            "receiver_msisdn": receiver.msisdn,
            "duration_seconds": min(duration, MAX_CALL_SECONDS),
            "tower_id": self.rng.choice(TOWER_IDS),
            "dropped": dropped,
        }

    def corrupt(self) -> Event:
        event = self.generate()
        fault = self.rng.choice(("marathon_call", "unknown_tower", "negative_duration"))
        if fault == "marathon_call":
            event["duration_seconds"] = MAX_CALL_SECONDS + 3600
        elif fault == "unknown_tower":
            event["tower_id"] = "ATL-XXX-999"
        else:
            event["duration_seconds"] = -30
        return event

    def key(self, event: Event) -> str:
        return str(event["caller_id"])


class SmsGenerator(EventGenerator):
    event_type = "sms"
    topic = "sms"

    def generate(self) -> Event:
        sender = self.registry.random_subscriber(self.rng)
        receiver = self.registry.random_subscriber(self.rng)
        return {
            **self.envelope(),
            "sender_id": sender.subscriber_id,
            "receiver_msisdn": receiver.msisdn,
            "length": self.rng.randint(1, 320),
        }

    def corrupt(self) -> Event:
        event = self.generate()
        fault = self.rng.choice(("empty_sms", "oversized", "unknown_subscriber"))
        if fault == "empty_sms":
            event["length"] = 0
        elif fault == "oversized":
            event["length"] = 5000
        else:
            event["sender_id"] = 999_999_999
        return event

    def key(self, event: Event) -> str:
        return str(event["sender_id"])
