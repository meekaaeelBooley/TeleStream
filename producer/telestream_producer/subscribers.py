"""Subscriber lifecycle events."""

from telestream_producer.base import Event, EventGenerator
from telestream_producer.catalog import PLANS, PROVINCES
from telestream_producer.registry import Subscriber


class SubscriberCreatedGenerator(EventGenerator):
    event_type = "subscriber_created"
    topic = "subscriber-created"

    def _new_msisdn(self) -> str:
        while True:
            msisdn = f"27{self.rng.choice('678')}{self.rng.randrange(10**8):08d}"
            if not self.registry.is_known_msisdn(msisdn):
                return msisdn

    def generate(self) -> Event:
        subscriber = Subscriber(
            subscriber_id=self.registry.next_subscriber_id(),
            msisdn=self._new_msisdn(),
            plan=self.rng.choice(PLANS),
            province=self.rng.choice(PROVINCES),
        )
        self.registry.add(subscriber)
        return {
            **self.envelope(),
            "subscriber_id": subscriber.subscriber_id,
            "msisdn": subscriber.msisdn,
            "plan": subscriber.plan,
            "province": subscriber.province,
        }

    def corrupt(self) -> Event:
        event = self.generate()
        fault = self.rng.choice(("bad_msisdn", "bad_plan", "future_timestamp"))
        if fault == "bad_msisdn":
            event["msisdn"] = f"0{event['msisdn'][2:]}"  # local format, breaks 27... contract
        elif fault == "bad_plan":
            event["plan"] = "Unlimited"
        else:
            event["timestamp"] = self.future_timestamp()
        return event

    def key(self, event: Event) -> str:
        return str(event["subscriber_id"])
