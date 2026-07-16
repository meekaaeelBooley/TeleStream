"""Revenue events: airtime purchases and bundle purchases."""

from telestream_producer.base import Event, EventGenerator
from telestream_producer.catalog import BUNDLE_PRICES, BUNDLES, PAYMENT_METHODS

AIRTIME_DENOMINATIONS = (5.0, 10.0, 12.0, 29.0, 50.0, 100.0, 150.0, 275.0, 500.0)


class AirtimePurchaseGenerator(EventGenerator):
    event_type = "airtime_purchase"
    topic = "airtime-purchases"

    def generate(self) -> Event:
        subscriber = self.registry.random_subscriber(self.rng)
        return {
            **self.envelope(),
            "subscriber_id": subscriber.subscriber_id,
            "amount": self.rng.choice(AIRTIME_DENOMINATIONS),
            "payment_method": self.rng.choice(PAYMENT_METHODS),
        }

    def corrupt(self) -> Event:
        event = self.generate()
        fault = self.rng.choice(("negative_amount", "unknown_subscriber", "future_timestamp"))
        if fault == "negative_amount":
            event["amount"] = -float(event["amount"])
        elif fault == "unknown_subscriber":
            event["subscriber_id"] = 999_999_999
        else:
            event["timestamp"] = self.future_timestamp()
        return event

    def key(self, event: Event) -> str:
        return str(event["subscriber_id"])


class BundlePurchaseGenerator(EventGenerator):
    event_type = "bundle_purchase"
    topic = "bundle-purchases"

    def generate(self) -> Event:
        subscriber = self.registry.random_subscriber(self.rng)
        bundle = self.rng.choice(BUNDLES)
        return {
            **self.envelope(),
            "subscriber_id": subscriber.subscriber_id,
            "bundle_code": bundle.bundle_code,
            "price": bundle.price,
        }

    def corrupt(self) -> Event:
        event = self.generate()
        fault = self.rng.choice(("unknown_bundle", "wrong_price", "unknown_subscriber"))
        if fault == "unknown_bundle":
            event["bundle_code"] = "DATA_999GB"
        elif fault == "wrong_price":
            event["price"] = BUNDLE_PRICES[str(event["bundle_code"])] + 50.0
        else:
            event["subscriber_id"] = 999_999_999
        return event

    def key(self, event: Event) -> str:
        return str(event["subscriber_id"])
