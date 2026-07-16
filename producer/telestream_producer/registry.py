"""In-memory registry of active subscribers.

Generators share one registry so every event references a subscriber that
actually exists — the same referential integrity a real network has. The
subscriber generator adds entries; all other generators sample from it.
"""

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class Subscriber:
    subscriber_id: int
    msisdn: str
    plan: str
    province: str


class SubscriberRegistry:
    def __init__(self) -> None:
        self._subscribers: list[Subscriber] = []
        self._msisdns: set[str] = set()
        self._next_id = 10001

    def __len__(self) -> int:
        return len(self._subscribers)

    def next_subscriber_id(self) -> int:
        subscriber_id = self._next_id
        self._next_id += 1
        return subscriber_id

    def is_known_msisdn(self, msisdn: str) -> bool:
        return msisdn in self._msisdns

    def add(self, subscriber: Subscriber) -> None:
        if subscriber.msisdn in self._msisdns:
            raise ValueError(f"duplicate msisdn: {subscriber.msisdn}")
        self._subscribers.append(subscriber)
        self._msisdns.add(subscriber.msisdn)

    def random_subscriber(self, rng: random.Random) -> Subscriber:
        if not self._subscribers:
            raise LookupError("registry is empty — seed subscribers first")
        return rng.choice(self._subscribers)
