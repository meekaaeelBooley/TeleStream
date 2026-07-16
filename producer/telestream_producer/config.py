"""Environment-driven producer configuration."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ProducerConfig:
    bootstrap_servers: str = "localhost:9092"
    events_per_second: int = 50
    error_rate: float = 0.02
    initial_subscribers: int = 500
    seed: int | None = None

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        seed_raw = os.environ.get("SEED", "")
        return cls(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers),
            events_per_second=int(os.environ.get("EVENTS_PER_SECOND", cls.events_per_second)),
            error_rate=float(os.environ.get("ERROR_RATE", cls.error_rate)),
            initial_subscribers=int(os.environ.get("INITIAL_SUBSCRIBERS", cls.initial_subscribers)),
            seed=int(seed_raw) if seed_raw else None,
        )

    def __post_init__(self) -> None:
        if self.events_per_second <= 0:
            raise ValueError("events_per_second must be positive")
        if not 0.0 <= self.error_rate < 1.0:
            raise ValueError("error_rate must be in [0, 1)")
        if self.initial_subscribers <= 0:
            raise ValueError("initial_subscribers must be positive")
