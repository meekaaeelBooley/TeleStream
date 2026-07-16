"""Business rules engine — pure functions, no Spark dependency.

Each rule is (name, predicate); a predicate returns True when the event is
acceptable. `apply_rules` returns the names of every rule an event violates
(empty list = clean). Stateless rules only — referential checks that need
warehouse state (subscriber exists) are handled as joins in the streaming job.
"""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from telestream_producer.catalog import (
    BUNDLE_PRICES,
    PAYMENT_METHODS,
    PLANS,
    PROVINCES,
    TECHNOLOGIES,
    TOWER_IDS,
    TOWER_STATUSES,
)

Event = dict[str, Any]
Rule = tuple[str, Callable[[Event], bool]]

MAX_CALL_SECONDS = 6 * 60 * 60
MAX_SESSION_MB = 10240.0
MAX_RECHARGE = 1000.0
MAX_SMS_LENGTH = 1600
CLOCK_SKEW = timedelta(seconds=30)


def _timestamp_not_future(event: Event) -> bool:
    try:
        ts = datetime.fromisoformat(str(event["timestamp"]))
    except (ValueError, KeyError):
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts <= datetime.now(UTC) + CLOCK_SKEW


def _msisdn_valid(value: Any) -> bool:
    s = str(value)
    return len(s) == 11 and s.startswith("27") and s.isdigit()


COMMON_RULES: list[Rule] = [
    ("timestamp_not_in_future", _timestamp_not_future),
]

RULES: dict[str, list[Rule]] = {
    "subscriber_created": [
        *COMMON_RULES,
        ("msisdn_format", lambda e: _msisdn_valid(e["msisdn"])),
        ("plan_known", lambda e: e["plan"] in PLANS),
        ("province_known", lambda e: e["province"] in PROVINCES),
    ],
    "airtime_purchase": [
        *COMMON_RULES,
        ("amount_positive", lambda e: float(e["amount"]) > 0),
        ("amount_within_limit", lambda e: float(e["amount"]) <= MAX_RECHARGE),
        ("payment_method_known", lambda e: e["payment_method"] in PAYMENT_METHODS),
    ],
    "bundle_purchase": [
        *COMMON_RULES,
        ("bundle_exists", lambda e: e["bundle_code"] in BUNDLE_PRICES),
        (
            "price_matches_catalog",
            lambda e: float(e["price"]) == BUNDLE_PRICES.get(str(e["bundle_code"]), -1.0),
        ),
    ],
    "voice_call": [
        *COMMON_RULES,
        ("duration_not_negative", lambda e: int(e["duration_seconds"]) >= 0),
        ("duration_within_limit", lambda e: int(e["duration_seconds"]) <= MAX_CALL_SECONDS),
        ("tower_exists", lambda e: e["tower_id"] in TOWER_IDS),
        ("receiver_msisdn_format", lambda e: _msisdn_valid(e["receiver_msisdn"])),
    ],
    "sms": [
        *COMMON_RULES,
        ("length_positive", lambda e: int(e["length"]) >= 1),
        ("length_within_limit", lambda e: int(e["length"]) <= MAX_SMS_LENGTH),
        ("receiver_msisdn_format", lambda e: _msisdn_valid(e["receiver_msisdn"])),
    ],
    "data_usage": [
        *COMMON_RULES,
        ("volume_positive", lambda e: float(e["mb_used"]) > 0),
        ("volume_within_limit", lambda e: float(e["mb_used"]) <= MAX_SESSION_MB),
        ("session_positive", lambda e: int(e["session_seconds"]) > 0),
        ("tower_exists", lambda e: e["tower_id"] in TOWER_IDS),
        ("technology_known", lambda e: e["technology"] in TECHNOLOGIES),
    ],
    "tower_status": [
        *COMMON_RULES,
        ("signal_in_range", lambda e: 0 <= int(e["signal_strength"]) <= 100),
        ("devices_not_negative", lambda e: int(e["connected_devices"]) >= 0),
        ("tower_exists", lambda e: e["tower_id"] in TOWER_IDS),
        ("status_known", lambda e: e["status"] in TOWER_STATUSES),
    ],
}


def apply_rules(event_type: str, event: Event) -> list[str]:
    """Names of all rules the event violates; empty when the event is clean.

    A rule whose predicate raises (missing/mistyped field) counts as violated —
    malformed data must never pass on a technicality.
    """
    violations: list[str] = []
    for name, predicate in RULES.get(event_type, []):
        try:
            ok = predicate(event)
        except Exception:
            ok = False
        if not ok:
            violations.append(name)
    return violations
