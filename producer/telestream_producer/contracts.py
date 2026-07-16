"""Pandera contracts for every event type.

These implement docs/event-schemas.md — that document is canonical. The
contracts are used by producer unit tests (valid events must pass, injected
corruption must fail) and can validate any batch of events as a DataFrame.
"""

from typing import Any

import pandas as pd
import pandera.pandas as pa

from telestream_producer.catalog import (
    BUNDLE_CODES,
    PAYMENT_METHODS,
    PLANS,
    PROVINCES,
    TECHNOLOGIES,
    TOWER_IDS,
    TOWER_STATUSES,
)

MSISDN_REGEX = r"^27\d{9}$"
UUID_REGEX = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
MAX_CALL_SECONDS = 6 * 60 * 60
MAX_SESSION_MB = 10240.0
MAX_RECHARGE = 1000.0
CLOCK_SKEW_SECONDS = 30


def _timestamps_valid(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, utc=True, errors="coerce", format="ISO8601")
    limit = pd.Timestamp.now(tz="UTC") + pd.Timedelta(seconds=CLOCK_SKEW_SECONDS)
    return parsed.notna() & (parsed <= limit)


def _envelope(event_type: str) -> dict[str, pa.Column]:
    return {
        "event_id": pa.Column(str, pa.Check.str_matches(UUID_REGEX)),
        "event_type": pa.Column(str, pa.Check.eq(event_type)),
        "timestamp": pa.Column(str, pa.Check(_timestamps_valid, name="timestamp_valid")),
        "schema_version": pa.Column(int, pa.Check.ge(1)),
    }


SCHEMAS: dict[str, pa.DataFrameSchema] = {
    "subscriber_created": pa.DataFrameSchema(
        {
            **_envelope("subscriber_created"),
            "subscriber_id": pa.Column(int, pa.Check.gt(0)),
            "msisdn": pa.Column(str, pa.Check.str_matches(MSISDN_REGEX)),
            "plan": pa.Column(str, pa.Check.isin(PLANS)),
            "province": pa.Column(str, pa.Check.isin(PROVINCES)),
        },
        strict=True,
    ),
    "airtime_purchase": pa.DataFrameSchema(
        {
            **_envelope("airtime_purchase"),
            "subscriber_id": pa.Column(int, pa.Check.gt(0)),
            "amount": pa.Column(float, [pa.Check.gt(0), pa.Check.le(MAX_RECHARGE)]),
            "payment_method": pa.Column(str, pa.Check.isin(PAYMENT_METHODS)),
        },
        strict=True,
    ),
    "bundle_purchase": pa.DataFrameSchema(
        {
            **_envelope("bundle_purchase"),
            "subscriber_id": pa.Column(int, pa.Check.gt(0)),
            "bundle_code": pa.Column(str, pa.Check.isin(BUNDLE_CODES)),
            "price": pa.Column(float, pa.Check.gt(0)),
        },
        strict=True,
    ),
    "voice_call": pa.DataFrameSchema(
        {
            **_envelope("voice_call"),
            "caller_id": pa.Column(int, pa.Check.gt(0)),
            "receiver_msisdn": pa.Column(str, pa.Check.str_matches(MSISDN_REGEX)),
            "duration_seconds": pa.Column(int, [pa.Check.ge(0), pa.Check.le(MAX_CALL_SECONDS)]),
            "tower_id": pa.Column(str, pa.Check.isin(TOWER_IDS)),
            "dropped": pa.Column(bool),
        },
        strict=True,
    ),
    "sms": pa.DataFrameSchema(
        {
            **_envelope("sms"),
            "sender_id": pa.Column(int, pa.Check.gt(0)),
            "receiver_msisdn": pa.Column(str, pa.Check.str_matches(MSISDN_REGEX)),
            "length": pa.Column(int, [pa.Check.ge(1), pa.Check.le(1600)]),
        },
        strict=True,
    ),
    "data_usage": pa.DataFrameSchema(
        {
            **_envelope("data_usage"),
            "subscriber_id": pa.Column(int, pa.Check.gt(0)),
            "mb_used": pa.Column(float, [pa.Check.gt(0), pa.Check.le(MAX_SESSION_MB)]),
            "session_seconds": pa.Column(int, pa.Check.gt(0)),
            "tower_id": pa.Column(str, pa.Check.isin(TOWER_IDS)),
            "technology": pa.Column(str, pa.Check.isin(TECHNOLOGIES)),
        },
        strict=True,
    ),
    "tower_status": pa.DataFrameSchema(
        {
            **_envelope("tower_status"),
            "tower_id": pa.Column(str, pa.Check.isin(TOWER_IDS)),
            "technology": pa.Column(str, pa.Check.isin(TECHNOLOGIES)),
            "signal_strength": pa.Column(int, [pa.Check.ge(0), pa.Check.le(100)]),
            "connected_devices": pa.Column(int, pa.Check.ge(0)),
            "status": pa.Column(str, pa.Check.isin(TOWER_STATUSES)),
        },
        strict=True,
    ),
    "dlq_record": pa.DataFrameSchema(
        {
            **_envelope("dlq_record"),
            "source_topic": pa.Column(str),
            "rejection_reason": pa.Column(str),
            "original_payload": pa.Column(str),
        },
        strict=True,
    ),
}


def validate(event_type: str, events: list[dict[str, Any]]) -> pd.DataFrame:
    """Validate a batch of events against its contract.

    Raises pandera.errors.SchemaError on the first violation.
    """
    schema = SCHEMAS[event_type]
    frame = pd.DataFrame(events)
    return schema.validate(frame)
