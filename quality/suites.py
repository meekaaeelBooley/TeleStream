"""Great Expectations suites for the warehouse — the at-rest quality layer.

The in-stream layer (pandera contracts + business rules) guards what enters;
these suites verify what actually landed: no nulls or duplicate event ids,
value ranges hold, references resolve, timestamps are sane. Suites are built
per run because referential value sets and the clock are read at runtime.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import great_expectations.expectations as gxe
import pandas as pd
from telestream_producer.catalog import PLANS, PROVINCES, TECHNOLOGIES, TOWER_STATUSES

MAX_CALL_SECONDS = 6 * 60 * 60
MAX_SESSION_MB = 10240.0
MAX_RECHARGE = 1000.0
MSISDN_REGEX = r"^27\d{9}$"


def _ts_ceiling() -> datetime:
    return datetime.now(UTC) + timedelta(minutes=1)


def build_suites(tables: dict[str, pd.DataFrame]) -> dict[str, list[Any]]:
    """Expectations per warehouse table, given the loaded tables (dimension
    contents feed the referential value sets)."""
    bundle_keys = tables["dim_bundle"]["bundle_key"].tolist()
    tower_keys = tables["dim_tower"]["tower_key"].tolist()
    subscriber_keys = tables["dim_subscriber"]["subscriber_key"].tolist()

    def fact_common(subscriber_col: str) -> list[Any]:
        return [
            gxe.ExpectColumnValuesToNotBeNull(column="event_id"),
            gxe.ExpectColumnValuesToBeUnique(column="event_id"),
            gxe.ExpectColumnValuesToNotBeNull(column=subscriber_col),
            gxe.ExpectColumnValuesToBeInSet(column=subscriber_col, value_set=subscriber_keys),
            gxe.ExpectColumnMaxToBeBetween(column="event_timestamp", max_value=_ts_ceiling()),
        ]

    return {
        "dim_subscriber": [
            gxe.ExpectColumnValuesToNotBeNull(column="subscriber_id"),
            gxe.ExpectColumnValuesToBeUnique(column="subscriber_id"),
            gxe.ExpectColumnValuesToBeUnique(column="msisdn"),
            gxe.ExpectColumnValuesToMatchRegex(column="msisdn", regex=MSISDN_REGEX),
            gxe.ExpectColumnValuesToBeInSet(column="plan", value_set=list(PLANS)),
            gxe.ExpectColumnValuesToBeInSet(column="province", value_set=list(PROVINCES)),
        ],
        "fact_recharges": [
            *fact_common("subscriber_key"),
            gxe.ExpectColumnValuesToBeBetween(
                column="amount", min_value=0, strict_min=True, max_value=MAX_RECHARGE
            ),
        ],
        "fact_bundle_sales": [
            *fact_common("subscriber_key"),
            gxe.ExpectColumnValuesToBeInSet(column="bundle_key", value_set=bundle_keys),
            gxe.ExpectColumnValuesToBeBetween(column="price", min_value=0, strict_min=True),
        ],
        "fact_calls": [
            *fact_common("caller_key"),
            gxe.ExpectColumnValuesToBeInSet(column="tower_key", value_set=tower_keys),
            gxe.ExpectColumnValuesToBeBetween(
                column="duration_seconds", min_value=0, max_value=MAX_CALL_SECONDS
            ),
        ],
        "fact_data_usage": [
            *fact_common("subscriber_key"),
            gxe.ExpectColumnValuesToBeInSet(column="tower_key", value_set=tower_keys),
            gxe.ExpectColumnValuesToBeBetween(
                column="mb_used", min_value=0, strict_min=True, max_value=MAX_SESSION_MB
            ),
            gxe.ExpectColumnValuesToBeInSet(column="technology", value_set=list(TECHNOLOGIES)),
        ],
        "tower_status_current": [
            gxe.ExpectColumnValuesToBeBetween(column="signal_strength", min_value=0, max_value=100),
            gxe.ExpectColumnValuesToBeInSet(column="status", value_set=list(TOWER_STATUSES)),
        ],
    }
