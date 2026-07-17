"""Great Expectations suites against the live warehouse: they must pass on
real pipeline output, and they must catch a deliberately planted bad row."""

import os
import sys
import uuid
from pathlib import Path

import psycopg2
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.integration

DSN = os.environ.get(
    "TELESTREAM_TEST_DSN", "postgresql://telestream:telestream@localhost:5432/telestream"
)


def run_checks() -> dict[str, bool]:
    from quality.run_checks import run

    return run(DSN)


def test_warehouse_passes_all_suites() -> None:
    results = run_checks()
    assert results, "no suites ran"
    failing = [table for table, ok in results.items() if not ok]
    assert not failing, f"quality suites failed for: {failing}"


def test_suites_catch_planted_bad_row() -> None:
    """A negative recharge (schema allows it; quality must not) is planted,
    detected, and removed."""
    bad_event_id = str(uuid.uuid4())
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fact_recharges
                    (event_id, subscriber_key, date_key, event_timestamp, amount, payment_method)
                SELECT %s, subscriber_key,
                       to_char(now() AT TIME ZONE 'UTC', 'YYYYMMDD')::int, now(), -50.0, 'Card'
                FROM dim_subscriber LIMIT 1
                """,
                (bad_event_id,),
            )
        results = run_checks()
        assert results["fact_recharges"] is False, (
            "quality suite failed to flag a negative recharge"
        )
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM fact_recharges WHERE event_id = %s", (bad_event_id,))
        conn.close()


def test_suites_pass_again_after_cleanup() -> None:
    results = run_checks()
    assert results["fact_recharges"] is True
