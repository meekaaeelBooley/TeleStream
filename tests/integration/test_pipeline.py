"""End-to-end assertions against the running docker compose stack.

Run with:  docker compose up -d --build  then  pytest -m integration
Connects to the published ports on localhost (Postgres 5432, Kafka 29092).
"""

import os
import time
from collections.abc import Callable

import psycopg2
import pytest

pytestmark = pytest.mark.integration

DSN = os.environ.get(
    "TELESTREAM_TEST_DSN", "postgresql://telestream:telestream@localhost:5432/telestream"
)
KAFKA = os.environ.get("TELESTREAM_TEST_KAFKA", "localhost:29092")

FACT_TABLES = ["fact_recharges", "fact_bundle_sales", "fact_calls", "fact_sms", "fact_data_usage"]

EXPECTED_TOPICS = {
    "subscriber-created",
    "airtime-purchases",
    "bundle-purchases",
    "voice-calls",
    "sms",
    "data-usage",
    "tower-events",
    "failed-transactions",
}


def wait_for(check: Callable[[], bool], timeout: float = 180.0, interval: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return True
        time.sleep(interval)
    return check()


@pytest.fixture(scope="module")
def conn():
    connection = None
    error: Exception | None = None
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and connection is None:
        try:
            connection = psycopg2.connect(DSN)
        except psycopg2.OperationalError as exc:  # postgres still starting
            error = exc
            time.sleep(3)
    if connection is None:
        raise AssertionError(f"postgres not reachable at {DSN}: {error}")
    connection.autocommit = True
    yield connection
    connection.close()


def count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")  # noqa: S608 - fixed table names
        return int(cur.fetchone()[0])


def test_all_topics_exist() -> None:
    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(bootstrap_servers=KAFKA)
    try:
        assert set(admin.list_topics()) >= EXPECTED_TOPICS
    finally:
        admin.close()


def test_subscriber_dimension_populates(conn) -> None:
    assert wait_for(lambda: count(conn, "dim_subscriber") >= 400), (
        "dim_subscriber never reached seed volume"
    )


@pytest.mark.parametrize("table", FACT_TABLES)
def test_fact_tables_receive_rows(conn, table: str) -> None:
    assert wait_for(lambda: count(conn, table) > 0), f"{table} stayed empty"


def test_dlq_captures_rejections(conn) -> None:
    def reasons() -> set[str]:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT rejection_reason FROM dlq_records")
            return {row[0] for row in cur.fetchall()}

    # Producer-side payment failures reach the warehouse one hop earlier than
    # Spark-detected violations (events query -> DLQ topic -> DLQ query), so
    # each category gets its own wait rather than one snapshot.
    assert wait_for(lambda: "PAYMENT_DECLINED" in reasons()), "no payment failures in DLQ"
    assert wait_for(
        lambda: any(r.startswith(("RULE_VIOLATION", "SCHEMA_VIOLATION")) for r in reasons())
    ), "no rule/schema violations in DLQ"


def test_rollups_populate(conn) -> None:
    assert wait_for(lambda: count(conn, "agg_revenue_minute") > 0)
    assert wait_for(lambda: count(conn, "agg_calls_minute") > 0)


def test_no_orphan_fact_keys(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM fact_calls f
            LEFT JOIN dim_subscriber s ON s.subscriber_key = f.caller_key
            LEFT JOIN dim_tower t ON t.tower_key = f.tower_key
            WHERE s.subscriber_key IS NULL OR t.tower_key IS NULL
            """
        )
        assert cur.fetchone()[0] == 0


def test_tower_status_current_updates(conn) -> None:
    assert wait_for(lambda: count(conn, "tower_status_current") > 0)


def test_no_duplicate_event_ids(conn) -> None:
    for table in FACT_TABLES:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) - count(DISTINCT event_id) FROM {table}"  # noqa: S608
            )
            assert cur.fetchone()[0] == 0, f"duplicate event_ids in {table}"
