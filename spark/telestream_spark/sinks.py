"""PostgreSQL sink: idempotent fact upserts, dimension maintenance, DLQ
persistence, and rollup refresh.

Pure psycopg2 — no Spark dependency — so the logic is testable against any
Postgres. Idempotency contract: every fact insert is ON CONFLICT (event_id)
DO NOTHING, and rollups are recomputed from facts for the affected minute
range (delete + insert), so replaying a batch can never double-count.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg2
import psycopg2.extras

Event = dict[str, Any]


def connect(dsn: str) -> "psycopg2.extensions.connection":
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def _ts(event: Event) -> datetime:
    ts = datetime.fromisoformat(str(event["timestamp"]))
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


def known_subscriber_ids(conn: "psycopg2.extensions.connection", ids: list[int]) -> set[int]:
    if not ids:
        return set()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT subscriber_id FROM dim_subscriber WHERE subscriber_id = ANY(%s)", (ids,)
        )
        return {row[0] for row in cur.fetchall()}


def upsert_subscribers(conn: "psycopg2.extensions.connection", events: list[Event]) -> None:
    if not events:
        return
    rows = [(e["subscriber_id"], e["msisdn"], e["plan"], e["province"], _ts(e)) for e in events]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dim_subscriber (subscriber_id, msisdn, plan, province, created_at)
            VALUES %s
            ON CONFLICT (subscriber_id) DO NOTHING
            """,
            rows,
        )


def update_tower_status(conn: "psycopg2.extensions.connection", events: list[Event]) -> None:
    if not events:
        return
    rows = [
        (
            e["tower_id"],
            e["technology"],
            e["signal_strength"],
            e["connected_devices"],
            e["status"],
            _ts(e),
        )
        for e in events
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO tower_status_current
                (tower_id, technology, signal_strength, connected_devices, status, updated_at)
            VALUES %s
            ON CONFLICT (tower_id) DO UPDATE SET
                technology = EXCLUDED.technology,
                signal_strength = EXCLUDED.signal_strength,
                connected_devices = EXCLUDED.connected_devices,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at
            WHERE EXCLUDED.updated_at > tower_status_current.updated_at
            """,
            rows,
        )


_FACT_INSERTS: dict[str, tuple[str, tuple[str, ...]]] = {
    "airtime_purchase": (
        """
        INSERT INTO fact_recharges
            (event_id, subscriber_key, date_key, event_timestamp, amount, payment_method)
        SELECT v.event_id::uuid, s.subscriber_key,
               to_char(v.ts::timestamptz AT TIME ZONE 'UTC', 'YYYYMMDD')::int,
               v.ts::timestamptz, v.amount, v.payment_method
        FROM (VALUES %s) AS v(event_id, subscriber_id, ts, amount, payment_method)
        JOIN dim_subscriber s ON s.subscriber_id = v.subscriber_id
        ON CONFLICT (event_id) DO NOTHING
        """,
        ("event_id", "subscriber_id", "timestamp", "amount", "payment_method"),
    ),
    "bundle_purchase": (
        """
        INSERT INTO fact_bundle_sales
            (event_id, subscriber_key, bundle_key, date_key, event_timestamp, price)
        SELECT v.event_id::uuid, s.subscriber_key, b.bundle_key,
               to_char(v.ts::timestamptz AT TIME ZONE 'UTC', 'YYYYMMDD')::int,
               v.ts::timestamptz, v.price
        FROM (VALUES %s) AS v(event_id, subscriber_id, bundle_code, ts, price)
        JOIN dim_subscriber s ON s.subscriber_id = v.subscriber_id
        JOIN dim_bundle b ON b.bundle_code = v.bundle_code
        ON CONFLICT (event_id) DO NOTHING
        """,
        ("event_id", "subscriber_id", "bundle_code", "timestamp", "price"),
    ),
    "voice_call": (
        """
        INSERT INTO fact_calls
            (event_id, caller_key, tower_key, date_key, event_timestamp,
             receiver_msisdn, duration_seconds, dropped)
        SELECT v.event_id::uuid, s.subscriber_key, t.tower_key,
               to_char(v.ts::timestamptz AT TIME ZONE 'UTC', 'YYYYMMDD')::int,
               v.ts::timestamptz, v.receiver_msisdn, v.duration_seconds, v.dropped
        FROM (VALUES %s)
            AS v(event_id, caller_id, tower_id, ts, receiver_msisdn, duration_seconds, dropped)
        JOIN dim_subscriber s ON s.subscriber_id = v.caller_id
        JOIN dim_tower t ON t.tower_id = v.tower_id
        ON CONFLICT (event_id) DO NOTHING
        """,
        (
            "event_id",
            "caller_id",
            "tower_id",
            "timestamp",
            "receiver_msisdn",
            "duration_seconds",
            "dropped",
        ),
    ),
    "sms": (
        """
        INSERT INTO fact_sms
            (event_id, sender_key, date_key, event_timestamp, receiver_msisdn, length)
        SELECT v.event_id::uuid, s.subscriber_key,
               to_char(v.ts::timestamptz AT TIME ZONE 'UTC', 'YYYYMMDD')::int,
               v.ts::timestamptz, v.receiver_msisdn, v.length
        FROM (VALUES %s) AS v(event_id, sender_id, ts, receiver_msisdn, length)
        JOIN dim_subscriber s ON s.subscriber_id = v.sender_id
        ON CONFLICT (event_id) DO NOTHING
        """,
        ("event_id", "sender_id", "timestamp", "receiver_msisdn", "length"),
    ),
    "data_usage": (
        """
        INSERT INTO fact_data_usage
            (event_id, subscriber_key, tower_key, date_key, event_timestamp,
             mb_used, session_seconds, technology)
        SELECT v.event_id::uuid, s.subscriber_key, t.tower_key,
               to_char(v.ts::timestamptz AT TIME ZONE 'UTC', 'YYYYMMDD')::int,
               v.ts::timestamptz, v.mb_used, v.session_seconds, v.technology
        FROM (VALUES %s)
            AS v(event_id, subscriber_id, tower_id, ts, mb_used, session_seconds, technology)
        JOIN dim_subscriber s ON s.subscriber_id = v.subscriber_id
        JOIN dim_tower t ON t.tower_id = v.tower_id
        ON CONFLICT (event_id) DO NOTHING
        """,
        (
            "event_id",
            "subscriber_id",
            "tower_id",
            "timestamp",
            "mb_used",
            "session_seconds",
            "technology",
        ),
    ),
}

SUBSCRIBER_FIELD: dict[str, str] = {
    "airtime_purchase": "subscriber_id",
    "bundle_purchase": "subscriber_id",
    "voice_call": "caller_id",
    "sms": "sender_id",
    "data_usage": "subscriber_id",
}


def insert_facts(
    conn: "psycopg2.extensions.connection", event_type: str, events: list[Event]
) -> list[Event]:
    """Insert fact rows; returns events referencing unknown subscribers
    (referential violations, destined for the DLQ)."""
    if not events:
        return []
    sub_field = SUBSCRIBER_FIELD[event_type]
    known = known_subscriber_ids(conn, [int(e[sub_field]) for e in events])
    insertable = [e for e in events if int(e[sub_field]) in known]
    unknown = [e for e in events if int(e[sub_field]) not in known]

    if insertable:
        sql, fields = _FACT_INSERTS[event_type]
        rows = [tuple(_ts(e) if f == "timestamp" else e[f] for f in fields) for e in insertable]
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows)
    return unknown


def insert_dlq(conn: "psycopg2.extensions.connection", events: list[Event]) -> None:
    if not events:
        return
    rows = [
        (
            e["event_id"],
            e["source_topic"],
            e["rejection_reason"],
            e["original_payload"],
            _ts(e),
        )
        for e in events
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dlq_records
                (event_id, source_topic, rejection_reason, original_payload, rejected_at)
            VALUES %s
            ON CONFLICT (event_id) DO NOTHING
            """,
            rows,
        )


_ROLLUP_REFRESH = """
DELETE FROM agg_revenue_minute WHERE minute BETWEEN %(lo)s AND %(hi)s;
INSERT INTO agg_revenue_minute (minute, province, revenue_type, total_amount, txn_count)
SELECT date_trunc('minute', r.event_timestamp), s.province, r.revenue_type,
       sum(r.amount), count(*)
FROM (
    SELECT subscriber_key, event_timestamp, amount, 'AIRTIME' AS revenue_type
    FROM fact_recharges
    UNION ALL
    SELECT subscriber_key, event_timestamp, price, 'BUNDLE'
    FROM fact_bundle_sales
) r
JOIN dim_subscriber s USING (subscriber_key)
WHERE r.event_timestamp >= %(lo)s AND r.event_timestamp < %(hi_next)s
GROUP BY 1, 2, 3;

DELETE FROM agg_calls_minute WHERE minute BETWEEN %(lo)s AND %(hi)s;
INSERT INTO agg_calls_minute (minute, tower_key, call_count, dropped_count, total_duration)
SELECT date_trunc('minute', event_timestamp), tower_key, count(*),
       count(*) FILTER (WHERE dropped), sum(duration_seconds)
FROM fact_calls
WHERE event_timestamp >= %(lo)s AND event_timestamp < %(hi_next)s
GROUP BY 1, 2;

DELETE FROM agg_data_minute WHERE minute BETWEEN %(lo)s AND %(hi)s;
INSERT INTO agg_data_minute (minute, tower_key, technology, total_mb, session_count)
SELECT date_trunc('minute', event_timestamp), tower_key, technology, sum(mb_used), count(*)
FROM fact_data_usage
WHERE event_timestamp >= %(lo)s AND event_timestamp < %(hi_next)s
GROUP BY 1, 2, 3;

DELETE FROM agg_dlq_minute WHERE minute BETWEEN %(lo)s AND %(hi)s;
INSERT INTO agg_dlq_minute (minute, source_topic, rejection_reason, record_count)
SELECT date_trunc('minute', rejected_at), source_topic, rejection_reason, count(*)
FROM dlq_records
WHERE rejected_at >= %(lo)s AND rejected_at < %(hi_next)s
GROUP BY 1, 2, 3;
"""


def refresh_rollups(conn: "psycopg2.extensions.connection", lo: datetime, hi: datetime) -> None:
    """Recompute every rollup for the affected minute range from facts —
    delete + insert, so replays are idempotent."""
    lo_min = lo.replace(second=0, microsecond=0)
    hi_min = hi.replace(second=0, microsecond=0)
    hi_next = hi_min + timedelta(minutes=1)
    with conn.cursor() as cur:
        cur.execute(_ROLLUP_REFRESH, {"lo": lo_min, "hi": hi_min, "hi_next": hi_next})
