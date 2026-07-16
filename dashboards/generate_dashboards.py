"""Generate Grafana dashboard JSON from compact panel definitions.

Dashboards are code: edit the definitions here, regenerate, commit both.
Run from the repo root:  python dashboards/generate_dashboards.py
A unit test asserts the committed JSON matches this script's output.
"""

import json
from pathlib import Path
from typing import Any

PG = {"type": "postgres", "uid": "telestream-pg"}
PROM = {"type": "prometheus", "uid": "telestream-prom"}

GRID_W = 24


def sql_target(sql: str, fmt: str = "time_series") -> dict[str, Any]:
    return {
        "datasource": PG,
        "format": fmt,
        "rawQuery": True,
        "rawSql": sql.strip(),
        "refId": "A",
    }


def prom_target(expr: str, legend: str = "") -> dict[str, Any]:
    return {
        "datasource": PROM,
        "expr": expr,
        "legendFormat": legend,
        "refId": "A",
    }


def panel(
    title: str,
    kind: str,
    target: dict[str, Any],
    *,
    w: int = 8,
    h: int = 7,
    unit: str | None = None,
    description: str = "",
) -> dict[str, Any]:
    defaults: dict[str, Any] = {"color": {"mode": "palette-classic"}}
    if unit:
        defaults["unit"] = unit
    ds = target.get("datasource", PG)
    return {
        "title": title,
        "description": description,
        "type": kind,
        "datasource": ds,
        "targets": [target],
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {},
        "gridPos": {"w": w, "h": h},
    }


def dashboard(uid: str, title: str, panels: list[dict[str, Any]]) -> dict[str, Any]:
    x = y = row_h = 0
    for p in panels:
        w, h = p["gridPos"]["w"], p["gridPos"]["h"]
        if x + w > GRID_W:
            x, y = 0, y + row_h
            row_h = 0
        p["gridPos"].update({"x": x, "y": y})
        x += w
        row_h = max(row_h, h)
    for i, p in enumerate(panels):
        p["id"] = i + 1
    return {
        "uid": uid,
        "title": title,
        "tags": ["telestream"],
        "timezone": "utc",
        "schemaVersion": 39,
        "refresh": "10s",
        "time": {"from": "now-30m", "to": "now"},
        "panels": panels,
        "editable": False,
    }


# ---------------------------------------------------------------------------
# Dashboard definitions
# ---------------------------------------------------------------------------

EXECUTIVE = dashboard(
    "telestream-executive",
    "Executive Overview",
    [
        panel(
            "Active Subscribers",
            "stat",
            sql_target("SELECT count(*) FROM dim_subscriber WHERE is_active", "table"),
            w=6,
            h=5,
        ),
        panel(
            "Revenue (last hour)",
            "stat",
            sql_target(
                """
                SELECT coalesce(sum(total_amount), 0) FROM agg_revenue_minute
                WHERE minute > now() - interval '1 hour'
                """,
                "table",
            ),
            w=6,
            h=5,
            unit="currencyZAR",
        ),
        panel(
            "Calls (last hour)",
            "stat",
            sql_target(
                """
                SELECT coalesce(sum(call_count), 0) FROM agg_calls_minute
                WHERE minute > now() - interval '1 hour'
                """,
                "table",
            ),
            w=6,
            h=5,
        ),
        panel(
            "Rejected Events (last hour)",
            "stat",
            sql_target(
                """
                SELECT coalesce(sum(record_count), 0) FROM agg_dlq_minute
                WHERE minute > now() - interval '1 hour'
                """,
                "table",
            ),
            w=6,
            h=5,
        ),
        panel(
            "Revenue per Minute",
            "timeseries",
            sql_target(
                """
                SELECT minute AS time, revenue_type AS metric, sum(total_amount) AS value
                FROM agg_revenue_minute
                WHERE $__timeFilter(minute)
                GROUP BY 1, 2 ORDER BY 1
                """
            ),
            w=12,
            unit="currencyZAR",
        ),
        panel(
            "Calls per Minute",
            "timeseries",
            sql_target(
                """
                SELECT minute AS time, sum(call_count) AS calls, sum(dropped_count) AS dropped
                FROM agg_calls_minute
                WHERE $__timeFilter(minute)
                GROUP BY 1 ORDER BY 1
                """
            ),
            w=12,
        ),
        panel(
            "New Subscribers per Minute",
            "timeseries",
            sql_target(
                """
                SELECT date_trunc('minute', created_at) AS time, count(*) AS subscribers
                FROM dim_subscriber
                WHERE $__timeFilter(created_at)
                GROUP BY 1 ORDER BY 1
                """
            ),
            w=12,
        ),
        panel(
            "Data Usage per Minute (MB)",
            "timeseries",
            sql_target(
                """
                SELECT minute AS time, sum(total_mb) AS mb
                FROM agg_data_minute
                WHERE $__timeFilter(minute)
                GROUP BY 1 ORDER BY 1
                """
            ),
            w=12,
            unit="decmbytes",
        ),
    ],
)

NETWORK = dashboard(
    "telestream-network",
    "Network Operations",
    [
        panel(
            "Tower Status",
            "table",
            sql_target(
                """
                SELECT t.tower_name AS "Tower", c.technology AS "Tech",
                       c.signal_strength AS "Signal", c.connected_devices AS "Devices",
                       c.status AS "Status", c.updated_at AS "Updated"
                FROM tower_status_current c
                JOIN dim_tower t USING (tower_id)
                ORDER BY c.signal_strength ASC
                """,
                "table",
            ),
            w=12,
            h=10,
        ),
        panel(
            "Tower Load — Data Sessions per Minute",
            "timeseries",
            sql_target(
                """
                SELECT m.minute AS time, t.tower_name AS metric, sum(m.session_count) AS value
                FROM agg_data_minute m JOIN dim_tower t USING (tower_key)
                WHERE $__timeFilter(m.minute)
                GROUP BY 1, 2 ORDER BY 1
                """
            ),
            w=12,
            h=10,
        ),
        panel(
            "Dropped Calls per Minute",
            "timeseries",
            sql_target(
                """
                SELECT minute AS time, sum(dropped_count) AS dropped
                FROM agg_calls_minute
                WHERE $__timeFilter(minute)
                GROUP BY 1 ORDER BY 1
                """
            ),
            w=12,
        ),
        panel(
            "Traffic by Technology (MB per minute)",
            "timeseries",
            sql_target(
                """
                SELECT minute AS time, technology AS metric, sum(total_mb) AS value
                FROM agg_data_minute
                WHERE $__timeFilter(minute)
                GROUP BY 1, 2 ORDER BY 1
                """
            ),
            w=12,
            unit="decmbytes",
        ),
    ],
)

SALES = dashboard(
    "telestream-sales",
    "Sales & Revenue",
    [
        panel(
            "Airtime Revenue per Minute",
            "timeseries",
            sql_target(
                """
                SELECT minute AS time, sum(total_amount) AS airtime
                FROM agg_revenue_minute WHERE revenue_type = 'AIRTIME'
                  AND $__timeFilter(minute)
                GROUP BY 1 ORDER BY 1
                """
            ),
            w=12,
            unit="currencyZAR",
        ),
        panel(
            "Bundle Revenue per Minute",
            "timeseries",
            sql_target(
                """
                SELECT minute AS time, sum(total_amount) AS bundles
                FROM agg_revenue_minute WHERE revenue_type = 'BUNDLE'
                  AND $__timeFilter(minute)
                GROUP BY 1 ORDER BY 1
                """
            ),
            w=12,
            unit="currencyZAR",
        ),
        panel(
            "Top Bundles (last hour)",
            "barchart",
            sql_target(
                """
                SELECT b.bundle_name AS bundle, count(*) AS sales
                FROM fact_bundle_sales f JOIN dim_bundle b USING (bundle_key)
                WHERE f.event_timestamp > now() - interval '1 hour'
                GROUP BY 1 ORDER BY 2 DESC LIMIT 8
                """,
                "table",
            ),
            w=12,
            h=9,
        ),
        panel(
            "Revenue by Province (last hour)",
            "barchart",
            sql_target(
                """
                SELECT province, sum(total_amount) AS revenue
                FROM agg_revenue_minute
                WHERE minute > now() - interval '1 hour'
                GROUP BY 1 ORDER BY 2 DESC
                """,
                "table",
            ),
            w=12,
            h=9,
            unit="currencyZAR",
        ),
        panel(
            "Average Recharge (last hour)",
            "stat",
            sql_target(
                """
                SELECT coalesce(avg(amount), 0) FROM fact_recharges
                WHERE event_timestamp > now() - interval '1 hour'
                """,
                "table",
            ),
            w=8,
            h=5,
            unit="currencyZAR",
        ),
        panel(
            "Recharges by Payment Method (last hour)",
            "piechart",
            sql_target(
                """
                SELECT payment_method, count(*) FROM fact_recharges
                WHERE event_timestamp > now() - interval '1 hour'
                GROUP BY 1
                """,
                "table",
            ),
            w=8,
            h=5,
        ),
        panel(
            "Failed Payments (last hour)",
            "stat",
            sql_target(
                """
                SELECT count(*) FROM dlq_records
                WHERE rejection_reason = 'PAYMENT_DECLINED'
                  AND rejected_at > now() - interval '1 hour'
                """,
                "table",
            ),
            w=8,
            h=5,
        ),
    ],
)

CUSTOMER = dashboard(
    "telestream-customer",
    "Customer Insights",
    [
        panel(
            "Subscribers by Province",
            "barchart",
            sql_target(
                "SELECT province, count(*) FROM dim_subscriber GROUP BY 1 ORDER BY 2 DESC",
                "table",
            ),
            w=12,
            h=9,
        ),
        panel(
            "Subscribers by Plan",
            "piechart",
            sql_target(
                "SELECT plan, count(*) FROM dim_subscriber GROUP BY 1",
                "table",
            ),
            w=12,
            h=9,
        ),
        panel(
            "ARPU (last 24h)",
            "stat",
            sql_target(
                """
                SELECT coalesce(sum(r.total_amount), 0)
                       / greatest(count(DISTINCT s.subscriber_key), 1)
                FROM dim_subscriber s
                LEFT JOIN agg_revenue_minute r ON r.minute > now() - interval '24 hours'
                """,
                "table",
            ),
            w=8,
            h=5,
            unit="currencyZAR",
            description="Revenue in window divided by subscriber base",
        ),
        panel(
            "Top Data Users (last hour)",
            "table",
            sql_target(
                """
                SELECT s.msisdn AS "MSISDN", s.province AS "Province",
                       round(sum(f.mb_used), 1) AS "MB Used"
                FROM fact_data_usage f
                JOIN dim_subscriber s ON s.subscriber_key = f.subscriber_key
                WHERE f.event_timestamp > now() - interval '1 hour'
                GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10
                """,
                "table",
            ),
            w=16,
            h=9,
        ),
    ],
)

PIPELINE = dashboard(
    "telestream-pipeline",
    "Pipeline Operations",
    [
        panel(
            "Rejections per Minute by Reason",
            "timeseries",
            sql_target(
                """
                SELECT minute AS time, rejection_reason AS metric, sum(record_count) AS value
                FROM agg_dlq_minute
                WHERE $__timeFilter(minute)
                GROUP BY 1, 2 ORDER BY 1
                """
            ),
            w=12,
            h=9,
        ),
        panel(
            "Latest Dead-Letter Records",
            "table",
            sql_target(
                """
                SELECT rejected_at AS "Rejected", source_topic AS "Topic",
                       rejection_reason AS "Reason",
                       left(original_payload::text, 120) AS "Payload"
                FROM dlq_records ORDER BY rejected_at DESC LIMIT 25
                """,
                "table",
            ),
            w=12,
            h=9,
        ),
        panel(
            "Kafka Consumer Lag",
            "timeseries",
            prom_target("sum(kafka_consumergroup_lag) by (consumergroup)", "{{consumergroup}}"),
            w=12,
        ),
        panel(
            "Messages In per Topic (rate)",
            "timeseries",
            prom_target(
                "sum(rate(kafka_topic_partition_current_offset[1m])) by (topic)", "{{topic}}"
            ),
            w=12,
        ),
    ],
)

ALL = [EXECUTIVE, NETWORK, SALES, CUSTOMER, PIPELINE]


def main() -> None:
    out_dir = Path(__file__).parent
    for dash in ALL:
        path = out_dir / f"{dash['uid'].removeprefix('telestream-')}.json"
        path.write_text(json.dumps(dash, indent=2, sort_keys=True) + "\n", newline="\n")
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
