"""Sink-layer logic that must hold without a database."""

from typing import Any
from unittest.mock import MagicMock

from telestream_spark import sinks


def tower_event(tower_id: str, ts: str, signal: int) -> dict[str, Any]:
    return {
        "tower_id": tower_id,
        "technology": "5G",
        "signal_strength": signal,
        "connected_devices": 100,
        "status": "HEALTHY",
        "timestamp": ts,
    }


def test_tower_upsert_collapses_to_latest_per_tower() -> None:
    """Multiple same-tower events in one batch must become a single upsert row
    (Postgres rejects multi-row ON CONFLICT UPDATE hitting one key twice)."""
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    events = [
        tower_event("CPT-CBD-001", "2026-07-17T10:00:00+00:00", 80),
        tower_event("CPT-CBD-001", "2026-07-17T10:00:05+00:00", 55),
        tower_event("JHB-SDT-001", "2026-07-17T10:00:01+00:00", 90),
        tower_event("CPT-CBD-001", "2026-07-17T09:59:59+00:00", 99),
    ]

    calls: list[list[tuple[Any, ...]]] = []
    original = sinks.psycopg2.extras.execute_values
    try:
        sinks.psycopg2.extras.execute_values = (  # type: ignore[assignment]
            lambda cur, sql, rows: calls.append(list(rows))
        )
        sinks.update_tower_status(conn, events)
    finally:
        sinks.psycopg2.extras.execute_values = original  # type: ignore[assignment]

    assert len(calls) == 1
    rows = calls[0]
    by_tower = {row[0]: row for row in rows}
    assert len(rows) == 2, "expected one row per distinct tower"
    # The latest CPT-CBD-001 event (10:00:05, signal 55) must win.
    assert by_tower["CPT-CBD-001"][2] == 55
    assert cursor is not None
