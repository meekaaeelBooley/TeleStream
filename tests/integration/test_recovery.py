"""Failure-mode tests: the pipeline must survive broker restarts, Spark
restarts, and warehouse outages without losing or duplicating data.

These tests restart containers, so they run last (file name sorts after
test_pipeline / test_quality) and need the docker CLI. Recovery relies on:
restart policies, Spark checkpointing, and idempotent (ON CONFLICT) sinks.
"""

import os
import shutil
import subprocess
import time

import psycopg2
import pytest

pytestmark = pytest.mark.integration

DSN = os.environ.get(
    "TELESTREAM_TEST_DSN", "postgresql://telestream:telestream@localhost:5432/telestream"
)

FACT_TABLES = ["fact_recharges", "fact_bundle_sales", "fact_calls", "fact_sms", "fact_data_usage"]

DOCKER = shutil.which("docker") or r"C:\Program Files\Docker\Docker\resources\bin\docker.exe"

if not shutil.which("docker") and not os.path.exists(DOCKER):
    pytest.skip("docker CLI not available", allow_module_level=True)


def docker(*args: str, timeout: int = 120) -> str:
    proc = subprocess.run(
        [DOCKER, *args], capture_output=True, text=True, timeout=timeout, check=True
    )
    return proc.stdout.strip()


def fact_total() -> int:
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        total = 0
        for table in FACT_TABLES:
            cur.execute(f"SELECT count(*) FROM {table}")  # noqa: S608 - fixed names
            total += int(cur.fetchone()[0])
        return total


def duplicate_event_ids() -> int:
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        dupes = 0
        for table in FACT_TABLES:
            cur.execute(f"SELECT count(*) - count(DISTINCT event_id) FROM {table}")  # noqa: S608
            dupes += int(cur.fetchone()[0])
        return dupes


def wait_until_growing(baseline: int, timeout: float = 300.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if fact_total() > baseline:
                return True
        except psycopg2.OperationalError:
            pass  # warehouse may still be coming back
        time.sleep(5)
    return False


def wait_for_health(container: str, timeout: float = 180.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = docker(
            "inspect",
            container,
            "--format",
            "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
        )
        if status in ("healthy", "running"):
            return True
        time.sleep(5)
    return False


def test_broker_restart_pipeline_recovers() -> None:
    baseline = fact_total()
    docker("restart", "telestream-kafka", timeout=180)
    assert wait_for_health("telestream-kafka"), "kafka never came back healthy"
    assert wait_until_growing(baseline), "no new facts after broker restart"
    assert duplicate_event_ids() == 0


def test_spark_restart_no_loss_or_duplicates() -> None:
    baseline = fact_total()
    docker("restart", "telestream-spark", timeout=180)
    assert wait_until_growing(baseline), "no new facts after spark restart"
    assert duplicate_event_ids() == 0


def test_warehouse_outage_recovery() -> None:
    baseline = fact_total()
    docker("stop", "telestream-postgres", timeout=120)
    time.sleep(20)  # let spark batches fail against the missing warehouse
    docker("start", "telestream-postgres")
    assert wait_for_health("telestream-postgres"), "postgres never came back healthy"
    # Spark's driver exits on sink failure; the container restart policy plus
    # checkpoints resume it. Give the JVM time to boot again.
    assert wait_until_growing(baseline, timeout=360.0), "no new facts after warehouse outage"
    assert duplicate_event_ids() == 0
