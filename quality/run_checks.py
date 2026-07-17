"""Run the Great Expectations warehouse suites against a live warehouse.

Usage:  python quality/run_checks.py  [--dsn postgresql://...]
Exit code 0 when every suite passes, 1 otherwise. Used by the integration
tests and runnable standalone against the compose stack.
"""

import argparse
import sys
import warnings

import great_expectations as gx
import pandas as pd
import psycopg2

from quality.suites import build_suites

DEFAULT_DSN = "postgresql://telestream:telestream@localhost:5432/telestream"


def load_tables(dsn: str) -> dict[str, pd.DataFrame]:
    tables = [
        "dim_subscriber",
        "dim_bundle",
        "dim_tower",
        "fact_recharges",
        "fact_bundle_sales",
        "fact_calls",
        "fact_data_usage",
        "tower_status_current",
    ]
    with psycopg2.connect(dsn) as conn, warnings.catch_warnings():
        # pandas warns that psycopg2 is not SQLAlchemy; fine for reads.
        warnings.simplefilter("ignore", UserWarning)
        return {t: pd.read_sql(f"SELECT * FROM {t}", conn) for t in tables}  # noqa: S608


def run(dsn: str) -> dict[str, bool]:
    tables = load_tables(dsn)
    suites = build_suites(tables)

    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas(name="warehouse")

    results: dict[str, bool] = {}
    for table, expectations in suites.items():
        asset = source.add_dataframe_asset(name=table)
        batch_def = asset.add_batch_definition_whole_dataframe(f"{table}_batch")
        batch = batch_def.get_batch(batch_parameters={"dataframe": tables[table]})
        suite = gx.ExpectationSuite(name=f"{table}_suite")
        for expectation in expectations:
            suite.add_expectation(expectation)
        outcome = batch.validate(suite)
        results[table] = bool(outcome.success)
        if not outcome.success:
            for res in outcome.results:
                config = res.expectation_config
                if not res.success and config is not None:
                    print(f"  FAILED {table}: {config.type} {dict(config.kwargs)}")
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    args = parser.parse_args()
    results = run(args.dsn)
    for table, ok in results.items():
        print(f"{'PASS' if ok else 'FAIL'}  {table}")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
