"""dashboards/*.json are generated from generate_dashboards.py — this test
fails when a committed dashboard is stale relative to its definition."""

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

sys.path.insert(0, str(REPO_ROOT / "dashboards"))

from generate_dashboards import ALL  # noqa: E402


def test_committed_dashboards_match_definitions() -> None:
    for dash in ALL:
        name = dash["uid"].removeprefix("telestream-")
        committed = json.loads((REPO_ROOT / "dashboards" / f"{name}.json").read_text())
        assert committed == dash, (
            f"{name}.json is stale — run: python dashboards/generate_dashboards.py"
        )


def test_dashboard_uids_unique() -> None:
    uids = [d["uid"] for d in ALL]
    assert len(uids) == len(set(uids))


def test_panels_have_positive_dimensions() -> None:
    for dash in ALL:
        for panel in dash["panels"]:
            grid = panel["gridPos"]
            assert grid["w"] > 0 and grid["h"] > 0
            assert grid["x"] + grid["w"] <= 24
