"""warehouse/seeds.sql is generated from the producer catalog — this test
fails when the committed file is stale relative to the catalog."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

sys.path.insert(0, str(REPO_ROOT / "warehouse"))

from generate_seeds import render_seeds  # noqa: E402


def test_committed_seeds_match_catalog() -> None:
    committed = (REPO_ROOT / "warehouse" / "seeds.sql").read_text(encoding="utf-8")
    assert committed == render_seeds(), (
        "seeds.sql is stale — run: python warehouse/generate_seeds.py"
    )
