import sys
from pathlib import Path

# The spark package is deployed into the Spark image, not pip-installed
# locally; make it importable for unit tests.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spark"))
