"""TeleStream Spark streaming package.

`rules` and `schemas` are pure Python (no pyspark import) so they can be
unit-tested anywhere; only the streaming job itself needs a Spark runtime.
"""

__version__ = "0.1.0"
