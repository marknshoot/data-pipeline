"""pytest configuration shared by every test module."""

from __future__ import annotations

import importlib.util

# The Spark tests import pyspark and need a JVM. The host has neither (Spark runs in
# the container, via `make spark-test`), so skip collecting them rather than failing
# the whole run at import time. In the container pyspark is installed and they are
# collected normally.
collect_ignore: list[str] = []
if importlib.util.find_spec("pyspark") is None:
    collect_ignore.append("test_clean_events.py")
