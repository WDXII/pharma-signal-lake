"""
conftest.py — pytest fixtures shared across tests/test_signals.py.

Tests run INSIDE Databricks via tests/run_unit_tests.py, against the
cluster's already-running Spark session. SparkSession.builder.getOrCreate()
attaches to that existing session directly, so no local Java/Spark install
is required (see DEVELOPER.md Issue 2 for why a local install is painful
on this machine's Python 3.13).
"""

import os
import sys

import pytest

# Make src/signal_math.py importable as `from signal_math import ...`
# without needing the package installed. databricks.yml syncs src/ and
# tests/ as siblings under the same workspace folder.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()
