# Databricks notebook source
# MAGIC %md
# MAGIC # run_unit_tests — Execute tests/test_signals.py Inside Databricks
# MAGIC **Project:** PharmaSignalLake
# MAGIC **Purpose:** Run the pytest suite for `src/signal_math.py` (PRR/ROR/chi_sq
# MAGIC math and case dedup) against the cluster's already-running Spark session.
# MAGIC
# MAGIC Databricks Free Edition serverless compute has no local Java, so pytest
# MAGIC cannot run from this machine — this notebook installs pytest on the
# MAGIC cluster and runs the suite here instead. Every test uses
# MAGIC `SparkSession.builder.getOrCreate()` (see `tests/conftest.py`), which
# MAGIC attaches to this notebook's own Spark session rather than starting a new
# MAGIC one, so no local Spark/Java install is needed either.
# MAGIC
# MAGIC Read-only tooling. This notebook does not read or write any pipeline
# MAGIC table and is NOT part of `99_run_pipeline`.

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

import pytest

# This notebook lives in tests/ itself, so its own working directory is the
# test suite's directory (same os.getcwd() assumption used in
# notebooks/02_silver_clean.py and notebooks/03_gold_signals.py for ../src).
TESTS_DIR = os.getcwd()

print("=" * 60)
print("  PharmaSignalLake — Unit Tests (src/signal_math.py)")
print("=" * 60)

sys.dont_write_bytecode = True          # Workspace files don't allow __pycache__ writes
exit_code = pytest.main(["-v", "-p", "no:cacheprovider", TESTS_DIR])   # no .pytest_cache either

print("=" * 60)

if exit_code != 0:
    raise RuntimeError(
        f"Unit tests failed (pytest exit code {exit_code}). See output above."
    )

print("\n  ✓ All unit tests passed.")
