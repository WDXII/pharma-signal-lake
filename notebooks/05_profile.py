# Databricks notebook source
# MAGIC %md
# MAGIC # 05_profile — Pipeline Profiling (Baseline / After)
# MAGIC **Project:** PharmaSignalLake
# MAGIC **Purpose:** Collect row-count and data-quality metrics from the Bronze, Silver and
# MAGIC Gold tables so a pipeline change can be measured before and after it is applied.
# MAGIC
# MAGIC **Read-only tooling.** This notebook only READS pipeline tables. It writes only to its
# MAGIC own `_`-prefixed tables and is NOT part of `99_run_pipeline`.
# MAGIC
# MAGIC | Widget `run_label` | What it does |
# MAGIC |--------------------|--------------|
# MAGIC | `baseline` | Profile the current tables, snapshot `gold_signals` → `_gold_signals_baseline`, print the Delta version |
# MAGIC | `after`    | Profile the tables again after the fixed pipeline has run |
# MAGIC
# MAGIC Metrics are written to `_profile_results` as
# MAGIC `(run_label, layer, metric, value, source_quarter, profiled_at)`.
# MAGIC Re-running with the same `run_label` replaces that label's rows for the quarter (idempotent).
# MAGIC
# MAGIC This notebook must work on both the unfixed and the fixed tables, so it never reads a
# MAGIC column that the signal fix adds (`chi_sq`, `haldane_applied`).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------
# MAGIC %md
# MAGIC ## Imports and Run Label

# COMMAND ----------

import re

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    col, countDistinct, current_timestamp, expr, lit, row_number, when
)
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

dbutils.widgets.text("run_label", "baseline", "Run label (baseline / after)")
RUN_LABEL = dbutils.widgets.get("run_label").strip()

if not re.fullmatch(r"[A-Za-z0-9_]+", RUN_LABEL):
    raise ValueError(
        f"run_label must contain only letters, digits and underscores; got {RUN_LABEL!r}"
    )

BASELINE_LABEL = "baseline"

# Columns added by the signal fix. Used ONLY to guard against taking a baseline too late.
FIX_ADDED_COLUMNS = ["chi_sq", "haldane_applied"]

print(f"  run_label : {RUN_LABEL}")
print(f"  quarter   : {QUARTER}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def read_quarter(path: str) -> DataFrame:
    """Read a Delta table and filter it to the configured quarter."""
    return (
        spark.read.format("delta").load(path)
        .filter(col("source_quarter") == QUARTER)
    )


def get_delta_version(path: str) -> int:
    """Return the latest Delta version number of the table at `path`."""
    return DeltaTable.forPath(spark, path).history(1).select("version").first()[0]


def assert_gold_signals_unfixed() -> None:
    """
    Stop a baseline run if gold_signals already carries the columns the fix adds.
    A baseline taken after the fix would overwrite the only copy of the old results.
    """
    present = [c for c in FIX_ADDED_COLUMNS if c in spark.read.format("delta").load(GOLD["signals"]).columns]
    if present:
        raise RuntimeError(
            f"gold_signals already has fix columns {present}: this is NOT the unfixed table. "
            "A baseline must be taken before 99_run_pipeline runs with the fixed code. "
            "Restore the old version with time travel (DESCRIBE HISTORY) or use run_label='after'."
        )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Metrics — `bronze_demo` and child tables

# COMMAND ----------

def count_rows(df: DataFrame) -> int:
    """Total number of rows."""
    return df.count()


def count_distinct_values(df: DataFrame, column: str) -> int:
    """Number of distinct non-null values in `column`."""
    return df.select(countDistinct(column)).first()[0]


def count_caseids_with_multiple_versions(demo: DataFrame) -> int:
    """Number of caseids that appear with more than one distinct caseversion."""
    return (
        demo.groupBy("caseid")
        .agg(countDistinct("caseversion").alias("n_versions"))
        .filter(col("n_versions") > 1)
        .count()
    )


def count_superseded_rows(demo: DataFrame) -> int:
    """DEMO rows that are not the single surviving row of their caseid (rows − distinct caseid)."""
    return count_rows(demo) - count_distinct_values(demo, "caseid")


def superseded_primaryids(demo: DataFrame) -> DataFrame:
    """
    primaryids that are NOT the latest version of their caseid.

    The latest version is the row with the highest integer caseversion per caseid
    (primaryid descending as a deterministic tiebreak). try_cast keeps a malformed
    caseversion from raising under ANSI mode; such rows sort last.
    """
    latest_first = Window.partitionBy("caseid").orderBy(
        expr("try_cast(caseversion AS INT)").desc_nulls_last(),
        col("primaryid").desc(),
    )
    survivors = (
        demo.withColumn("rn", row_number().over(latest_first))
        .filter(col("rn") == 1)
        .select("primaryid")
    )
    return demo.select("primaryid").distinct().join(survivors, on="primaryid", how="left_anti")


def count_rows_on_superseded_versions(child: DataFrame, superseded: DataFrame) -> int:
    """Rows of a child table whose primaryid is a superseded case version."""
    return child.join(superseded, on="primaryid", how="left_semi").count()


def profile_bronze() -> list:
    """Collect all Bronze metrics as (layer, metric, value) tuples."""
    demo = read_quarter(BRONZE["demo"])
    superseded = superseded_primaryids(demo)

    metrics = [
        ("bronze", "bronze_demo_rows",                   count_rows(demo)),
        ("bronze", "bronze_demo_distinct_primaryid",     count_distinct_values(demo, "primaryid")),
        ("bronze", "bronze_demo_distinct_caseid",        count_distinct_values(demo, "caseid")),
        ("bronze", "bronze_demo_caseids_multi_version",  count_caseids_with_multiple_versions(demo)),
        ("bronze", "bronze_demo_superseded_rows",        count_superseded_rows(demo)),
    ]
    for child_key in ["drug", "reac", "indi", "outc", "rpsr", "ther"]:
        child = read_quarter(BRONZE[child_key])
        metrics.append((
            "bronze",
            f"bronze_{child_key}_rows_on_superseded_versions",
            count_rows_on_superseded_versions(child, superseded),
        ))
    return metrics

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Metrics

# COMMAND ----------

def count_orphan_rows(child: DataFrame, parent: DataFrame) -> int:
    """Rows of `child` whose primaryid does not exist in `parent`."""
    return child.join(parent.select("primaryid").distinct(), on="primaryid", how="left_anti").count()


def profile_silver() -> list:
    """Collect all Silver metrics as (layer, metric, value) tuples."""
    cases   = read_quarter(SILVER["cases"])
    dr      = read_quarter(SILVER["drug_reactions"])
    therapy = read_quarter(SILVER["therapy"])

    return [
        ("silver", "silver_cases_rows",                       count_rows(cases)),
        ("silver", "silver_cases_distinct_primaryid",         count_distinct_values(cases, "primaryid")),
        ("silver", "silver_cases_distinct_caseid",            count_distinct_values(cases, "caseid")),
        ("silver", "silver_drug_reactions_rows",              count_rows(dr)),
        ("silver", "silver_drug_reactions_orphan_primaryid",  count_orphan_rows(dr, cases)),
        ("silver", "silver_therapy_rows",                     count_rows(therapy)),
        ("silver", "silver_therapy_orphan_primaryid",         count_orphan_rows(therapy, cases)),
    ]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Metrics — `gold_signals`
# MAGIC Raw 2x2 cells from the existing columns (no fix columns needed):
# MAGIC `a = case_count`, `b = drug_total − case_count`, `c = event_total − case_count`,
# MAGIC `d = total_reports − drug_total − event_total + case_count`

# COMMAND ----------

def any_zero_cell() -> Column:
    """Boolean column: true when any raw 2x2 cell (a, b, c, d) is 0."""
    a = col("case_count").cast(LongType())
    b = col("drug_total").cast(LongType()) - a
    c = col("event_total").cast(LongType()) - a
    d = col("total_reports").cast(LongType()) - col("drug_total").cast(LongType()) - col("event_total").cast(LongType()) + a
    return (a == 0) | (b == 0) | (c == 0) | (d == 0)


def count_by_signal_flag(gold: DataFrame) -> list:
    """Pair count per signal_flag value (a null flag is reported as 'NULL')."""
    rows = gold.groupBy("signal_flag").count().collect()
    return [(f"gold_signal_flag_{r['signal_flag'] or 'NULL'}", r["count"]) for r in rows]


def count_null(gold: DataFrame, column: str) -> int:
    """Rows where `column` is null."""
    return gold.filter(col(column).isNull()).count()


def count_null_prr_with_min_cases(gold: DataFrame) -> int:
    """Pairs that meet the Evans case minimum but have no PRR (silently unclassifiable)."""
    return gold.filter((col("case_count") >= EVANS_MIN_CASES) & col("prr").isNull()).count()


def count_exclusive_pairs(gold: DataFrame, meets_min_cases: bool) -> int:
    """
    Pairs where every report of the reaction involves the drug (case_count = event_total),
    split by whether the pair meets the Evans case minimum.
    """
    case_filter = (col("case_count") >= EVANS_MIN_CASES) if meets_min_cases else (col("case_count") < EVANS_MIN_CASES)
    return gold.filter((col("case_count") == col("event_total")) & case_filter).count()


def count_pairs_with_zero_cell(gold: DataFrame) -> int:
    """Pairs where at least one raw 2x2 cell is 0."""
    return gold.filter(any_zero_cell()).count()


def profile_gold() -> list:
    """Collect all Gold metrics as (layer, metric, value) tuples."""
    gold = read_quarter(GOLD["signals"])

    metrics = [("gold", "gold_signals_pairs", count_rows(gold))]
    metrics += [("gold", name, value) for name, value in count_by_signal_flag(gold)]
    metrics += [
        ("gold", "gold_null_prr",                          count_null(gold, "prr")),
        ("gold", "gold_null_ror",                          count_null(gold, "ror")),
        ("gold", "gold_null_prr_case_count_ge_min",        count_null_prr_with_min_cases(gold)),
        ("gold", "gold_exclusive_pairs_case_count_ge_min", count_exclusive_pairs(gold, meets_min_cases=True)),
        ("gold", "gold_exclusive_pairs_case_count_lt_min", count_exclusive_pairs(gold, meets_min_cases=False)),
        ("gold", "gold_pairs_with_zero_cell",              count_pairs_with_zero_cell(gold)),
    ]
    return metrics


def profile_meta() -> list:
    """Run metadata stored alongside the metrics: the Delta version of gold_signals profiled."""
    return [("meta", "gold_signals_delta_version", get_delta_version(GOLD["signals"]))]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write Results and Baseline Snapshot

# COMMAND ----------

PROFILE_SCHEMA = StructType([
    StructField("layer",  StringType(), False),
    StructField("metric", StringType(), False),
    StructField("value",  LongType(),   True),
])


def build_profile_df(metrics: list) -> DataFrame:
    """Turn (layer, metric, value) tuples into the _profile_results row layout."""
    return (
        spark.createDataFrame([(l, m, int(v)) for l, m, v in metrics], PROFILE_SCHEMA)
        .withColumn("run_label", lit(RUN_LABEL))
        .withColumn("source_quarter", lit(QUARTER))
        .withColumn("profiled_at", current_timestamp())
        .select("run_label", "layer", "metric", "value", "source_quarter", "profiled_at")
    )


def write_profile_results(df: DataFrame) -> None:
    """
    Write metrics to _profile_results. Replaces only this run_label + quarter,
    so re-running a label is idempotent and other labels are untouched.
    """
    path = VALIDATION["profile_results"]
    if DeltaTable.isDeltaTable(spark, path):
        (
            df.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"run_label = '{RUN_LABEL}' AND source_quarter = '{QUARTER}'")
            .save(path)
        )
    else:
        df.write.format("delta").mode("overwrite").save(path)
    print(f"  ✓ {df.count()} metrics written to {path} (run_label = {RUN_LABEL})")


def snapshot_gold_signals_baseline() -> int:
    """Copy the current gold_signals to _gold_signals_baseline (overwrite). Returns row count."""
    path = VALIDATION["gold_signals_baseline"]
    snapshot = spark.read.format("delta").load(GOLD["signals"])
    (
        snapshot.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    n_rows = spark.read.format("delta").load(path).count()
    print(f"  ✓ gold_signals snapshot: {n_rows:,} rows → {path}")
    return n_rows

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Profile

# COMMAND ----------

print("=" * 60)
print(f"  PROFILE — run_label: {RUN_LABEL} — Quarter: {QUARTER}")
print("=" * 60)

if RUN_LABEL == BASELINE_LABEL:
    assert_gold_signals_unfixed()

print("\n[BRONZE]  bronze_demo + 6 child tables...")
bronze_metrics = profile_bronze()

print("[SILVER]  silver_cases, silver_drug_reactions, silver_therapy...")
silver_metrics = profile_silver()

print("[GOLD]    gold_signals...")
gold_metrics = profile_gold()

meta_metrics = profile_meta()

all_metrics = bronze_metrics + silver_metrics + gold_metrics + meta_metrics
profile_df  = build_profile_df(all_metrics)
write_profile_results(profile_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Baseline Snapshot (only when run_label = baseline)

# COMMAND ----------

if RUN_LABEL == BASELINE_LABEL:
    print("\n[BASELINE SNAPSHOT]")
    snapshot_gold_signals_baseline()
    print(f"  gold_signals current Delta version: {get_delta_version(GOLD['signals'])}")
    print("  → Record this version. It is the time-travel fallback for the fix.")
else:
    print(f"\n  run_label = {RUN_LABEL}: no baseline snapshot taken.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

print("\n" + "=" * 60)
print(f"  PROFILE RESULTS — {RUN_LABEL} — {QUARTER}")
print("=" * 60)
for layer, metric, value in all_metrics:
    print(f"  {layer:<7} {metric:<48} {value:>12,}")
print("=" * 60)

display(
    spark.read.format("delta").load(VALIDATION["profile_results"])
    .filter((col("run_label") == RUN_LABEL) & (col("source_quarter") == QUARTER))
    .orderBy(
        when(col("layer") == "bronze", 1).when(col("layer") == "silver", 2)
        .when(col("layer") == "gold", 3).otherwise(4),
        "metric",
    )
)
