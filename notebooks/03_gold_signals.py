# Databricks notebook source
# MAGIC %md
# MAGIC # 03_gold_signals — Gold Layer Signal Detection
# MAGIC **Project:** PharmaSignalLake
# MAGIC **Purpose:** Read Silver tables and produce three analytics-ready Gold tables:
# MAGIC   - `gold_signals`      — PRR, ROR, and signal classification per drug-reaction pair
# MAGIC   - `gold_demographics` — Age group, sex, country distribution per case
# MAGIC   - `gold_outcomes`     — Outcome and reporter type distribution
# MAGIC
# MAGIC **Run after:** `02_silver_clean`
# MAGIC **Run before:** `04_export`

# COMMAND ----------

# Load configuration
# MAGIC %run ./00_config

# COMMAND ----------
# MAGIC %md
# MAGIC ## Imports
# MAGIC
# MAGIC ANSI mode is left ON (Databricks Serverless/Photon default). Division safety
# MAGIC comes from the Haldane correction guaranteeing every PRR/ROR denominator is
# MAGIC > 0 (see `compute_signals` in `src/signal_math.py`) — not from disabling
# MAGIC ANSI. If a DIVIDE_BY_ZERO error appears here, it means a guard is wrong;
# MAGIC fix the guard, do not re-disable ANSI (see DEVELOPER.md Issue 7).
# MAGIC
# MAGIC `compute_signals` itself lives in `src/signal_math.py`, not in this
# MAGIC notebook, so it can be unit tested with plain pytest
# MAGIC (`tests/test_signals.py`) without needing `spark`/`dbutils` or this
# MAGIC notebook's `%run ./00_config` globals in scope.

# COMMAND ----------

import os
import sys

# databricks.yml syncs src/ as a sibling of notebooks/ (both under .../files/).
sys.path.append(os.path.join(os.getcwd(), "..", "src"))

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, when, lit,
    current_timestamp, round as spark_round,
    sum as spark_sum, explode, split
)
from signal_math import compute_signals

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helper: Read Silver Delta Table

# COMMAND ----------

def read_silver(table_key: str) -> DataFrame:
    """Read a Silver Delta table filtered to the current quarter."""
    path = SILVER[table_key]
    return (
        spark.read
        .format("delta")
        .load(path)
        .filter(col("source_quarter") == QUARTER)
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helper: Write Gold Delta Table

# COMMAND ----------

def write_gold(df: DataFrame, table_key: str) -> int:
    """
    Write a DataFrame to a Gold Delta table.
    Overwrites data for the current quarter only (idempotent per quarter).
    Returns row count for logging.
    """
    path  = GOLD[table_key]
    count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("source_quarter")
        .save(path)
    )

    print(f"  ✓ {table_key}: {count:,} rows written to {path}")
    return count

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Table 1 — gold_signals
# MAGIC ### PRR, ROR, and Yates-Corrected Chi-Squared Disproportionality
# MAGIC
# MAGIC **2x2 contingency table notation:**
# MAGIC ```
# MAGIC                     Reaction R    All other reactions
# MAGIC   Drug D            a = A         b = B - A
# MAGIC   All other drugs   c = C - A     d = N - B - C + A
# MAGIC
# MAGIC   A = cases with drug D AND reaction R
# MAGIC   B = total cases with drug D (any reaction)
# MAGIC   C = total cases with reaction R (any drug)
# MAGIC   N = total cases in dataset
# MAGIC ```
# MAGIC
# MAGIC **Haldane-Anscombe correction (D2):** if any of a/b/c/d is 0, add 0.5 to
# MAGIC all four cells before computing PRR/ROR. `haldane_applied` records whether
# MAGIC this happened. `chi_sq` is always computed on the RAW, uncorrected cells.
# MAGIC
# MAGIC **PRR** = (a/(a+b)) / (c/(c+d)) — Haldane-adjusted cells
# MAGIC **ROR** = (a·d) / (b·c) — Haldane-adjusted cells
# MAGIC **chi_sq (Yates)** = N·(\|ad − bc\| − N/2)² / ((a+b)(c+d)(a+c)(b+d)) — raw cells; null if any marginal is 0
# MAGIC
# MAGIC **signal_flag (full Evans Criteria, D3):** `SIGNAL` iff
# MAGIC `case_count >= EVANS_MIN_CASES AND prr >= EVANS_MIN_PRR AND chi_sq >= EVANS_MIN_CHI2`,
# MAGIC else `NO_SIGNAL`. `case_count` is always the raw `a` (= A), never the
# MAGIC Haldane-adjusted value.
# MAGIC
# MAGIC The math itself is in `compute_signals()` in `src/signal_math.py` (imported
# MAGIC above) — a pure function with no table I/O, unit tested in
# MAGIC `tests/test_signals.py`. This notebook only supplies the thresholds from
# MAGIC `00_config.py`.

# COMMAND ----------

def build_gold_signals() -> DataFrame:
    """
    Read silver_drug_reactions, compute N, and delegate all PRR/ROR/chi_sq/
    signal_flag math to the pure compute_signals() function (src/signal_math.py).
    """

    dr = read_silver("drug_reactions")

    # N = total unique cases in the dataset. Counted distinct primaryid to
    # avoid inflating N when a case has multiple PS drugs.
    N = dr.select("primaryid").distinct().count()
    print(f"  Total unique cases (N): {N:,}")

    gold_signals = (
        compute_signals(dr, N, EVANS_MIN_CASES, EVANS_MIN_PRR, EVANS_MIN_CHI2)
        .withColumn("gold_load_timestamp", current_timestamp())
        .withColumn("source_quarter", lit(QUARTER))
        .select(
            "drugname",
            "pt",
            "case_count",
            "drug_total",
            "event_total",
            "total_reports",
            "prr",
            "ror",
            "chi_sq",
            "haldane_applied",
            "signal_flag",
            "source_quarter",
            "gold_load_timestamp",
        )
        .orderBy(col("prr").desc_nulls_last())
    )

    return gold_signals

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Table 2 — gold_demographics
# MAGIC ### Age group, sex, and country distribution

# COMMAND ----------

def build_gold_demographics() -> DataFrame:
    """
    Aggregate case demographics for the Power BI Demographics dashboard page.
    Produces counts by age group, sex, and top reporter countries.
    """

    cases = read_silver("cases")

    # Age group distribution
    age_dist = (
        cases
        .groupBy("age_group", "source_quarter")
        .agg(countDistinct("primaryid").alias("case_count"))
        .withColumn("dimension", lit("AGE_GROUP"))
        .withColumnRenamed("age_group", "dimension_value")
    )

    # Sex distribution
    sex_dist = (
        cases
        .withColumn(
            "sex_label",
            when(col("sex") == "M", "MALE")
            .when(col("sex") == "F", "FEMALE")
            .otherwise("UNKNOWN")
        )
        .groupBy("sex_label", "source_quarter")
        .agg(countDistinct("primaryid").alias("case_count"))
        .withColumn("dimension", lit("SEX"))
        .withColumnRenamed("sex_label", "dimension_value")
    )

    # Country distribution (top 20 by case count)
    country_dist = (
        cases
        .filter(col("reporter_country").isNotNull())
        .groupBy("reporter_country", "source_quarter")
        .agg(countDistinct("primaryid").alias("case_count"))
        .orderBy(col("case_count").desc())
        .limit(20)
        .withColumn("dimension", lit("REPORTER_COUNTRY"))
        .withColumnRenamed("reporter_country", "dimension_value")
    )

    # Occupation (reporter type) distribution
    occp_dist = (
        cases
        .filter(col("occp_cod").isNotNull())
        .groupBy("occp_cod", "source_quarter")
        .agg(countDistinct("primaryid").alias("case_count"))
        .withColumn("dimension", lit("OCCUPATION"))
        .withColumnRenamed("occp_cod", "dimension_value")
    )

    # Union all dimensions into one flat table — easier for Power BI slicers
    gold_demo = (
        age_dist
        .union(sex_dist)
        .union(country_dist)
        .union(occp_dist)
        .withColumn("gold_load_timestamp", current_timestamp())
        .select(
            "dimension",
            "dimension_value",
            "case_count",
            "source_quarter",
            "gold_load_timestamp",
        )
    )

    return gold_demo

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Table 3 — gold_outcomes
# MAGIC ### Outcome and reporter source distribution

# COMMAND ----------

def build_gold_outcomes() -> DataFrame:
    """
    Aggregate case outcomes and report sources for the Power BI
    Case Outcomes dashboard page.
    """

    cases = read_silver("cases")

    # Outcome distribution
    # silver_cases.outc_cod is a comma-separated list of codes (one row per
    # case — see 02_silver_clean.py's build_silver_cases). Explode it back
    # into one row per (case, code) before the per-code distribution below,
    # so case_count is unaffected by that aggregation.
    outc_dist = (
        cases
        .filter(col("outc_cod").isNotNull())
        .withColumn("outc_cod", explode(split(col("outc_cod"), ",")))
        .withColumn(
            "outc_label",
            when(col("outc_cod") == "DE", "Death")
            .when(col("outc_cod") == "LT", "Life-Threatening")
            .when(col("outc_cod") == "HO", "Hospitalization")
            .when(col("outc_cod") == "DS", "Disability")
            .when(col("outc_cod") == "CA", "Congenital Anomaly")
            .when(col("outc_cod") == "RI", "Required Intervention")
            .when(col("outc_cod") == "OT", "Other Serious")
            .otherwise(col("outc_cod"))
        )
        .groupBy("outc_cod", "outc_label", "source_quarter")
        .agg(countDistinct("primaryid").alias("case_count"))
        .withColumn("dimension", lit("OUTCOME"))
    )

    # Reporter source distribution
    # Same explode as outc_dist above — rpsr_cod is also a comma-separated
    # list per case in silver_cases.
    rpsr_dist = (
        cases
        .filter(col("rpsr_cod").isNotNull())
        .withColumn("rpsr_cod", explode(split(col("rpsr_cod"), ",")))
        .withColumn(
            "rpsr_label",
            when(col("rpsr_cod") == "HP",  "Healthcare Professional")
            .when(col("rpsr_cod") == "CSM", "Consumer")
            .when(col("rpsr_cod") == "FGN", "Foreign")
            .when(col("rpsr_cod") == "LIT", "Literature")
            .when(col("rpsr_cod") == "UF",  "User Facility")
            .when(col("rpsr_cod") == "DDR", "Drug Distributor")
            .otherwise(col("rpsr_cod"))
        )
        .groupBy("rpsr_cod", "rpsr_label", "source_quarter")
        .agg(countDistinct("primaryid").alias("case_count"))
        .withColumn("dimension", lit("REPORTER_SOURCE"))
    )

    # Combine into one Gold outcomes table
    gold_outc = (
        outc_dist
        .select(
            col("dimension"),
            col("outc_cod").alias("code"),
            col("outc_label").alias("label"),
            col("case_count"),
            col("source_quarter"),
        )
        .union(
            rpsr_dist.select(
                col("dimension"),
                col("rpsr_cod").alias("code"),
                col("rpsr_label").alias("label"),
                col("case_count"),
                col("source_quarter"),
            )
        )
        .withColumn("gold_load_timestamp", current_timestamp())
        .select(
            "dimension",
            "code",
            "label",
            "case_count",
            "source_quarter",
            "gold_load_timestamp",
        )
        .orderBy("dimension", col("case_count").desc())
    )

    return gold_outc

# COMMAND ----------
# MAGIC %md
# MAGIC ## Execute All Three Gold Tables

# COMMAND ----------

print("=" * 60)
print(f"  GOLD LAYER — Signal Detection — Quarter: {QUARTER}")
print("=" * 60)

gold_log = {}

# ── gold_signals ──────────────────────────────────────────────────────────────
print("\n[GOLD_SIGNALS]  Computing PRR and ROR...")
try:
    df_signals = build_gold_signals()
    n_rows     = write_gold(df_signals, "signals")
    gold_log["gold_signals"] = {"status": "SUCCESS", "rows": n_rows}
except Exception as e:
    print(f"  ✗ ERROR: {e}")
    gold_log["gold_signals"] = {"status": "FAILED", "rows": 0, "error": str(e)}

# ── gold_demographics ─────────────────────────────────────────────────────────
print("\n[GOLD_DEMOGRAPHICS]  Aggregating demographics...")
try:
    df_demo = build_gold_demographics()
    n_rows  = write_gold(df_demo, "demographics")
    gold_log["gold_demographics"] = {"status": "SUCCESS", "rows": n_rows}
except Exception as e:
    print(f"  ✗ ERROR: {e}")
    gold_log["gold_demographics"] = {"status": "FAILED", "rows": 0, "error": str(e)}

# ── gold_outcomes ─────────────────────────────────────────────────────────────
print("\n[GOLD_OUTCOMES]  Aggregating outcomes...")
try:
    df_outc = build_gold_outcomes()
    n_rows  = write_gold(df_outc, "outcomes")
    gold_log["gold_outcomes"] = {"status": "SUCCESS", "rows": n_rows}
except Exception as e:
    print(f"  ✗ ERROR: {e}")
    gold_log["gold_outcomes"] = {"status": "FAILED", "rows": 0, "error": str(e)}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Summary + Signal Statistics

# COMMAND ----------

print("\n" + "=" * 60)
print("  GOLD LAYER SUMMARY")
print("=" * 60)

failures = []
for table, result in gold_log.items():
    status = result["status"]
    rows   = result["rows"]
    icon   = "✓" if status == "SUCCESS" else "✗"
    print(f"  {icon} {table:<25} {status:<10} {rows:>10,} rows")
    if status == "FAILED":
        failures.append(table)

print("=" * 60)

if failures:
    raise RuntimeError(f"Gold layer failed for: {failures}. Review errors above.")

# ── Signal detection stats ────────────────────────────────────────────────────
print("\n  Signal Detection Results:")
print("-" * 60)

signals_df = (
    spark.read.format("delta").load(GOLD["signals"])
    .filter(col("source_quarter") == QUARTER)
)

signal_counts = (
    signals_df
    .groupBy("signal_flag")
    .agg(count("*").alias("pair_count"))
    .orderBy("signal_flag")
    .collect()
)

for row in signal_counts:
    flag  = row["signal_flag"]
    pairs = row["pair_count"]
    print(f"  {flag:<20} : {pairs:>8,} drug-reaction pairs")

total_pairs      = signals_df.count()
signals_only     = signals_df.filter(col("signal_flag") == "SIGNAL").count()
haldane_applied  = signals_df.filter(col("haldane_applied")).count()

print(f"\n  Total pairs evaluated : {total_pairs:,}")
print(f"  Signals detected      : {signals_only:,}")
print(f"  Signal rate           : {signals_only/total_pairs*100:.2f}%")
print(f"  Haldane correction applied : {haldane_applied:,} pairs ({haldane_applied/total_pairs*100:.2f}%)")
print("=" * 60)

print("\n  ✓ Gold layer complete. Ready to export for Power BI.")
