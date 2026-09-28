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
# MAGIC > 0 (see `compute_signals` below) — not from disabling ANSI. If a
# MAGIC DIVIDE_BY_ZERO error appears here, it means a guard is wrong; fix the guard,
# MAGIC do not re-disable ANSI (see DEVELOPER.md Issue 7).

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, when, lit,
    current_timestamp, round as spark_round,
    sum as spark_sum, abs as spark_abs
)
from pyspark.sql.types import DoubleType

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

# COMMAND ----------

def compute_signals(dr: DataFrame, n_total: int) -> DataFrame:
    """
    Pure function: compute PRR, ROR, Yates chi_sq and signal_flag for every
    (drugname, pt) pair in `dr`. Reads and writes no tables — takes the
    drug-reaction DataFrame and the total case count N as plain arguments so
    it can be unit tested with synthetic data (tests/test_signals.py).

    Cells: a = A, b = B - A, c = C - A, d = N - B - C + A.
    Haldane-Anscombe correction (+0.5 to all four cells) is applied to
    a/b/c/d for PRR/ROR only, whenever any cell is 0 — this guarantees every
    PRR/ROR denominator is > 0, so no when() guard is needed for them under
    ANSI mode (D5). chi_sq always uses the RAW, uncorrected cells and is
    null when any of its four marginals is 0.
    """

    N = float(n_total)

    # ── Step 1: Compute A — cases per (drug, reaction) pair ───────────────────
    pair_counts = (
        dr
        .select("primaryid", "drugname", "pt")
        .distinct()                             # one row per case-drug-reaction
        .groupBy("drugname", "pt")
        .agg(countDistinct("primaryid").alias("A"))
    )

    # ── Step 2: Compute B — total cases per drug (across all reactions) ────────
    drug_totals = (
        dr
        .select("primaryid", "drugname")
        .distinct()
        .groupBy("drugname")
        .agg(countDistinct("primaryid").alias("B"))
    )

    # ── Step 3: Compute C — total cases per reaction (across all drugs) ────────
    reaction_totals = (
        dr
        .select("primaryid", "pt")
        .distinct()
        .groupBy("pt")
        .agg(countDistinct("primaryid").alias("C"))
    )

    # ── Step 4: Assemble the 2x2 cells ──────────────────────────────────────────
    contingency = (
        pair_counts
        .join(drug_totals,     on="drugname", how="left")
        .join(reaction_totals, on="pt",       how="left")
        .withColumn("A", col("A").cast(DoubleType()))
        .withColumn("B", col("B").cast(DoubleType()))
        .withColumn("C", col("C").cast(DoubleType()))
        .withColumn("N", lit(N))
        .withColumn("a", col("A"))
        .withColumn("b", col("B") - col("A"))
        .withColumn("c", col("C") - col("A"))
        .withColumn("d", col("N") - col("B") - col("C") + col("A"))
    )

    # ── Step 5: Haldane-Anscombe correction, PRR/ROR cells only ────────────────
    contingency = contingency.withColumn(
        "haldane_applied",
        (col("a") == 0) | (col("b") == 0) | (col("c") == 0) | (col("d") == 0)
    )

    contingency = (
        contingency
        .withColumn("a_h", when(col("haldane_applied"), col("a") + 0.5).otherwise(col("a")))
        .withColumn("b_h", when(col("haldane_applied"), col("b") + 0.5).otherwise(col("b")))
        .withColumn("c_h", when(col("haldane_applied"), col("c") + 0.5).otherwise(col("c")))
        .withColumn("d_h", when(col("haldane_applied"), col("d") + 0.5).otherwise(col("d")))
    )

    # ── Step 6: PRR and ROR on Haldane-adjusted cells ──────────────────────────
    # Haldane guarantees a_h, b_h, c_h, d_h are all > 0, so (a_h+b_h) > 0,
    # (c_h+d_h) > 0, and (b_h*c_h) > 0 always — no null guard needed.
    contingency = (
        contingency
        .withColumn(
            "prr",
            spark_round(
                (col("a_h") / (col("a_h") + col("b_h"))) /
                (col("c_h") / (col("c_h") + col("d_h"))),
                4
            )
        )
        .withColumn(
            "ror",
            spark_round(
                (col("a_h") * col("d_h")) / (col("b_h") * col("c_h")),
                4
            )
        )
    )

    # ── Step 7: chi_sq (Yates), on RAW cells, null if any marginal is 0 ───────
    marginal_ab = col("a") + col("b")   # = B
    marginal_cd = col("c") + col("d")   # = N - B
    marginal_ac = col("a") + col("c")   # = C
    marginal_bd = col("b") + col("d")   # = N - C

    contingency = contingency.withColumn(
        "chi_sq",
        when(
            (marginal_ab > 0) & (marginal_cd > 0) & (marginal_ac > 0) & (marginal_bd > 0),
            spark_round(
                (col("N") *
                    (spark_abs(col("a") * col("d") - col("b") * col("c")) - col("N") / 2.0) ** 2
                ) / (marginal_ab * marginal_cd * marginal_ac * marginal_bd),
                4
            )
        ).otherwise(None)
    )

    # ── Step 8: Classify signals using full Evans Criteria (D3) ───────────────
    # SIGNAL    : case_count (raw a) >= EVANS_MIN_CASES AND prr >= EVANS_MIN_PRR
    #             AND chi_sq >= EVANS_MIN_CHI2
    # NO_SIGNAL : everything else (including chi_sq is null)

    signals = (
        contingency
        .withColumn(
            "signal_flag",
            when(
                (col("a") >= EVANS_MIN_CASES) &
                (col("prr") >= EVANS_MIN_PRR) &
                (col("chi_sq") >= EVANS_MIN_CHI2),
                "SIGNAL"
            )
            .otherwise("NO_SIGNAL")
        )
        .select(
            "drugname",
            "pt",
            col("a").cast("int").alias("case_count"),
            col("B").cast("int").alias("drug_total"),
            col("C").cast("int").alias("event_total"),
            col("N").cast("int").alias("total_reports"),
            "prr",
            "ror",
            "chi_sq",
            "haldane_applied",
            "signal_flag",
        )
    )

    return signals


def build_gold_signals() -> DataFrame:
    """
    Read silver_drug_reactions, compute N, and delegate all PRR/ROR/chi_sq/
    signal_flag math to the pure compute_signals() function.
    """

    dr = read_silver("drug_reactions")

    # N = total unique cases in the dataset. Counted distinct primaryid to
    # avoid inflating N when a case has multiple PS drugs.
    N = dr.select("primaryid").distinct().count()
    print(f"  Total unique cases (N): {N:,}")

    gold_signals = (
        compute_signals(dr, N)
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
    outc_dist = (
        cases
        .filter(col("outc_cod").isNotNull())
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
    rpsr_dist = (
        cases
        .filter(col("rpsr_cod").isNotNull())
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
