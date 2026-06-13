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

# Photon enforces ANSI SQL mode and throws on divide-by-zero even inside when()
# guards. Our PRR/ROR when() conditions are logically correct — this setting makes
# Spark return NULL instead of raising, which is the intended behavior.
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, when, lit,
    current_timestamp, round as spark_round,
    sum as spark_sum
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
# MAGIC ### PRR and ROR Disproportionality Signal Detection
# MAGIC
# MAGIC **Contingency table notation:**
# MAGIC ```
# MAGIC                     Reaction R    All other reactions
# MAGIC   Drug D            A             B - A
# MAGIC   All other drugs   C - A         N - B - C + A
# MAGIC
# MAGIC   A = cases with drug D AND reaction R
# MAGIC   B = total cases with drug D (any reaction)
# MAGIC   C = total cases with reaction R (any drug)
# MAGIC   N = total cases in dataset
# MAGIC ```
# MAGIC
# MAGIC **PRR** = (A/B) / ((C-A)/(N-B))
# MAGIC **ROR** = (A/(B-A)) / ((C-A)/(N-B-C+A))

# COMMAND ----------

def build_gold_signals() -> DataFrame:
    """
    Compute PRR and ROR for all drug-reaction pairs and classify signals
    using Evans Criteria.

    PRR signal threshold  : cases >= EVANS_MIN_CASES AND prr >= EVANS_MIN_PRR
    Weak signal threshold : cases >= EVANS_MIN_CASES AND prr >= WEAK_SIGNAL_PRR
    """

    dr = read_silver("drug_reactions")

    # ── Step 1: Compute N — total unique cases in the dataset ─────────────────
    # N is a scalar value we'll join back in as a literal column.
    # We count distinct primaryid to avoid inflating N when a case has
    # multiple PS drugs.

    N = dr.select("primaryid").distinct().count()
    print(f"  Total unique cases (N): {N:,}")

    # ── Step 2: Compute A — cases per (drug, reaction) pair ───────────────────
    pair_counts = (
        dr
        .select("primaryid", "drugname", "pt")
        .distinct()                             # one row per case-drug-reaction
        .groupBy("drugname", "pt")
        .agg(countDistinct("primaryid").alias("A"))
    )

    # ── Step 3: Compute B — total cases per drug (across all reactions) ────────
    drug_totals = (
        dr
        .select("primaryid", "drugname")
        .distinct()
        .groupBy("drugname")
        .agg(countDistinct("primaryid").alias("B"))
    )

    # ── Step 4: Compute C — total cases per reaction (across all drugs) ────────
    reaction_totals = (
        dr
        .select("primaryid", "pt")
        .distinct()
        .groupBy("pt")
        .agg(countDistinct("primaryid").alias("C"))
    )

    # ── Step 5: Assemble the contingency table ─────────────────────────────────
    contingency = (
        pair_counts
        .join(drug_totals,     on="drugname", how="left")
        .join(reaction_totals, on="pt",       how="left")
        .withColumn("N", lit(N).cast(DoubleType()))
        .withColumn("A", col("A").cast(DoubleType()))
        .withColumn("B", col("B").cast(DoubleType()))
        .withColumn("C", col("C").cast(DoubleType()))
    )

    # ── Step 6: Compute PRR ────────────────────────────────────────────────────
    # PRR = (A / B) / ((C - A) / (N - B))
    # Guard against division by zero: if denominator == 0, return null

    contingency = contingency.withColumn(
        "prr",
        when(
            (col("B") > 0) & ((col("N") - col("B")) > 0) & ((col("C") - col("A")) > 0),
            spark_round(
                (col("A") / col("B")) / ((col("C") - col("A")) / (col("N") - col("B"))),
                4
            )
        ).otherwise(None)
    )

    # ── Step 7: Compute ROR ────────────────────────────────────────────────────
    # ROR = (A / (B - A)) / ((C - A) / (N - B - C + A))
    # Guard: B > A (drug has other reactions), denominator != 0

    contingency = contingency.withColumn(
        "ror",
        when(
            (col("B") - col("A") > 0) &
            (col("N") - col("B") - col("C") + col("A") > 0) &
            (col("C") - col("A") >= 0),
            spark_round(
                (col("A") / (col("B") - col("A"))) /
                ((col("C") - col("A")) / (col("N") - col("B") - col("C") + col("A"))),
                4
            )
        ).otherwise(None)
    )

    # ── Step 8: Classify signals using Evans Criteria ─────────────────────────
    # SIGNAL      : cases >= EVANS_MIN_CASES AND prr >= EVANS_MIN_PRR
    # WEAK_SIGNAL : cases >= EVANS_MIN_CASES AND prr >= WEAK_SIGNAL_PRR
    # NO_SIGNAL   : everything else

    gold_signals = (
        contingency
        .withColumn(
            "signal_flag",
            when(
                (col("A") >= EVANS_MIN_CASES) & (col("prr") >= EVANS_MIN_PRR),
                "SIGNAL"
            )
            .when(
                (col("A") >= EVANS_MIN_CASES) & (col("prr") >= WEAK_SIGNAL_PRR),
                "WEAK_SIGNAL"
            )
            .otherwise("NO_SIGNAL")
        )
        .withColumn("gold_load_timestamp", current_timestamp())
        .withColumn("source_quarter", lit(QUARTER))
        .select(
            "drugname",
            "pt",
            col("A").cast("int").alias("case_count"),
            col("B").cast("int").alias("drug_total"),
            col("C").cast("int").alias("event_total"),
            col("N").cast("int").alias("total_reports"),
            "prr",
            "ror",
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
    count      = write_gold(df_signals, "signals")
    gold_log["gold_signals"] = {"status": "SUCCESS", "rows": count}
except Exception as e:
    print(f"  ✗ ERROR: {e}")
    gold_log["gold_signals"] = {"status": "FAILED", "rows": 0, "error": str(e)}

# ── gold_demographics ─────────────────────────────────────────────────────────
print("\n[GOLD_DEMOGRAPHICS]  Aggregating demographics...")
try:
    df_demo = build_gold_demographics()
    count   = write_gold(df_demo, "demographics")
    gold_log["gold_demographics"] = {"status": "SUCCESS", "rows": count}
except Exception as e:
    print(f"  ✗ ERROR: {e}")
    gold_log["gold_demographics"] = {"status": "FAILED", "rows": 0, "error": str(e)}

# ── gold_outcomes ─────────────────────────────────────────────────────────────
print("\n[GOLD_OUTCOMES]  Aggregating outcomes...")
try:
    df_outc = build_gold_outcomes()
    count   = write_gold(df_outc, "outcomes")
    gold_log["gold_outcomes"] = {"status": "SUCCESS", "rows": count}
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

total_pairs = signals_df.count()
signals_only = signals_df.filter(col("signal_flag") == "SIGNAL").count()

print(f"\n  Total pairs evaluated : {total_pairs:,}")
print(f"  Signals detected      : {signals_only:,}")
print(f"  Signal rate           : {signals_only/total_pairs*100:.2f}%")
print("=" * 60)

print("\n  ✓ Gold layer complete. Ready to export for Power BI.")
