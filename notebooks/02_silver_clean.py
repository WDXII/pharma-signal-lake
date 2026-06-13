# Databricks notebook source
# MAGIC %md
# MAGIC # 02_silver_clean — Silver Layer Cleansing and Integration
# MAGIC **Project:** PharmaSignalLake
# MAGIC **Purpose:** Read Bronze Delta tables, apply data quality rules,
# MAGIC standardize values, deduplicate cases, and produce three
# MAGIC integrated Silver tables:
# MAGIC   - `silver_cases`           — DEMO + OUTC + RPSR
# MAGIC   - `silver_drug_reactions`  — DRUG (PS only) + REAC
# MAGIC   - `silver_therapy`         — THER (cleaned dates)
# MAGIC
# MAGIC **Run after:** `01_bronze_ingest`
# MAGIC **Run before:** `03_gold_signals`

# COMMAND ----------

# Load configuration
# MAGIC %run ./00_config

# COMMAND ----------
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, upper, trim, max as spark_max, when, coalesce,
    current_timestamp, lit, to_date, datediff,
    row_number, cast
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helper: Read Bronze Delta Table

# COMMAND ----------

def read_bronze(file_key: str) -> DataFrame:
    """Read a Bronze Delta table for the current quarter."""
    path = BRONZE[file_key]
    return (
        spark.read
        .format("delta")
        .load(path)
        .filter(col("source_quarter") == QUARTER)
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helper: Write Silver Delta Table

# COMMAND ----------

def write_silver(df: DataFrame, table_key: str) -> int:
    """
    Write a DataFrame to a Silver Delta table.
    Uses OVERWRITE partitioned by source_quarter (idempotent per quarter).
    Returns row count for logging.
    """
    path  = SILVER[table_key]
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
# MAGIC ## Silver Table 1 — silver_cases
# MAGIC ### Source: DEMO + OUTC + RPSR
# MAGIC
# MAGIC **Transformations:**
# MAGIC 1. Deduplicate DEMO — keep latest `caseversion` per `primaryid`
# MAGIC 2. Normalize age to years using `age_cod` multiplier
# MAGIC 3. Flag missing/invalid ages (do NOT drop — preserves case count)
# MAGIC 4. Left join OUTC on `primaryid`
# MAGIC 5. Left join RPSR on `primaryid`

# COMMAND ----------

def build_silver_cases() -> DataFrame:
    """
    Build silver_cases: deduplicated case demographics with outcome
    and reporter source joined in.
    """

    # ── Step 1: Load and deduplicate DEMO ─────────────────────────────────────
    # FAERS allows reporters to update a case by submitting a new caseversion.
    # We keep the record with the highest caseversion per primaryid.

    demo_raw = read_bronze("demo")

    window_latest = Window.partitionBy("primaryid").orderBy(
        col("caseversion").cast(IntegerType()).desc()
    )

    demo_dedup = (
        demo_raw
        .withColumn("rn", row_number().over(window_latest))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # ── Step 2: Normalize age to years ────────────────────────────────────────
    # FAERS stores age as a number + a unit code (YR, MON, WK, DY, HR, DEC).
    # We convert everything to years for consistent demographic analysis.

    demo_aged = (
        demo_dedup
        .withColumn("age_num", col("age").cast(DoubleType()))
        .withColumn(
            "age_years",
            when(col("age_cod") == "YR",  col("age_num") * 1.0)
            .when(col("age_cod") == "MON", col("age_num") / 12.0)
            .when(col("age_cod") == "WK",  col("age_num") / 52.0)
            .when(col("age_cod") == "DY",  col("age_num") / 365.0)
            .when(col("age_cod") == "HR",  col("age_num") / 8760.0)
            .when(col("age_cod") == "DEC", col("age_num") * 10.0)
            .otherwise(None)
        )
        .withColumn(
            "age_group",
            when(col("age_years").isNull(), "UNKNOWN")
            .when(col("age_years") < 2,    "NEONATE_INFANT")
            .when(col("age_years") < 12,   "CHILD")
            .when(col("age_years") < 18,   "ADOLESCENT")
            .when(col("age_years") < 65,   "ADULT")
            .otherwise("ELDERLY")
        )
        .withColumn(
            "age_quality",
            when(col("age").isNull(), "MISSING")
            .when(col("age_cod").isNull(), "MISSING_UNIT")
            .when(col("age_years") < 0, "INVALID_NEGATIVE")
            .when(col("age_years") > 120, "INVALID_OVER_120")
            .otherwise("VALID")
        )
    )

    # ── Step 3: Load OUTC and RPSR ────────────────────────────────────────────
    # RPSR can have multiple rows per primaryid (multiple report sources).
    # We aggregate to a comma-separated list to avoid fan-out on the join.

    outc = (
        read_bronze("outc")
        .select("primaryid", "outc_cod")
        .dropDuplicates(["primaryid", "outc_cod"])
    )

    rpsr = (
        read_bronze("rpsr")
        .select("primaryid", "rpsr_cod")
        .dropDuplicates(["primaryid", "rpsr_cod"])
    )

    # ── Step 4: Join DEMO + OUTC + RPSR ───────────────────────────────────────
    silver_cases = (
        demo_aged
        .join(outc, on="primaryid", how="left")
        .join(rpsr, on="primaryid", how="left")
        .withColumn("silver_load_timestamp", current_timestamp())
        .select(
            "primaryid",
            "caseid",
            "caseversion",
            "i_f_code",
            "event_dt",
            "age",
            "age_cod",
            "age_years",
            "age_group",
            "age_quality",
            "sex",
            "reporter_country",
            "occr_country",
            "occp_cod",
            "wt",
            "wt_cod",
            "outc_cod",
            "rpsr_cod",
            "source_quarter",
            "silver_load_timestamp",
        )
    )

    return silver_cases

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Table 2 — silver_drug_reactions
# MAGIC ### Source: DRUG (PS only) + REAC
# MAGIC
# MAGIC **Transformations:**
# MAGIC 1. Filter DRUG to Primary Suspect drugs only (`role_cod = 'PS'`)
# MAGIC 2. Standardize `drugname` and `prod_ai` — UPPER + TRIM
# MAGIC 3. Standardize `pt` (MedDRA Preferred Term) — UPPER + TRIM
# MAGIC 4. Join DRUG to REAC on `primaryid`

# COMMAND ----------

def build_silver_drug_reactions() -> DataFrame:
    """
    Build silver_drug_reactions: one row per (case, PS drug, reaction).
    This is the source table for PRR/ROR signal detection.
    """

    # ── Step 1: Load and filter DRUG to Primary Suspects ──────────────────────
    # We only want drugs that reporters identified as likely causes.
    # Concomitant (C) and interacting (I) drugs would dilute signal math.

    drug = (
        read_bronze("drug")
        .filter(col("role_cod") == PRIMARY_SUSPECT_CODE)
        .withColumn("drugname",  upper(trim(col("drugname"))))
        .withColumn("prod_ai",   upper(trim(col("prod_ai"))))
        .select(
            "primaryid",
            "drug_seq",
            "role_cod",
            "drugname",
            "prod_ai",
            "route",
            "dose_amt",
            "dose_unit",
            "dose_form",
            "dose_freq",
        )
    )

    # ── Step 2: Load REAC and standardize Preferred Terms ─────────────────────
    reac = (
        read_bronze("reac")
        .withColumn("pt", upper(trim(col("pt"))))
        .select("primaryid", "pt", "drug_rec_act")
        .dropDuplicates(["primaryid", "pt"])   # remove exact duplicate reactions
    )

    # ── Step 3: Join DRUG + REAC ──────────────────────────────────────────────
    # Inner join: we only want cases where both a PS drug AND a reaction exist.
    # Cases with no reaction record are not useful for signal detection.

    silver_dr = (
        drug
        .join(reac, on="primaryid", how="inner")
        .withColumn("silver_load_timestamp", current_timestamp())
        .select(
            "primaryid",
            "drug_seq",
            "drugname",
            "prod_ai",
            "role_cod",
            "route",
            "dose_amt",
            "dose_unit",
            "dose_form",
            "dose_freq",
            "pt",
            "drug_rec_act",
            "source_quarter",
            "silver_load_timestamp",
        )
    )

    return silver_dr

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Table 3 — silver_therapy
# MAGIC ### Source: THER
# MAGIC
# MAGIC **Transformations:**
# MAGIC 1. Parse `start_dt` and `end_dt` from YYYYMMDD string to date type
# MAGIC 2. Compute therapy duration in days where both dates are present
# MAGIC 3. Flag rows where date parsing failed

# COMMAND ----------

def build_silver_therapy() -> DataFrame:
    """
    Build silver_therapy: cleaned therapy dates with duration computed.
    """

    ther = read_bronze("ther")

    silver_ther = (
        ther
        .withColumn(
            "start_date",
            to_date(
                when(col("start_dt").rlike(r"^\d{8}$"), col("start_dt")),
                "yyyyMMdd"
            )
        )
        .withColumn(
            "end_date",
            to_date(
                when(col("end_dt").rlike(r"^\d{8}$"), col("end_dt")),
                "yyyyMMdd"
            )
        )
        .withColumn(
            "duration_days",
            when(
                col("start_date").isNotNull() & col("end_date").isNotNull(),
                datediff(col("end_date"), col("start_date"))
            ).otherwise(None)
        )
        .withColumn(
            "date_quality",
            when(col("start_dt").isNull(), "MISSING_START")
            .when(col("start_date").isNull(), "INVALID_START_FORMAT")
            .when(col("end_date").isNull() & col("end_dt").isNotNull(), "INVALID_END_FORMAT")
            .when(col("end_date").isNull(), "NO_END_DATE")
            .when(col("duration_days") < 0, "END_BEFORE_START")
            .otherwise("VALID")
        )
        .withColumn("silver_load_timestamp", current_timestamp())
        .select(
            "primaryid",
            "caseid",
            "dsg_drug_seq",
            "start_dt",
            "end_dt",
            "start_date",
            "end_date",
            "duration_days",
            "dur",
            "dur_cod",
            "date_quality",
            "source_quarter",
            "silver_load_timestamp",
        )
    )

    return silver_ther

# COMMAND ----------
# MAGIC %md
# MAGIC ## Execute All Three Silver Tables

# COMMAND ----------

print("=" * 60)
print(f"  SILVER TRANSFORMATION — Quarter: {QUARTER}")
print("=" * 60)

silver_log = {}

# ── silver_cases ──────────────────────────────────────────────────────────────
print("\n[SILVER_CASES]  Building from DEMO + OUTC + RPSR...")
try:
    df_cases = build_silver_cases()
    count    = write_silver(df_cases, "cases")
    silver_log["silver_cases"] = {"status": "SUCCESS", "rows": count}
except Exception as e:
    print(f"  ✗ ERROR: {e}")
    silver_log["silver_cases"] = {"status": "FAILED", "rows": 0, "error": str(e)}

# ── silver_drug_reactions ─────────────────────────────────────────────────────
print("\n[SILVER_DRUG_REACTIONS]  Building from DRUG (PS) + REAC...")
try:
    df_dr = build_silver_drug_reactions()
    count = write_silver(df_dr, "drug_reactions")
    silver_log["silver_drug_reactions"] = {"status": "SUCCESS", "rows": count}
except Exception as e:
    print(f"  ✗ ERROR: {e}")
    silver_log["silver_drug_reactions"] = {"status": "FAILED", "rows": 0, "error": str(e)}

# ── silver_therapy ────────────────────────────────────────────────────────────
print("\n[SILVER_THERAPY]  Building from THER...")
try:
    df_ther = build_silver_therapy()
    count   = write_silver(df_ther, "therapy")
    silver_log["silver_therapy"] = {"status": "SUCCESS", "rows": count}
except Exception as e:
    print(f"  ✗ ERROR: {e}")
    silver_log["silver_therapy"] = {"status": "FAILED", "rows": 0, "error": str(e)}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("  SILVER TRANSFORMATION SUMMARY")
print("=" * 60)

failures = []
for table, result in silver_log.items():
    status = result["status"]
    rows   = result["rows"]
    icon   = "✓" if status == "SUCCESS" else "✗"
    print(f"  {icon} {table:<30} {status:<10} {rows:>10,} rows")
    if status == "FAILED":
        failures.append(table)

print("=" * 60)

if failures:
    raise RuntimeError(
        f"Silver transformation failed for: {failures}. "
        "Review error messages above before proceeding to Gold."
    )
else:
    print("\n  ✓ All Silver tables built successfully. Ready for Gold layer.")
