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
    row_number, cast, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

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
# MAGIC ## Case Deduplication — DEMO
# MAGIC FAERS reporters can update a case by submitting a new `caseversion`.
# MAGIC `primaryid` already encodes `caseid` + `caseversion`, so partitioning by
# MAGIC `primaryid` is a no-op — every `primaryid` is already unique. The entity
# MAGIC key is `caseid`: we keep exactly one row per `caseid`, the highest
# MAGIC `caseversion`, and every downstream child table is filtered (left_semi
# MAGIC join) to the surviving `primaryid`s so superseded-version rows never
# MAGIC reach Silver.

# COMMAND ----------

def dedup_demo() -> DataFrame:
    """
    Deduplicate DEMO to one row per caseid, keeping the highest caseversion.
    caseversion is parsed with try_cast so a malformed value returns NULL
    (sorted last) under ANSI mode instead of raising.
    """
    demo_raw  = read_bronze("demo")
    raw_count = demo_raw.count()

    caseversion_int = expr("try_cast(caseversion AS INT)")
    window_latest = Window.partitionBy("caseid").orderBy(
        caseversion_int.desc_nulls_last()
    )

    demo_dedup = (
        demo_raw
        .withColumn("rn", row_number().over(window_latest))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    dedup_count = demo_dedup.count()

    print(
        f"  DEMO dedup (by caseid, keep max caseversion): "
        f"{raw_count:,} -> {dedup_count:,} rows "
        f"({raw_count - dedup_count:,} superseded versions removed)"
    )

    return demo_dedup

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Table 1 — silver_cases
# MAGIC ### Source: DEMO (deduplicated) + OUTC + RPSR
# MAGIC
# MAGIC **Transformations:**
# MAGIC 1. Normalize age to years using `age_cod` multiplier
# MAGIC 2. Flag missing/invalid ages (do NOT drop — preserves case count)
# MAGIC 3. Left-semi-join OUTC and RPSR to the surviving primaryids, then left join on `primaryid`

# COMMAND ----------

def build_silver_cases(demo_dedup: DataFrame) -> DataFrame:
    """
    Build silver_cases from an already-deduplicated DEMO DataFrame,
    joined with the OUTC and RPSR rows belonging to surviving cases.
    """

    survivors = demo_dedup.select("primaryid")

    # ── Step 1: Normalize age to years ────────────────────────────────────────
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

    # ── Step 2: Load OUTC and RPSR, filtered to surviving cases ───────────────
    # RPSR can have multiple rows per primaryid (multiple report sources).
    # We aggregate to a comma-separated list to avoid fan-out on the join.

    outc_raw   = read_bronze("outc")
    outc_count = outc_raw.count()
    outc = (
        outc_raw
        .join(survivors, on="primaryid", how="left_semi")
        .select("primaryid", "outc_cod")
        .dropDuplicates(["primaryid", "outc_cod"])
    )
    print(f"  OUTC: {outc_count:,} rows -> {outc.count():,} rows kept for surviving cases")

    rpsr_raw   = read_bronze("rpsr")
    rpsr_count = rpsr_raw.count()
    rpsr = (
        rpsr_raw
        .join(survivors, on="primaryid", how="left_semi")
        .select("primaryid", "rpsr_cod")
        .dropDuplicates(["primaryid", "rpsr_cod"])
    )
    print(f"  RPSR: {rpsr_count:,} rows -> {rpsr.count():,} rows kept for surviving cases")

    # ── Step 3: Join DEMO + OUTC + RPSR ───────────────────────────────────────
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
# MAGIC ### Source: DRUG (PS only, surviving cases) + REAC (surviving cases)
# MAGIC
# MAGIC **Transformations:**
# MAGIC 1. Left-semi-join DRUG and REAC to the surviving primaryids
# MAGIC 2. Filter DRUG to Primary Suspect drugs only (`role_cod = 'PS'`)
# MAGIC 3. Standardize `drugname` and `prod_ai` — UPPER + TRIM
# MAGIC 4. Standardize `pt` (MedDRA Preferred Term) — UPPER + TRIM
# MAGIC 5. Join DRUG to REAC on `primaryid`

# COMMAND ----------

def build_silver_drug_reactions(survivors: DataFrame) -> DataFrame:
    """
    Build silver_drug_reactions: one row per (case, PS drug, reaction),
    restricted to the surviving (highest-caseversion) primaryids.
    This is the source table for PRR/ROR signal detection.
    """

    # ── Step 1: Load DRUG, filter to surviving cases and Primary Suspects ─────
    # We only want drugs that reporters identified as likely causes.
    # Concomitant (C) and interacting (I) drugs would dilute signal math.

    drug_raw   = read_bronze("drug")
    drug_count = drug_raw.count()

    drug = (
        drug_raw
        .join(survivors, on="primaryid", how="left_semi")
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
            "source_quarter",
        )
    )
    print(f"  DRUG: {drug_count:,} rows -> {drug.count():,} rows kept for surviving cases (PS only)")

    # ── Step 2: Load REAC, filter to surviving cases, standardize PTs ─────────
    reac_raw   = read_bronze("reac")
    reac_count = reac_raw.count()

    reac = (
        reac_raw
        .join(survivors, on="primaryid", how="left_semi")
        .withColumn("pt", upper(trim(col("pt"))))
        .select("primaryid", "pt", "drug_rec_act")
        .dropDuplicates(["primaryid", "pt"])   # remove exact duplicate reactions
    )
    print(f"  REAC: {reac_count:,} rows -> {reac.count():,} rows kept for surviving cases")

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
# MAGIC ### Source: THER (surviving cases)
# MAGIC
# MAGIC **Transformations:**
# MAGIC 1. Left-semi-join THER to the surviving primaryids
# MAGIC 2. Parse `start_dt` and `end_dt` from YYYYMMDD string to date type
# MAGIC 3. Compute therapy duration in days where both dates are present
# MAGIC 4. Flag rows where date parsing failed

# COMMAND ----------

def build_silver_therapy(survivors: DataFrame) -> DataFrame:
    """
    Build silver_therapy: cleaned therapy dates with duration computed,
    restricted to the surviving (highest-caseversion) primaryids.
    """

    ther_raw   = read_bronze("ther")
    ther_count = ther_raw.count()

    ther = ther_raw.join(survivors, on="primaryid", how="left_semi")
    print(f"  THER: {ther_count:,} rows -> {ther.count():,} rows kept for surviving cases")

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

# ── case deduplication (shared by all three tables below) ─────────────────────
print("\n[DEDUP]  Deduplicating DEMO on caseid...")
demo_dedup = dedup_demo()
survivors  = demo_dedup.select("primaryid")

# ── silver_cases ──────────────────────────────────────────────────────────────
print("\n[SILVER_CASES]  Building from DEMO (deduped) + OUTC + RPSR...")
try:
    df_cases = build_silver_cases(demo_dedup)
    count    = write_silver(df_cases, "cases")
    silver_log["silver_cases"] = {"status": "SUCCESS", "rows": count}
except Exception as e:
    print(f"  ✗ ERROR: {e}")
    silver_log["silver_cases"] = {"status": "FAILED", "rows": 0, "error": str(e)}

# ── silver_drug_reactions ─────────────────────────────────────────────────────
print("\n[SILVER_DRUG_REACTIONS]  Building from DRUG (PS, surviving) + REAC (surviving)...")
try:
    df_dr = build_silver_drug_reactions(survivors)
    count = write_silver(df_dr, "drug_reactions")
    silver_log["silver_drug_reactions"] = {"status": "SUCCESS", "rows": count}
except Exception as e:
    print(f"  ✗ ERROR: {e}")
    silver_log["silver_drug_reactions"] = {"status": "FAILED", "rows": 0, "error": str(e)}

# ── silver_therapy ────────────────────────────────────────────────────────────
print("\n[SILVER_THERAPY]  Building from THER (surviving)...")
try:
    df_ther = build_silver_therapy(survivors)
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
