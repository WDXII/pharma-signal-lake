# Databricks notebook source
# MAGIC %md
# MAGIC # 01_bronze_ingest — Bronze Layer Ingestion
# MAGIC **Project:** PharmaSignalLake
# MAGIC **Purpose:** Read all 7 raw FAERS source files from DBFS and write them
# MAGIC to Delta tables in the Bronze layer. No transformations are applied —
# MAGIC Bronze preserves the source data exactly as received.
# MAGIC
# MAGIC **Run after:** `00_config`
# MAGIC **Run before:** `02_silver_clean`

# COMMAND ----------

# Load configuration
# MAGIC %run ./00_config

# COMMAND ----------
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def read_faers_file(path: str, source_name: str) -> DataFrame:
    """
    Read a FAERS $-delimited text file from DBFS into a PySpark DataFrame.

    All columns are read as STRING — no type casting at Bronze.
    Two audit columns are added:
      - load_timestamp : the time this row was ingested
      - source_quarter : the quarter identifier from config (e.g. '2026Q1')

    Args:
        path        : DBFS path to the .txt file
        source_name : human-readable name for logging (e.g. 'DEMO')

    Returns:
        DataFrame with all source columns plus audit columns
    """
    print(f"  Reading {source_name} from: {path}")

    df = (
        spark.read
        .option("header", "true")
        .option("sep", FAERS_DELIMITER)
        .option("inferSchema", "false")       # all STRING at Bronze — deliberate
        .option("multiLine", "false")
        .option("encoding", "UTF-8")
        .csv(path)
    )

    # Add audit columns
    df = (
        df
        .withColumn("load_timestamp", current_timestamp())
        .withColumn("source_quarter", lit(QUARTER))
    )

    return df


def validate_row_count(df: DataFrame, source_name: str) -> int:
    """
    Count rows in a DataFrame. Raises an error if zero rows are found.
    Returns the row count for logging.
    """
    count = df.count()
    if count == 0:
        raise ValueError(
            f"BRONZE INGESTION FAILED: {source_name} returned 0 rows. "
            f"Check that the file exists at the configured path and is not empty."
        )
    print(f"  ✓ {source_name}: {count:,} rows")
    return count


def write_bronze(df: DataFrame, delta_path: str, source_name: str) -> None:
    """
    Write a DataFrame to a Delta table at the given path.

    Uses OVERWRITE mode partitioned by source_quarter so re-running for
    the same quarter is safe (idempotent). Running for a new quarter
    appends a new partition without touching existing data.

    Args:
        df          : DataFrame to write
        delta_path  : DBFS Delta path (from BRONZE config dict)
        source_name : human-readable name for logging
    """
    print(f"  Writing {source_name} to Delta: {delta_path}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("source_quarter")
        .save(delta_path)
    )

    print(f"  ✓ {source_name} written successfully")


def ingest_file(file_key: str) -> int:
    """
    End-to-end ingestion for a single FAERS file type.
    Reads, validates, and writes to Bronze Delta.

    Args:
        file_key : key from RAW_FILES / BRONZE dicts (e.g. 'demo')

    Returns:
        Row count of ingested file
    """
    source_name = file_key.upper()
    raw_path    = RAW_FILES[file_key]
    delta_path  = BRONZE[file_key]

    df    = read_faers_file(raw_path, source_name)
    count = validate_row_count(df, source_name)
    write_bronze(df, delta_path, source_name)

    return count

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Bronze Ingestion — All 7 Files

# COMMAND ----------

print("=" * 60)
print(f"  BRONZE INGESTION — Quarter: {QUARTER}")
print("=" * 60)

ingestion_log = {}
errors        = []

for key in RAW_FILES.keys():
    print(f"\n[{key.upper()}]")
    try:
        row_count = ingest_file(key)
        ingestion_log[key] = {"status": "SUCCESS", "rows": row_count}
    except Exception as e:
        print(f"  ✗ ERROR: {e}")
        errors.append({"file": key, "error": str(e)})
        ingestion_log[key] = {"status": "FAILED", "rows": 0}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ingestion Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("  BRONZE INGESTION SUMMARY")
print("=" * 60)

total_rows    = 0
success_count = 0

for key, result in ingestion_log.items():
    status = result["status"]
    rows   = result["rows"]
    icon   = "✓" if status == "SUCCESS" else "✗"
    print(f"  {icon} {key.upper():<8} {status:<10} {rows:>12,} rows")
    if status == "SUCCESS":
        total_rows    += rows
        success_count += 1

print("-" * 60)
print(f"  Files succeeded : {success_count} / {len(RAW_FILES)}")
print(f"  Total rows      : {total_rows:,}")
print("=" * 60)

if errors:
    print(f"\n  ⚠ {len(errors)} file(s) failed. Pipeline should not proceed to Silver.")
    for err in errors:
        print(f"    - {err['file'].upper()}: {err['error']}")
    raise RuntimeError("Bronze ingestion completed with errors. Review logs above.")
else:
    print("\n  ✓ All files ingested successfully. Ready for Silver layer.")
