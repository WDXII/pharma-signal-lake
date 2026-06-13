# Databricks notebook source
# MAGIC %md
# MAGIC # 04_export — Export Gold Layer for Power BI
# MAGIC **Project:** PharmaSignalLake
# MAGIC **Purpose:** Export the three Gold Delta tables to single CSV files
# MAGIC in a Unity Catalog Volume so they can be downloaded and loaded into Power BI Desktop.
# MAGIC
# MAGIC Spark writes CSVs as partitioned part-files by default.
# MAGIC This notebook merges those part-files into a single CSV per table
# MAGIC using `coalesce(1)` — appropriate at Gold scale (aggregated, small).
# MAGIC
# MAGIC **Run after:** `03_gold_signals`
# MAGIC **Output:** `/Volumes/workspace/default/pharma_raw/exports/`

# COMMAND ----------

# Load configuration
# MAGIC %run ./00_config

# COMMAND ----------
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helper: Export Gold Table to CSV

# COMMAND ----------

def export_to_csv(table_key: str, export_path: str) -> int:
    """
    Read a Gold Delta table for the current quarter and write it as a
    single CSV file to the export path.

    Uses coalesce(1) to produce one part file, then renames it to a
    clean filename using dbutils.

    Args:
        table_key   : key from GOLD config dict (e.g. 'signals')
        export_path : target CSV path (e.g. EXPORTS['signals'])

    Returns:
        Row count exported
    """

    gold_path = GOLD[table_key]
    temp_path = f"{BASE_EXPORT}/_tmp_{table_key}"

    # Read the Gold Delta table for this quarter
    df = (
        spark.read
        .format("delta")
        .load(gold_path)
        .filter(col("source_quarter") == QUARTER)
        .drop("gold_load_timestamp")   # internal audit column — not useful in CSV
    )

    row_count = df.count()
    print(f"  {table_key}: {row_count:,} rows to export")

    # Write to a temp folder as a single CSV part file
    (
        df
        .coalesce(1)                    # merge into one file — OK for Gold (small)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("sep", ",")
        .csv(temp_path)
    )

    # Find the part file Spark created (always starts with 'part-')
    files       = dbutils.fs.ls(temp_path)
    part_files  = [f.path for f in files if f.name.startswith("part-")]

    if not part_files:
        raise FileNotFoundError(
            f"No part file found in {temp_path} after write. "
            "The DataFrame may have been empty."
        )

    part_file = part_files[0]

    # Remove existing output file if present, then rename part file
    try:
        dbutils.fs.rm(export_path)
    except Exception:
        pass  # file didn't exist yet — that's fine

    dbutils.fs.mv(part_file, export_path)
    dbutils.fs.rm(temp_path, recurse=True)

    print(f"  ✓ Exported to: {export_path}")
    return row_count

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Exports

# COMMAND ----------

print("=" * 60)
print(f"  EXPORT TO CSV — Quarter: {QUARTER}")
print("=" * 60)

export_log = {}

for key, path in EXPORTS.items():
    print(f"\n[{key.upper()}]")
    try:
        rows = export_to_csv(key, path)
        export_log[key] = {"status": "SUCCESS", "rows": rows, "path": path}
    except Exception as e:
        print(f"  ✗ ERROR: {e}")
        export_log[key] = {"status": "FAILED", "rows": 0, "path": path, "error": str(e)}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Export Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("  EXPORT SUMMARY")
print("=" * 60)

for key, result in export_log.items():
    icon   = "✓" if result["status"] == "SUCCESS" else "✗"
    status = result["status"]
    rows   = result["rows"]
    path   = result["path"]
    print(f"  {icon} {key:<20} {status:<10} {rows:>8,} rows  →  {path}")

print("=" * 60)

failures = [k for k, v in export_log.items() if v["status"] == "FAILED"]
if failures:
    raise RuntimeError(f"Export failed for: {failures}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## How to Download to Your Local Machine
# MAGIC
# MAGIC 1. In the Databricks left sidebar, click **Catalog**
# MAGIC 2. Navigate to: **workspace → default → pharma_raw → Files tab → exports/**
# MAGIC 3. Click each CSV file → **Download**
# MAGIC 4. Save to a local folder (e.g. `C:\PharmaSignalLake\exports\`)
# MAGIC 5. Open Power BI Desktop → **Get Data** → **Text/CSV** → load each file

# COMMAND ----------

print("\n  ✓ All Gold tables exported successfully.")
print(f"  Files are at: {BASE_EXPORT}/")
print("\n  Next steps:")
print("  1. Download CSVs: Catalog → workspace → default → pharma_raw → Files → exports/")
print("  2. Open Power BI Desktop")
print("  3. Get Data → Text/CSV → load gold_signals.csv, gold_demographics.csv, gold_outcomes.csv")
print("  4. Build the three dashboard pages as specified in ARCHITECTURE.md")
