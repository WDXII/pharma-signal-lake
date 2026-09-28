# Databricks notebook source
# MAGIC %md
# MAGIC # 99_run_pipeline — PharmaSignalLake Master Orchestration
# MAGIC **Project:** PharmaSignalLake
# MAGIC **Purpose:** Run the entire pipeline end-to-end in a single click.
# MAGIC Executes notebooks 00 through 04 in sequence using `%run`.
# MAGIC
# MAGIC Each notebook is independently idempotent — if the pipeline
# MAGIC fails mid-run, you can re-run from the failed step without
# MAGIC re-running earlier steps.
# MAGIC
# MAGIC **Usage:** Open this notebook and click "Run All".

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Stages
# MAGIC
# MAGIC | Stage | Notebook | Description |
# MAGIC |-------|----------|-------------|
# MAGIC | 0 | `00_config` | Load all configuration variables |
# MAGIC | 1 | `01_bronze_ingest` | Ingest 7 FAERS files → Bronze Delta |
# MAGIC | 2 | `02_silver_clean` | Cleanse and integrate → Silver Delta |
# MAGIC | 3 | `03_gold_signals` | Compute PRR/ROR → Gold Delta |
# MAGIC | 4 | `04_export` | Export Gold → CSV for Power BI |

# COMMAND ----------

from datetime import datetime

pipeline_start = datetime.now()
print("=" * 60)
print("  PharmaSignalLake — Full Pipeline Run")
print(f"  Started: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 0 — Configuration

# COMMAND ----------

stage_start = datetime.now()
print("\n[STAGE 0] Loading configuration...")

# MAGIC %run ./00_config

print(f"  ✓ Configuration loaded ({(datetime.now()-stage_start).seconds}s)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 1 — Bronze Ingestion

# COMMAND ----------

stage_start = datetime.now()
print("\n[STAGE 1] Bronze ingestion...")

# MAGIC %run ./01_bronze_ingest

print(f"  ✓ Bronze complete ({(datetime.now()-stage_start).seconds}s)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 2 — Silver Cleansing

# COMMAND ----------

stage_start = datetime.now()
print("\n[STAGE 2] Silver cleansing and integration...")

# MAGIC %run ./02_silver_clean

print(f"  ✓ Silver complete ({(datetime.now()-stage_start).seconds}s)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 3 — Gold Signal Detection

# COMMAND ----------

stage_start = datetime.now()
print("\n[STAGE 3] Gold signal detection...")

# MAGIC %run ./03_gold_signals

print(f"  ✓ Gold complete ({(datetime.now()-stage_start).seconds}s)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 4 — Export

# COMMAND ----------

stage_start = datetime.now()
print("\n[STAGE 4] Exporting to CSV...")

# MAGIC %run ./04_export

print(f"  ✓ Export complete ({(datetime.now()-stage_start).seconds}s)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Complete

# COMMAND ----------

pipeline_end     = datetime.now()
total_seconds    = (pipeline_end - pipeline_start).seconds
total_minutes    = total_seconds // 60
remaining_secs   = total_seconds % 60

print("\n" + "=" * 60)
print("  ✓ PharmaSignalLake Pipeline — COMPLETE")
print(f"  Quarter processed : {QUARTER}")
print(f"  Finished          : {pipeline_end.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Total duration    : {total_minutes}m {remaining_secs}s")
print("=" * 60)
print("\n  Gold exports are ready for Power BI at:")
print(f"  {BASE_EXPORT}/")
