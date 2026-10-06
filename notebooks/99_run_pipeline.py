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
# MAGIC
# MAGIC Each `%run` below is deliberately alone in its own cell — Databricks
# MAGIC requires this; mixing `%run` with other Python statements in the same
# MAGIC cell silently falls back to IPython's unsupported `%run` line magic
# MAGIC instead of Databricks' notebook-chaining `%run` (see DEVELOPER.md
# MAGIC Issue 3 and Issue 12).

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

import time
from datetime import datetime, timezone

# Reset on every execution of this cell so a rerun never reuses a stale log.
stage_log      = []
pipeline_start = datetime.now(timezone.utc)
pipeline_t0    = time.perf_counter()
_open_stage    = None
_last_end_perf = pipeline_t0
print("=" * 60)
print("  PharmaSignalLake — Full Pipeline Run")
print(f"  Started: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
print("=" * 60)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage timing helpers
# MAGIC
# MAGIC Python cannot wrap a `%run` cell in `try/except`, so each stage is timed
# MAGIC with a begin cell before its `%run` and an end cell after it. If the
# MAGIC `%run` raises, Run All stops before the end cell: the stage is logged as
# MAGIC `FAILED` the next time `stage_begin` or the summary cell runs, with that
# MAGIC later time as its end (see DEVELOPER.md Issue 25).

# COMMAND ----------

def stage_begin(name):
    """Open a stage. A still-open previous stage never reached its end cell, so log it FAILED."""
    global _open_stage
    if _open_stage is not None:
        stage_end("FAILED")
    _open_stage = {
        "stage"    : name,
        "start_utc": datetime.now(timezone.utc),
        "t0"       : time.perf_counter(),
    }
    return _open_stage["t0"]


def stage_end(status="OK"):
    """Close the open stage and append one entry to stage_log."""
    global _open_stage, _last_end_perf
    if _open_stage is None:
        return
    t1      = time.perf_counter()
    end_utc = datetime.now(timezone.utc)
    stage_log.append({
        "stage"     : _open_stage["stage"],
        "start_utc" : _open_stage["start_utc"].isoformat(),
        "end_utc"   : end_utc.isoformat(),
        "duration_s": t1 - _open_stage["t0"],
        "status"    : status,
    })
    _last_end_perf = t1
    _open_stage    = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 0 — Configuration

# COMMAND ----------

stage_start = stage_begin("Loading configuration")
print("\n[STAGE 0] Loading configuration...")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

stage_end()
print(f"  ✓ Configuration loaded ({stage_log[-1]['duration_s']:.0f}s)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 1 — Bronze Ingestion

# COMMAND ----------

stage_start = stage_begin("Bronze ingestion")
print("\n[STAGE 1] Bronze ingestion...")

# COMMAND ----------

# MAGIC %run ./01_bronze_ingest

# COMMAND ----------

stage_end()
print(f"  ✓ Bronze complete ({stage_log[-1]['duration_s']:.0f}s)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 2 — Silver Cleansing

# COMMAND ----------

stage_start = stage_begin("Silver cleansing and integration")
print("\n[STAGE 2] Silver cleansing and integration...")

# COMMAND ----------

# MAGIC %run ./02_silver_clean

# COMMAND ----------

stage_end()
print(f"  ✓ Silver complete ({stage_log[-1]['duration_s']:.0f}s)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 3 — Gold Signal Detection

# COMMAND ----------

stage_start = stage_begin("Gold signal detection")
print("\n[STAGE 3] Gold signal detection...")

# COMMAND ----------

# MAGIC %run ./03_gold_signals

# COMMAND ----------

stage_end()
print(f"  ✓ Gold complete ({stage_log[-1]['duration_s']:.0f}s)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 4 — Export

# COMMAND ----------

stage_start = stage_begin("Exporting to CSV")
print("\n[STAGE 4] Exporting to CSV...")

# COMMAND ----------

# MAGIC %run ./04_export

# COMMAND ----------

stage_end()
print(f"  ✓ Export complete ({stage_log[-1]['duration_s']:.0f}s)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Complete

# COMMAND ----------

if _open_stage is not None:
    stage_end("FAILED")
if not stage_log:
    raise RuntimeError("stage_log is empty — run the cells above in order before this summary.")

all_ok           = all(s["status"] == "OK" for s in stage_log)
pipeline_end     = datetime.fromisoformat(stage_log[-1]["end_utc"])
total_seconds    = _last_end_perf - pipeline_t0
total_minutes    = int(total_seconds // 60)
remaining_secs   = total_seconds - total_minutes * 60
duration_sum     = sum(s["duration_s"] for s in stage_log)

print("\n" + "=" * 60)
if all_ok:
    print("  ✓ PharmaSignalLake Pipeline — COMPLETE")
else:
    print("  ✗ PharmaSignalLake Pipeline — INCOMPLETE (a stage FAILED)")
print(f"  Quarter processed : {QUARTER}")
print(f"  Finished          : {pipeline_end.strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"  Total duration    : {total_minutes}m {remaining_secs:.1f}s")
print("=" * 60)

fmt        = "%Y-%m-%d %H:%M:%S"
name_width = max(len("stage"), max(len(s["stage"]) for s in stage_log))
print(f"\n  {'stage':<{name_width}} | {'start_utc':<19} | {'end_utc':<19} | {'duration_s':>10} | status")
print(f"  {'-' * name_width}-+-{'-' * 19}-+-{'-' * 19}-+-{'-' * 10}-+-------")
for s in stage_log:
    start_txt = datetime.fromisoformat(s["start_utc"]).strftime(fmt)
    end_txt   = datetime.fromisoformat(s["end_utc"]).strftime(fmt)
    print(f"  {s['stage']:<{name_width}} | {start_txt:<19} | {end_txt:<19} | {s['duration_s']:>10.1f} | {s['status']}")
print(f"\n  Total wall time   : {total_seconds:.1f}s (sum of stages: {duration_sum:.1f}s)")
print("  Times are UTC")

# Sanity checks — warn only, never fail the run.
timing_warnings = []
for s in stage_log:
    s_start = datetime.fromisoformat(s["start_utc"])
    s_end   = datetime.fromisoformat(s["end_utc"])
    if s_start < pipeline_start:
        timing_warnings.append(f"{s['stage']}: start is earlier than pipeline_start")
    if s_end < s_start:
        timing_warnings.append(f"{s['stage']}: end is earlier than its start")
if duration_sum > total_seconds + 1.0:
    timing_warnings.append(f"stage durations sum to {duration_sum:.1f}s, more than total {total_seconds:.1f}s by over 1s")
for w in timing_warnings:
    print(f"  ⚠ TIMING WARNING: {w}")

if all_ok:
    print("\n  Gold exports are ready for Power BI at:")
    print(f"  {BASE_EXPORT}/")
