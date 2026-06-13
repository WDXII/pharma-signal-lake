# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config — PharmaSignalLake Pipeline Configuration
# MAGIC **Project:** PharmaSignalLake
# MAGIC **Purpose:** Central configuration for all pipeline notebooks.
# MAGIC Run this notebook first in every session via `%run ./00_config`.
# MAGIC No data is read or written here.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Quarter Setting
# MAGIC This is the only value you need to change when adding a new quarter.

# COMMAND ----------

# ── CHANGE THIS VALUE TO PROCESS A DIFFERENT QUARTER ──────────────────────────
QUARTER = "2026Q1"
# ──────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Base Paths

# COMMAND ----------

# Unity Catalog Volume — DBFS root is disabled on Free Edition (Unity Catalog default)
BASE_RAW    = f"/Volumes/workspace/default/pharma_raw/{QUARTER}"
BASE_BRONZE = "/Volumes/workspace/default/pharma_raw/bronze"
BASE_SILVER = "/Volumes/workspace/default/pharma_raw/silver"
BASE_GOLD   = "/Volumes/workspace/default/pharma_raw/gold"
BASE_EXPORT = "/Volumes/workspace/default/pharma_raw/exports"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Source File Names
# MAGIC FAERS files follow the pattern: {FILETYPE}{QUARTER}.txt

# COMMAND ----------

# FDA FAERS filenames use 2-digit years: "2026Q1" → "26Q1"
QUARTER_SHORT = QUARTER[2:]

RAW_FILES = {
    "demo" : f"{BASE_RAW}/DEMO{QUARTER_SHORT}.txt",
    "drug" : f"{BASE_RAW}/DRUG{QUARTER_SHORT}.txt",
    "reac" : f"{BASE_RAW}/REAC{QUARTER_SHORT}.txt",
    "indi" : f"{BASE_RAW}/INDI{QUARTER_SHORT}.txt",
    "outc" : f"{BASE_RAW}/OUTC{QUARTER_SHORT}.txt",
    "rpsr" : f"{BASE_RAW}/RPSR{QUARTER_SHORT}.txt",
    "ther" : f"{BASE_RAW}/THER{QUARTER_SHORT}.txt",
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Delta Table Paths

# COMMAND ----------

BRONZE = {
    "demo" : f"{BASE_BRONZE}/bronze_demo",
    "drug" : f"{BASE_BRONZE}/bronze_drug",
    "reac" : f"{BASE_BRONZE}/bronze_reac",
    "indi" : f"{BASE_BRONZE}/bronze_indi",
    "outc" : f"{BASE_BRONZE}/bronze_outc",
    "rpsr" : f"{BASE_BRONZE}/bronze_rpsr",
    "ther" : f"{BASE_BRONZE}/bronze_ther",
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Delta Table Paths

# COMMAND ----------

SILVER = {
    "cases"          : f"{BASE_SILVER}/silver_cases",
    "drug_reactions" : f"{BASE_SILVER}/silver_drug_reactions",
    "therapy"        : f"{BASE_SILVER}/silver_therapy",
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Delta Table Paths

# COMMAND ----------

GOLD = {
    "signals"      : f"{BASE_GOLD}/gold_signals",
    "demographics" : f"{BASE_GOLD}/gold_demographics",
    "outcomes"     : f"{BASE_GOLD}/gold_outcomes",
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Export Paths (CSV for Power BI)

# COMMAND ----------

EXPORTS = {
    "signals"      : f"{BASE_EXPORT}/gold_signals.csv",
    "demographics" : f"{BASE_EXPORT}/gold_demographics.csv",
    "outcomes"     : f"{BASE_EXPORT}/gold_outcomes.csv",
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Constants

# COMMAND ----------

# FAERS source file delimiter
FAERS_DELIMITER = "$"

# Drug role code for Primary Suspect — only PS drugs are used in signal detection
PRIMARY_SUSPECT_CODE = "PS"

# Evans Criteria thresholds for signal classification
EVANS_MIN_CASES = 3
EVANS_MIN_PRR   = 2.0

# PRR lower bound for weak signal (below EVANS_MIN_PRR, above this = weak signal)
WEAK_SIGNAL_PRR = 1.5

# Age codes and their multiplier to convert to years
AGE_TO_YEARS = {
    "YR"  : 1.0,
    "MON" : 1/12,
    "WK"  : 1/52,
    "DY"  : 1/365,
    "HR"  : 1/8760,
    "DEC" : 10.0,
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Confirmation

# COMMAND ----------

print("=" * 60)
print(f"  PharmaSignalLake — Configuration Loaded")
print("=" * 60)
print(f"  Quarter     : {QUARTER}")
print(f"  Raw path    : {BASE_RAW}")
print(f"  Bronze path : {BASE_BRONZE}")
print(f"  Silver path : {BASE_SILVER}")
print(f"  Gold path   : {BASE_GOLD}")
print(f"  Export path : {BASE_EXPORT}")
print("=" * 60)
print(f"  Evans Criteria: cases >= {EVANS_MIN_CASES}, PRR >= {EVANS_MIN_PRR}")
print("=" * 60)
