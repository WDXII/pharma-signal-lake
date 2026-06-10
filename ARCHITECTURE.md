# ARCHITECTURE.md

**Project:** PharmaSignalLake  
**Version:** 1.0  
**Date:** May 2026  
**Stack:** Databricks Free Edition · PySpark · Delta Lake · Power BI Desktop

---

## Table of Contents

1. [What This System Does](#1-what-this-system-does)
2. [If You Know SQL Server and SSIS — Read This First](#2-if-you-know-sql-server-and-ssis--read-this-first)
3. [Technology Stack](#3-technology-stack)
4. [High-Level Data Flow](#4-high-level-data-flow)
5. [Project Folder Structure](#5-project-folder-structure)
6. [Medallion Architecture](#6-medallion-architecture)
7. [Bronze Layer — Raw Ingestion](#7-bronze-layer--raw-ingestion)
8. [Silver Layer — Cleansing and Integration](#8-silver-layer--cleansing-and-integration)
9. [Gold Layer — Signal Detection](#9-gold-layer--signal-detection)
10. [Signal Detection Logic (PRR and ROR)](#10-signal-detection-logic-prr-and-ror)
11. [Reporting Layer — Power BI](#11-reporting-layer--power-bi)
12. [Notebook Execution Order](#12-notebook-execution-order)
13. [Data Dictionary](#13-data-dictionary)
14. [Design Decisions and Why](#14-design-decisions-and-why)

---

## 1. What This System Does

PharmaSignalLake is a batch data pipeline that:

1. **Reads** seven raw FAERS source files (CSV-like, `$`-delimited) from cloud storage
2. **Stores** them as-is in a Bronze Delta table layer — no transformations, full audit trail
3. **Cleans and joins** them in a Silver layer — deduplicated, standardized, integrated
4. **Computes** drug safety signals in a Gold layer using PRR and ROR disproportionality math
5. **Exposes** the results to Power BI for dashboarding

The pipeline runs manually on Databricks Free Edition (no scheduling). One run processes one or more quarters.

---

## 2. If You Know SQL Server and SSIS — Read This First

You already know the concepts. The tools just have different names.

| What You Know | PharmaSignalLake Equivalent | Notes |
|---------------|----------------------------|-------|
| SQL Server database | Databricks Catalog / Unity Catalog | Holds schemas and tables |
| SQL Server table | Delta Lake table | Parquet files + transaction log on DBFS |
| SSMS query window | Databricks notebook (SQL or Python cell) | Notebooks are the primary dev surface |
| SSIS package | Databricks notebook | One notebook = one pipeline stage |
| SSIS flat file source | Bronze ingestion notebook | Reads raw delimited files, no transforms |
| SSIS data flow (transforms) | Silver transformation notebook | Cleansing, deduplication, joins |
| SSIS aggregation transform | Gold signal notebook | GROUP BY + PRR/ROR math |
| SQL Server Agent job | Not available on Free Edition | Run notebooks manually in order |
| T-SQL | PySpark DataFrame API or Spark SQL | You can write SQL inside notebooks too |
| `SELECT INTO` | `df.write.format("delta").save(path)` | Writes a DataFrame to a Delta table |
| SSMS Object Explorer | Databricks Data Explorer | Browse catalogs, schemas, tables |
| `.bak` backup file | Delta Lake time travel (`VERSION AS OF`) | Roll back to any previous table state |

**The key mindset shift:** In SSIS you drag and drop components. In PySpark you chain DataFrame operations. The logic is identical — filter, join, transform, aggregate — just written as code.

---

## 3. Technology Stack

| Component | Tool | Why |
|-----------|------|-----|
| Compute | Databricks Free Edition | Industry-standard platform for lakehouse engineering; zero cost |
| Processing engine | Apache Spark (PySpark) | Handles multi-million-row FAERS files; distributed processing |
| Storage format | Delta Lake | ACID transactions, time travel, schema enforcement on Parquet |
| File storage | Unity Catalog Managed Volume | Free Edition enforces Unity Catalog by default, which disables DBFS root access. Raw files are stored in a Unity Catalog Volume at `/Volumes/workspace/default/pharma_raw`. |
| Orchestration | Manual notebook execution | Free Edition has no job scheduler |
| Reporting | Power BI Desktop | Free; connects to exported Gold data; industry standard |
| Version control | GitHub | All notebooks exported and committed after each session |
| Language | Python (PySpark) + Spark SQL | Industry standard for Databricks pipelines |

> **Note — Unity Catalog and DBFS:** Databricks Free Edition defaults to Unity Catalog governance, which disables DBFS root access (`DBFS_DISABLED` / `SQLSTATE 56038`). Raw FAERS files cannot be stored at `/FileStore/...`. Instead, a Unity Catalog Managed Volume is used. The Volume path `/Volumes/<catalog>/<schema>/<volume>` is the Unity Catalog equivalent of a DBFS folder for raw file storage and is fully accessible from PySpark notebooks.

---

## 4. High-Level Data Flow

```
FDA FAERS ZIP (public download)
        │
        ▼
  Extract 7 .txt files
  (DEMO, DRUG, REAC, INDI, OUTC, RPSR, THER)
        │
        ▼
  Upload to Unity Catalog Volume /Volumes/workspace/default/pharma_raw/2026Q1/
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER                           │
│  Raw Delta tables — one per file type, partitioned by       │
│  source_quarter. No transformations. Full audit trail.      │
│                                                             │
│  bronze_demo  bronze_drug  bronze_reac  bronze_indi         │
│  bronze_outc  bronze_rpsr  bronze_ther                      │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                      SILVER LAYER                           │
│  Cleansed, deduplicated, standardized, joined.              │
│                                                             │
│  silver_cases          (DEMO + OUTC + RPSR joined)          │
│  silver_drug_reactions (DRUG + REAC joined, PS only)        │
│  silver_therapy        (THER cleaned)                       │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                       GOLD LAYER                            │
│  Aggregated, analytics-ready. Signal detection applied.     │
│                                                             │
│  gold_signals          (PRR, ROR, signal classification)    │
│  gold_demographics     (age/sex/country distribution)       │
│  gold_outcomes         (outcome code distribution)          │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
              Export to CSV / Parquet
                          │
                          ▼
                   Power BI Desktop
          ┌─────────────────────────────┐
          │  Page 1: Signal Leaderboard │
          │  Page 2: Demographics       │
          │  Page 3: Case Outcomes      │
          └─────────────────────────────┘
```

---

## 5. Project Folder Structure

### DBFS File Storage

```
/FileStore/
└── pharma_signal_lake/
    ├── raw/
    │   └── 2026Q1/
    │       ├── DEMO26Q1.txt
    │       ├── DRUG26Q1.txt
    │       ├── INDI26Q1.txt
    │       ├── OUTC26Q1.txt
    │       ├── REAC26Q1.txt
    │       ├── RPSR26Q1.txt
    │       └── THER26Q1.txt
    ├── bronze/
    │   ├── bronze_demo/        ← Delta table
    │   ├── bronze_drug/
    │   ├── bronze_reac/
    │   ├── bronze_indi/
    │   ├── bronze_outc/
    │   ├── bronze_rpsr/
    │   └── bronze_ther/
    ├── silver/
    │   ├── silver_cases/
    │   ├── silver_drug_reactions/
    │   └── silver_therapy/
    ├── gold/
    │   ├── gold_signals/
    │   ├── gold_demographics/
    │   └── gold_outcomes/
    └── exports/
        ├── gold_signals.csv
        ├── gold_demographics.csv
        └── gold_outcomes.csv
```

### GitHub Repository Structure

```
pharma-signal-lake/
├── README.md
├── BRD.md
├── ARCHITECTURE.md
├── SETUP.md
├── notebooks/
│   ├── 00_config.py
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_clean.py
│   ├── 03_gold_signals.py
│   ├── 04_export.py
│   └── 99_run_pipeline.py
├── docs/
│   └── data_dictionary.md
└── powerbi/
    └── pharma_signal_lake.pbix
```

---

## 6. Medallion Architecture

The Medallion Architecture is a three-layer pattern for organizing data in a lakehouse. Think of it as progressively refining raw material into a finished product.

| Layer | Analogy | Purpose | Data State |
|-------|---------|---------|------------|
| **Bronze** | Raw warehouse receipt | Land data exactly as received | Unmodified source data + audit columns |
| **Silver** | Quality control | Fix, standardize, integrate | Cleansed, deduplicated, joined |
| **Gold** | Finished product | Deliver analytics-ready data | Aggregated, enriched, metrics-computed |

**Why this matters for your portfolio:** Medallion is the most-asked-about data architecture pattern in Databricks interviews. Being able to explain what each layer contains and why is table stakes.

---

## 7. Bronze Layer — Raw Ingestion

**Purpose:** Land every FAERS source file into Delta format with zero transformation.  
**Principle:** Bronze is sacred — never modify source data. If something breaks downstream, Bronze is your recovery point.

### What Happens

1. Read each `.txt` file from DBFS using PySpark with `$` as the delimiter
2. Add two audit columns to every row: `load_timestamp` (current time) and `source_quarter` (e.g., `"2026Q1"`)
3. Write to a Delta table at the Bronze path, partitioned by `source_quarter`
4. Log the row count; raise an error if zero rows were read

### Bronze Tables

| Table Name | Source File | Partition Key |
|-----------|-------------|---------------|
| `bronze_demo` | DEMO26Q1.txt | `source_quarter` |
| `bronze_drug` | DRUG26Q1.txt | `source_quarter` |
| `bronze_reac` | REAC26Q1.txt | `source_quarter` |
| `bronze_indi` | INDI26Q1.txt | `source_quarter` |
| `bronze_outc` | OUTC26Q1.txt | `source_quarter` |
| `bronze_rpsr` | RPSR26Q1.txt | `source_quarter` |
| `bronze_ther` | THER26Q1.txt | `source_quarter` |

### Key Design Rules

- All columns land as `STRING` type at Bronze — no casting at this layer
- `OVERWRITE` mode is used so re-running Bronze is safe and idempotent
- Bronze tables are never read by Power BI — they exist for recovery and auditing only

---

## 8. Silver Layer — Cleansing and Integration

**Purpose:** Produce clean, trusted, integrated tables ready for analysis.  
**Principle:** Every transformation is intentional and documented. Nothing is dropped silently.

### silver_cases (DEMO + OUTC + RPSR)

**Transformations applied:**

| Step | What | Why |
|------|------|-----|
| Deduplication | Keep highest `caseversion` per `primaryid` | Prevents double-counting updated cases |
| Age casting | Cast `age` + `age_cod` to a normalized numeric age in years | Enables demographic analysis |
| Age flag | Flag nulls/invalid ages as `age_quality = 'MISSING'` instead of dropping | Preserves case count; flags data quality |
| Outcome join | Left join OUTC on `primaryid` | Adds `outc_cod` to the case record |
| Source join | Left join RPSR on `primaryid` | Adds `rpsr_cod` (reporter type) to the case record |
| Audit columns | `silver_load_timestamp`, `source_quarter` | Traceability |

### silver_drug_reactions (DRUG + REAC)

**Transformations applied:**

| Step | What | Why |
|------|------|-----|
| Role filter | Keep only `role_cod = 'PS'` (Primary Suspect) | Signal detection targets suspect drugs, not concomitant |
| Drug name standardize | UPPER + TRIM on `drugname` and `prod_ai` | Prevents `"Aspirin"` and `"ASPIRIN"` from being counted separately |
| PT standardize | UPPER + TRIM on `pt` (MedDRA Preferred Term) | Same reason — consistent grouping for signal math |
| Join | DRUG joined to REAC on `primaryid` | Creates one row per (case, drug, reaction) combination |
| Audit columns | `silver_load_timestamp`, `source_quarter` | Traceability |

### silver_therapy (THER)

**Transformations applied:**

| Step | What | Why |
|------|------|-----|
| Date standardize | Parse `start_dt` and `end_dt` from YYYYMMDD string to date type | Enables duration calculation |
| Duration compute | Calculate therapy duration in days where start and end are both present | Supports exposure analysis |
| Audit columns | `silver_load_timestamp`, `source_quarter` | Traceability |

---

## 9. Gold Layer — Signal Detection

**Purpose:** Produce analytics-ready aggregated tables with safety signal metrics computed.

### gold_signals

Built from `silver_drug_reactions`. Contains one row per unique `(drug, reaction)` pair.

| Column | Description |
|--------|-------------|
| `drugname` | Standardized drug name |
| `pt` | MedDRA Preferred Term (reaction) |
| `case_count` | Number of cases reporting this drug-reaction pair (A) |
| `drug_total` | Total cases reporting this drug for any reaction (B) |
| `event_total` | Total cases reporting this reaction for any drug (C) |
| `total_reports` | Total cases in the dataset (N) |
| `prr` | Proportional Reporting Ratio |
| `ror` | Reporting Odds Ratio |
| `signal_flag` | `"SIGNAL"`, `"WEAK_SIGNAL"`, or `"NO_SIGNAL"` |
| `source_quarter` | Quarter the data came from |

### gold_demographics

Built from `silver_cases`. Aggregated age group, sex, and country distributions.

### gold_outcomes

Built from `silver_cases`. Distribution of outcome codes (hospitalized, death, life-threatening, etc.).

---

## 10. Signal Detection Logic (PRR and ROR)

### The Contingency Table

Signal detection is built on comparing how often a specific drug-reaction pair appears versus how often you would expect it to appear by chance.

For a given drug **D** and reaction **R**:

```
                  Reaction R    All other reactions
Drug D            A             B - A
All other drugs   C - A         N - B - C + A

Where:
  A = cases reporting drug D AND reaction R
  B = total cases reporting drug D (any reaction)
  C = total cases reporting reaction R (any drug)
  N = total cases in the entire dataset
```

### PRR — Proportional Reporting Ratio

```
PRR = (A / B) / ((C - A) / (N - B))
```

**Plain English:** How much more often is this reaction reported for this drug compared to how often it's reported for every other drug?

- PRR = 1.0 → no disproportionality (expected rate)
- PRR = 3.0 → this reaction is reported 3x more often for this drug than for all other drugs combined
- PRR ≥ 2 AND cases ≥ 3 → Evans Criteria signal threshold

### ROR — Reporting Odds Ratio

```
ROR = (A / (B - A)) / ((C - A) / (N - B - C + A))
```

**Plain English:** The odds of this reaction given this drug, divided by the odds of this reaction given any other drug.

ROR is more statistically stable than PRR when case counts are small. Both are computed and included in `gold_signals`.

### Signal Classification

| Classification | Rule |
|----------------|------|
| `SIGNAL` | `case_count >= 3` AND `prr >= 2` |
| `WEAK_SIGNAL` | `case_count >= 3` AND `1.5 <= prr < 2` |
| `NO_SIGNAL` | Everything else |

---

## 11. Reporting Layer — Power BI

Power BI Desktop reads from exported CSV/Parquet files from the Gold layer.

### Dashboard Pages

**Page 1 — Signal Leaderboard**
- Table: Top drug-event pairs ranked by PRR descending
- Columns: Drug Name, Reaction (PT), Case Count, PRR, ROR, Signal Flag
- Filters: Signal Flag, Drug Name
- KPI cards: Total signals detected, Total cases analyzed

**Page 2 — Patient Demographics**
- Charts: Age group distribution (bar), Sex distribution (donut), Top 10 reporter countries (bar)
- Filter: Drug Name (to see demographics for a specific drug's cases)

**Page 3 — Case Outcomes**
- Chart: Outcome code distribution (bar: DE=Death, HO=Hospitalized, LT=Life-threatening, etc.)
- Chart: Reporter type distribution (HP=Healthcare Professional, CSM=Consumer, FGN=Foreign)

### Connection Method

Gold layer data is exported to CSV using the `04_export.py` notebook. Power BI connects to these local CSV files using **Get Data → Text/CSV**. This approach requires no Databricks connector license and works fully within the Free Edition and Power BI Desktop constraints.

---

## 12. Notebook Execution Order

Run notebooks in this sequence. Each notebook depends on the previous layer being complete.

```
00_config.py           ← Define all paths, file names, and settings. No data touched.
        │
        ▼
01_bronze_ingest.py    ← Read raw .txt files → write 7 Bronze Delta tables
        │
        ▼
02_silver_clean.py     ← Read Bronze → clean/join → write 3 Silver Delta tables
        │
        ▼
03_gold_signals.py     ← Read Silver → compute PRR/ROR → write 3 Gold Delta tables
        │
        ▼
04_export.py           ← Read Gold → export to CSV for Power BI
        │
        ▼
99_run_pipeline.py     ← (Optional) Runs 01 through 04 in sequence via %run magic
```

**Adding a new quarter:** Update `source_quarter` in `00_config.py` and the raw file path. Run notebooks 01 through 04. New data is appended to existing Delta tables via partition.

---

## 13. Data Dictionary

### Key Codes and Their Meanings

**DRUG.role_cod**

| Code | Meaning |
|------|---------|
| PS | Primary Suspect — the drug most likely associated with the adverse event |
| SS | Secondary Suspect |
| C | Concomitant — taken at the same time but not suspected |
| I | Interacting — suspected drug interaction |

**OUTC.outc_cod**

| Code | Meaning |
|------|---------|
| DE | Death |
| LT | Life-Threatening |
| HO | Hospitalization — Initial or Prolonged |
| DS | Disability |
| CA | Congenital Anomaly |
| RI | Required Intervention to Prevent Permanent Impairment |
| OT | Other Serious |

**RPSR.rpsr_cod**

| Code | Meaning |
|------|---------|
| HP | Healthcare Professional |
| CSM | Consumer |
| FGN | Foreign |
| LIT | Literature |
| UF | User Facility |
| DDR | Drug Distributor |

**DEMO.occp_cod**

| Code | Meaning |
|------|---------|
| MD | Physician |
| PH | Pharmacist |
| HP | Other Healthcare Professional |
| LW | Lawyer |
| CN | Consumer |
| OT | Other |

**DEMO.age_cod**

| Code | Meaning |
|------|---------|
| YR | Years |
| MON | Months |
| WK | Weeks |
| DY | Days |
| HR | Hours |
| DEC | Decades |

---

## 14. Design Decisions and Why

These rationales are worth memorizing — they are real interview questions.

| Decision | Why |
|----------|-----|
| **Delta Lake over plain Parquet** | Delta adds ACID transactions, schema enforcement, and time travel. For a pipeline that runs repeatedly (multiple quarters, re-runs on error), this prevents partial writes from corrupting data. Plain Parquet has no transaction guarantees. |
| **Medallion Architecture (3 layers)** | Separating raw, cleansed, and aggregated data means a bug in Silver never destroys Bronze. You can always re-derive Silver from Bronze. Each layer has a clear, testable contract. |
| **primaryid as join key, not caseid** | `caseid` alone doesn't uniquely identify a record — a case can have multiple versions. `primaryid` = `caseid` + version. Using `caseid` as the join key would produce duplicate rows. |
| **PS filter in Silver (role_cod = 'PS')** | Signal detection asks "did this drug cause this reaction?" Concomitant drugs are present but not suspected. Including them would dilute PRR/ROR values and produce false negatives. |
| **Uppercase + trim for drug names** | FAERS has no controlled vocabulary for drug names. The same drug appears as "aspirin", "ASPIRIN", "Aspirin ", and "ASPIR IN" across different reports. Without normalization, the signal math fragments across variants. |
| **Deduplication on highest caseversion** | FAERS allows reporters to update a case by submitting a new version. If both versions are kept, the case is double-counted. The latest version (`max(caseversion)`) is the most current report. |
| **Evans Criteria (≥3 cases, PRR ≥2)** | A PRR of 10 on 1 case is statistically meaningless — one unusual case is not a signal. The Evans minimum case threshold prevents false positives from rare single-report associations. |
| **Power BI over Tableau** | Power BI Desktop is free. Tableau requires a license. For a portfolio project, using a free tool with full functionality is the correct choice. Power BI is also the dominant tool in clinical data organizations. |
| **Manual notebook execution** | Databricks Free Edition does not support job scheduling. This is a constraint, not a design choice. In a production environment on paid Databricks, notebooks would be wrapped in a Workflow and scheduled. The architecture already supports this — `99_run_pipeline.py` is the scheduling entrypoint. |
| **One notebook per pipeline stage** | Mirrors professional Databricks project structure. Each notebook is independently testable and debuggable. A single monolithic notebook is harder to maintain and impossible to unit test. |

---

*End of ARCHITECTURE.md — Version 1.0 — PharmaSignalLake*
