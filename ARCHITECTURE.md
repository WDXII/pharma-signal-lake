# ARCHITECTURE.md

**Project:** PharmaSignalLake  
**Version:** 1.1  
**Date:** May 2026 (Revised 2026-09-29)  
**Stack:** Databricks Free Edition · PySpark · Delta Lake · Power BI Desktop

> **Revision 1.1** — Updated the Silver dedup key, the `gold_signals` schema, the PRR/ROR
> formulas, and Signal Classification to match `DECISIONS.md` D1-D4 and D7; updated the
> repository structure to include `src/`, `tests/`, `docs/`, `DECISIONS.md`, and notebooks
> 05-06 (see `DEVELOPER.md` Issues 13-14).

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
15. [Validation Layer](#15-validation-layer)

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
| SQL Server table | Delta Lake table | Parquet files + transaction log in Unity Catalog Volume |
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

### Unity Catalog Volume Storage

```
/Volumes/workspace/default/pharma_raw/
├── 2026Q1/
│   ├── DEMO26Q1.txt
│   ├── DRUG26Q1.txt
│   ├── INDI26Q1.txt
│   ├── OUTC26Q1.txt
│   ├── REAC26Q1.txt
│   ├── RPSR26Q1.txt
│   └── THER26Q1.txt
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
├── DEVELOPER.md              ← Dev environment notes and known issues
├── DECISIONS.md              ← Architecture and correctness decisions, as made
├── databricks.yml            ← Databricks Asset Bundle config (sync target + workspace host)
├── requirements-dev.txt      ← Local editor support only (no local PySpark required)
├── notebooks/
│   ├── 00_config.py
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_clean.py
│   ├── 03_gold_signals.py
│   ├── 04_export.py
│   ├── 05_profile.py        ← Read-only: before/after profiling (not part of 99_run_pipeline)
│   ├── 06_validate_fix.py   ← Read-only: independent signal-correctness validation
│   └── 99_run_pipeline.py
├── src/
│   └── signal_math.py       ← Pure PRR/ROR/chi_sq/dedup functions, unit-testable
├── tests/
│   ├── conftest.py
│   ├── test_signals.py
│   └── run_unit_tests.py
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

1. Read each `.txt` file from the Unity Catalog Volume using PySpark with `$` as the delimiter
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
| Deduplication | Keep highest `caseversion` per `caseid` (the case entity key — `primaryid` already includes `caseversion` and is unique per row) | Prevents double-counting updated cases |
| Age casting | Cast `age` + `age_cod` to a normalized numeric age in years | Enables demographic analysis |
| Age flag | Flag nulls/invalid ages as `age_quality = 'MISSING'` instead of dropping | Preserves case count; flags data quality |
| Outcome join | Aggregate OUTC to a comma-separated `outc_cod` list per `primaryid`, then left join | A case can have multiple outcome codes; joining without aggregating would fan `silver_cases` out to one row per (case, outcome) combination instead of one row per case |
| Source join | Aggregate RPSR to a comma-separated `rpsr_cod` list per `primaryid`, then left join | Same reasoning as the outcome join |
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
| `prr` | Proportional Reporting Ratio. Haldane-Anscombe corrected when any cell is zero — never null |
| `ror` | Reporting Odds Ratio. Same correction as `prr` — never null |
| `chi_sq` | Yates-corrected chi-squared, computed on the raw (uncorrected) cells. Null if any marginal is zero |
| `haldane_applied` | `true` if any contingency-table cell was zero and the Haldane-Anscombe correction was applied |
| `signal_flag` | `"SIGNAL"` or `"NO_SIGNAL"` (binary) |
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
- PRR ≥ 2 AND cases ≥ 3 AND chi-squared ≥ 4 → full Evans Criteria signal threshold

### ROR — Reporting Odds Ratio

```
ROR = (A / (B - A)) / ((C - A) / (N - B - C + A))
```

**Plain English:** The odds of this reaction given this drug, divided by the odds of this reaction given any other drug.

ROR is more statistically stable than PRR when case counts are small. Both are computed and included in `gold_signals`.

### Haldane-Anscombe Correction

If any of the four contingency-table cells (`A`, `B-A`, `C-A`, `N-B-C+A`) is zero, PRR and ROR
are undefined by the formulas above. Rather than let those pairs fall out of signal detection
with a null value, 0.5 is added to all four cells before computing PRR/ROR (not `chi_sq`, which
always uses the raw cells). This is recorded per-pair in a `haldane_applied` column, so every
pair — including the most exclusive, clinically interesting ones — gets a finite, if
conservative, PRR/ROR.

### Signal Classification

Full Evans Criteria — binary, no intermediate tier:

| Classification | Rule |
|----------------|------|
| `SIGNAL` | `case_count >= 3` AND `prr >= 2` AND `chi_sq (Yates) >= 4` |
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

**Adding a new quarter:** Update `QUARTER` in `00_config.py` and upload the new files to the Volume. Run notebooks 01 through 04. New data is written as a new partition; existing quarters are untouched.

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
| **Deduplication on `caseid`, keep highest `caseversion`** | FAERS allows reporters to update a case by submitting a new version. `primaryid` = `caseid` + version, so it's already unique per row — deduplicating on it is a no-op. `caseid` is the actual case entity key; keeping the highest `caseversion` per `caseid` is the fix. |
| **Full Evans Criteria (≥3 cases, PRR ≥2, chi-squared ≥4)** | A PRR of 10 on 1 case is statistically meaningless — one unusual case is not a signal. The case-count and chi-squared thresholds together screen out both rare single-report associations and pairs where PRR alone would overstate confidence. |
| **Haldane-Anscombe correction on zero cells** | Without it, PRR/ROR are undefined (null) whenever a contingency-table cell is zero — including the most exclusive drug-event pairs, the ones most worth flagging. The correction (+0.5 to all four cells) trades a small amount of precision for making every pair classifiable. |
| **Aggregate OUTC/RPSR before joining in `silver_cases`** | Both files can have multiple rows per case. Joining without aggregating first fans `silver_cases` out to one row per (case, outcome, source) combination instead of one row per case. `gold_outcomes` explodes the aggregated list back out before its per-code distribution, so its output is unaffected. |
| **D1 — Binary `signal_flag`** | Evans defines one threshold; the earlier 1.5 "weak" tier had no literature basis and diluted the meaning of a flag. See DECISIONS.md D1. |
| **D2 — Haldane correction, PRR/ROR only** | Applied only when a cell is 0, and never to `chi_sq`, so the significance test stays on real counts. See DECISIONS.md D2. |
| **D3 — Full Evans (n ≥ 3 AND PRR ≥ 2 AND χ² ≥ 4)** | See the Evans row above and DECISIONS.md D3. |
| **D4 — Dedup on `caseid`; child tables semi-joined** | See the deduplication row above and DECISIONS.md D4. |
| **D5 — ANSI mode stays ON** | Division safety comes from the math (Haldane makes every cell positive), not from disabling engine checks. Disabling ANSI (DEVELOPER.md Issue 7) hid the zero-cell problem instead of fixing it. See DECISIONS.md D5. |
| **D6 — Verifier is independent** | `sql/verify_signal_fix.sql` is written by a human outside the coding session and is read-only for the assistant; a fix cannot be graded by the code that made it. See DECISIONS.md D6. |
| **D7 — One row per case in `silver_cases`** | See the OUTC/RPSR aggregation row above and DECISIONS.md D7. |
| **Power BI over Tableau** | Power BI Desktop is free. Tableau requires a license. For a portfolio project, using a free tool with full functionality is the correct choice. Power BI is also the dominant tool in clinical data organizations. |
| **Manual notebook execution** | Databricks Free Edition does not support job scheduling. This is a constraint, not a design choice. In a production environment on paid Databricks, notebooks would be wrapped in a Workflow and scheduled. The architecture already supports this — `99_run_pipeline.py` is the scheduling entrypoint. |
| **One notebook per pipeline stage** | Mirrors professional Databricks project structure. Each notebook is independently testable and debuggable. A single monolithic notebook is harder to maintain and impossible to unit test. |

---

## 15. Validation Layer

The validation layer sits *beside* the Medallion layers. It reads pipeline tables and never
writes to them; it is **not** part of `99_run_pipeline.py`.

| Component | Role | Writes to |
|-----------|------|-----------|
| `notebooks/05_profile.py` | Row-count and data-quality metrics per layer; run once as `baseline` on the unfixed tables, once as `after` | `gold/_profile_results`, `gold/_gold_signals_baseline` (baseline run only) |
| `notebooks/06_validate_fix.py` | Checks V1–V10 (PASS/FAIL, raises on any FAIL), an independent plain-SQL recompute of every pair (V9), and the before/after report | its own `_`-prefixed tables only |
| `sql/verify_signal_fix.sql` | Independent SQL verification, written by Will (D6); read-only for the assistant | nothing |
| `src/signal_math.py` + `tests/` | Pure signal-math and dedup functions and their pytest suite, run in Databricks via `tests/run_unit_tests.py` | nothing |
| `notebooks/validation_queries.sql` | Ad-hoc exploration and data-quality queries | nothing |

Run order is in [SETUP.md § Verify Your Environment](SETUP.md#10-step-9--verify-your-environment);
results are recorded in [docs/validation-results.md](docs/validation-results.md).

---

*End of ARCHITECTURE.md — Version 1.1 — PharmaSignalLake*
