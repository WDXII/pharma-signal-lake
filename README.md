# PharmaSignalLake

> A Pharmacovigilance Signal Detection Lakehouse built on Databricks, PySpark, and Delta Lake

[![Platform](https://img.shields.io/badge/Platform-Databricks%20Free%20Edition-FF3621)](https://www.databricks.com/try-databricks)
[![Language](https://img.shields.io/badge/Language-PySpark%203.5-4B8BBE)](https://spark.apache.org/docs/latest/api/python/)
[![Storage](https://img.shields.io/badge/Storage-Delta%20Lake-00ADD8)](https://delta.io)
[![Dashboard](https://img.shields.io/badge/Dashboard-Power%20BI%20Desktop-F2C811)](https://powerbi.microsoft.com/desktop)
[![Data](https://img.shields.io/badge/Data-FDA%20FAERS%20Public-blue)](https://www.fda.gov/drugs/questions-and-answers-fdas-adverse-event-reporting-system-faers/fda-adverse-event-reporting-system-faers-latest-quarterly-data-files)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## What This Project Does

Pharmaceutical companies are legally required to monitor their drugs for safety signals after market approval — a practice called **pharmacovigilance**. The FDA receives hundreds of thousands of adverse event reports every quarter and publishes them publicly through the **FAERS (FDA Adverse Event Reporting System)**.

PharmaSignalLake is an end-to-end **data lakehouse pipeline** that:

1. **Ingests** raw FAERS quarterly data (7 source files, ~400K cases per quarter)
2. **Cleanses and integrates** across all source files into a unified analytical layer
3. **Detects safety signals** using industry-standard disproportionality algorithms (PRR and ROR)
4. **Classifies signals** using Evans Criteria — the same threshold used in real pharma signal detection
5. **Surfaces results** through a Power BI executive dashboard

---

## Architecture

```
FDA FAERS (public download)
        │
        ▼
┌───────────────────────────────────────┐
│  BRONZE  — Raw Delta tables           │
│  7 source files, no transformation    │
│  Partitioned by source_quarter        │
└─────────────────┬─────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────┐
│  SILVER  — Cleansed & Integrated      │
│  silver_cases · silver_drug_reactions │
│  silver_therapy                       │
└─────────────────┬─────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────┐
│  GOLD  — Signal Detection             │
│  gold_signals · gold_demographics     │
│  gold_outcomes                        │
└─────────────────┬─────────────────────┘
                  │
                  ▼
           Power BI Dashboard
   Signal Leaderboard · Demographics
           Case Outcomes
```

The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold), a standard pattern in enterprise lakehouse engineering.

---

## Signal Detection Method

### Proportional Reporting Ratio (PRR)

For each drug–reaction pair, PRR measures how much more often a reaction is reported for a specific drug compared to all other drugs in the database.

```
PRR = (A / B) / ((C - A) / (N - B))

A = cases reporting drug D AND reaction R
B = total cases reporting drug D (any reaction)
C = total cases reporting reaction R (any drug)
N = total cases in the dataset
```

### Reporting Odds Ratio (ROR)

ROR is an odds-ratio-based complement to PRR.

```
ROR = (A / (B - A)) / ((C - A) / (N - B - C + A))
```

### Zero Cells (Haldane-Anscombe Correction)

If any contingency-table cell is zero, PRR and ROR are computed after adding 0.5 to all four cells (Haldane-Anscombe correction), so they are never null. The `haldane_applied` column marks these pairs. `chi_sq` always uses the raw cells (D2).

### Evans Criteria (Signal Classification)

| Classification | Rule |
|---|---|
| `SIGNAL` | case_count ≥ 3 **AND** PRR ≥ 2.0 **AND** chi-squared (Yates-corrected) ≥ 4.0 |
| `NO_SIGNAL` | All other pairs |

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Cloud Platform | Databricks Free Edition (AWS) |
| Processing Engine | Apache Spark 3.5 (PySpark) |
| Storage Format | Delta Lake |
| File Storage | Unity Catalog Volumes |
| Language | Python 3 / Spark SQL |
| Dashboard | Power BI Desktop |
| Version Control | GitHub |

**Total cost: $0** — Databricks Free Edition, Power BI Desktop, and GitHub Free are all zero-cost.

---

## Data Source

**FDA Adverse Event Reporting System (FAERS)**
Public quarterly downloads — no registration or license required.

```
https://www.fda.gov/drugs/questions-and-answers-fdas-adverse-event-reporting-system-faers/fda-adverse-event-reporting-system-faers-latest-quarterly-data-files
```

### FAERS File Types

| File | Contents |
|------|----------|
| `DEMO` | Patient demographics — age, sex, reporter country |
| `DRUG` | All drugs in the report — role, name, dose, route |
| `REAC` | Adverse reactions (MedDRA Preferred Terms) |
| `INDI` | Drug indications |
| `OUTC` | Case outcomes — death, hospitalization, etc. |
| `RPSR` | Report sources — healthcare professional, consumer |
| `THER` | Therapy dates — start, end, duration |

> **Note:** Raw FAERS data files are not included in this repository (~330 MB per quarter for 2026Q1). See [Getting the Data](#getting-the-data) below.

---

## Repository Structure

```
pharma-signal-lake/
│
├── README.md                  ← You are here
├── BRD.md                     ← Business Requirements Document
├── ARCHITECTURE.md            ← Technical architecture and design decisions
├── SETUP.md                   ← Step-by-step environment setup guide
├── DEVELOPER.md               ← Dev environment notes and known issues/resolutions
├── DECISIONS.md               ← Architecture and correctness decisions, as made
├── databricks.yml             ← Databricks Asset Bundle config (notebook/src/test sync)
├── .gitignore
│
├── notebooks/
│   ├── 00_config.py           ← All configuration — paths, constants, settings
│   ├── 01_bronze_ingest.py    ← Raw ingestion → Bronze Delta tables
│   ├── 02_silver_clean.py     ← Cleansing and integration → Silver Delta tables
│   ├── 03_gold_signals.py     ← PRR/ROR signal detection → Gold Delta tables
│   ├── 04_export.py           ← Export Gold → CSV for Power BI
│   ├── 05_profile.py          ← Read-only: before/after row-count and quality metrics
│   ├── 06_validate_fix.py     ← Read-only: independent signal-correctness validation
│   ├── 99_run_pipeline.py     ← Run full pipeline end-to-end
│   └── validation_queries.sql ← Spark SQL for data quality checks
│
├── src/
│   └── signal_math.py         ← Pure PRR/ROR/chi_sq/dedup functions, unit-testable
│
├── sql/
│   ├── verify_signal_fix.sql           ← Independent verification queries (read-only for the assistant, D6)
│   └── case_review_rescued_signals.sql ← Case-level review of the top rescued signals (manual, read-only)
│
├── tests/
│   ├── conftest.py            ← Spark fixture (attaches to Databricks' running session)
│   ├── test_signals.py        ← pytest unit tests for src/signal_math.py
│   └── run_unit_tests.py      ← Notebook runner (installs pytest, runs the suite)
│
├── docs/
│   ├── data_dictionary.md     ← Column-level schema for Bronze/Silver/Gold tables
│   └── validation-results.md  ← Verified before/after results for the signal-correctness fix (2026Q1)
│
├── data/
│   ├── .gitignore             ← Prevents raw FAERS files from being committed
│   └── README.md              ← Instructions for placing FAERS data files here
│
└── powerbi/
    └── PharmaSignals.pbip  ← Power BI project (text files; see powerbi/README.md)
```

---

## Getting Started

### Prerequisites

- Databricks Free Edition account — [sign up here](https://www.databricks.com/try-databricks)
- Power BI Desktop — [download here](https://powerbi.microsoft.com/desktop) (Windows only, free)
- GitHub account (free)

See **[SETUP.md](SETUP.md)** for complete step-by-step setup instructions including environment verification.

---

## Getting the Data

1. Go to the [FDA FAERS quarterly data page](https://www.fda.gov/drugs/questions-and-answers-fdas-adverse-event-reporting-system-faers/fda-adverse-event-reporting-system-faers-latest-quarterly-data-files)
2. Under **FAERS ASCII Files**, download the most recent quarter ZIP (e.g., `faers_ascii_2026Q1.zip`)
3. Extract the ZIP — you will get 7 `.txt` files
4. Place them in the `data/2026Q1/` folder of this repository (they are gitignored — they will not be committed)
5. Upload the same files to Unity Catalog Volume at `/Volumes/workspace/default/pharma_raw/2026Q1/`

See **[SETUP.md § Step 5](SETUP.md#6-step-5--upload-data-to-unity-catalog-volume)** for upload instructions.

---

## Running the Pipeline

### Option A — Run the Full Pipeline in One Click

1. Open `notebooks/99_run_pipeline.py` in Databricks
2. Attach it to **Serverless** compute (not a SQL Warehouse, which cannot run these cells)
3. Click **Run All**. It ends with a per-stage UTC timing table

### Option B — Run Notebooks Individually

Run in this order. Each notebook depends on the previous layer being complete.

```
00_config.py          ← Always run first (re-initializes variables)
01_bronze_ingest.py   ← Reads raw files → Bronze Delta
02_silver_clean.py    ← Cleans → Silver Delta
03_gold_signals.py    ← Computes PRR/ROR → Gold Delta
04_export.py          ← Exports Gold → CSV
```

### Option C — Verify a Run

Follow the run order (seven steps plus an optional case review) in **[SETUP.md § Verify Your Environment](SETUP.md#10-step-9--verify-your-environment)**: record the `gold_signals` Delta version, capture the BEFORE baseline, run the pipeline, then run `05_profile.py` (`after`), `06_validate_fix.py` (V1–V10), and `sql/verify_signal_fix.sql`. Notebooks 05 and 06 are read-only and are not part of `99_run_pipeline.py`.

### Adding a New Quarter (untested)

1. Download the new quarter's FAERS files
2. Upload to Unity Catalog Volume at `/Volumes/workspace/default/pharma_raw/{QUARTER}/`
3. In `00_config.py`, change one line:
   ```python
   QUARTER = "2025Q4"   # ← update this
   ```
4. Run the pipeline.

Verified on 2026Q1 only. The notebooks write with `mode("overwrite")` partitioned by `source_quarter` and no `replaceWhere`, so whether a second quarter appends or replaces existing data has not been tested; a second-quarter run is planned after the initial release.

---

## Power BI Dashboard

**Status: in progress. The Signal Leaderboard page is built; Patient Demographics and Case Outcomes are next.** The dashboard has three pages, each reading from a Gold export CSV:

| Page | Source Table | Key Visuals |
|------|-------------|-------------|
| **Signal Leaderboard** | `gold_signals.csv` | Main leaderboard = non-exclusive `SIGNAL` pairs, top 10 ranked by report count (PRR breaks ties, D13), with report count, PRR and ROR columns and an `H` column that flags Haldane-corrected rows (D15). The main leaderboard filter is `SIGNAL` AND NOT `is_exclusive` (49,878 pairs on 2026Q1); a separate "Exclusive reports" panel shows the 106 `SIGNAL` pairs where the drug is the only reporter of the event (D8, amended). `is_exclusive` is derived in Power BI as `case_count = event_total` |
| **Patient Demographics** | `gold_demographics.csv` | Age group bar chart; gender donut chart; top 10 reporter countries bar chart (`EU` and unspecified excluded, D11) |
| **Case Outcomes** | `gold_outcomes.csv` | Outcome distribution (death, hospitalization, etc.); report source distribution |

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Delta Lake over Parquet | ACID transactions + time travel. Re-running the pipeline never corrupts existing data. |
| Medallion Architecture | Each layer has a clear contract. Bronze is always recoverable. Silver and Gold are independently testable. |
| `primaryid` as join key | `caseid` is not unique — a case can have multiple versions. `primaryid` includes the version number. |
| PS filter in Silver | Signal detection only uses Primary Suspect drugs. Including concomitant drugs would dilute PRR/ROR. |
| Uppercase + trim for drug names | FAERS has no drug name vocabulary control. Normalization prevents fragmentation (e.g., `"ASPIRIN"` vs `"Aspirin "`). |
| Full Evans Criteria (cases, PRR, chi-squared) | A PRR of 10 on 1 case is statistically meaningless. Requiring chi-squared (Yates) ≥ 4 in addition to the case-count and PRR thresholds screens out low-confidence pairs that PRR alone would flag. |
| Haldane-Anscombe correction on zero cells | Without it, any pair with an empty contingency-table cell gets a null PRR/ROR and silently drops out of signal detection — including some of the most exclusive, clinically interesting drug-event pairs. |
| D1 — Binary `signal_flag` | `SIGNAL` / `NO_SIGNAL` only; the 1.5 "weak" tier had no literature basis. |
| D2 — Haldane correction | +0.5 to all four cells when any is 0, for PRR/ROR only, so no pair gets a null PRR. |
| D3 — Full Evans | `case_count ≥ 3` AND `PRR ≥ 2` AND Yates `chi_sq ≥ 4`. |
| D4 — Dedup on `caseid` | `primaryid` already includes the version, so deduping on it is a no-op; keep max `caseversion` per `caseid`, semi-join child tables. |
| D5 — ANSI mode stays on | Division safety comes from the math, not from disabling engine checks. |
| D6 — Independent verifier | `sql/verify_signal_fix.sql` is written by a human, outside the coding session, and is read-only for the assistant. |
| D7 — One row per case in Silver | OUTC/RPSR are aggregated per `primaryid` before joining, so `silver_cases` does not fan out. |
| D8 — Exclusive pairs in a separate panel | Pairs where every report of the event names this drug have an uninterpretable PRR. Most rest on few reports (median 4), a handful on many. They go in their own Power BI panel (`is_exclusive`), shown without PRR or ROR, not in the main leaderboard. |
| D9 — Report-level dedup deferred | `case_count` counts reports, not patients. Patient-level duplicate detection is deferred (see Limitations). |
| D10 — Per-stage timing log | `99_run_pipeline.py` logs stage times in UTC, in memory only. |
| Power BI via CSV export | No Databricks connector license required. Works entirely within the Free Edition and Power BI Desktop constraints. |

---

## Limitations

This is a portfolio project. The following simplifications are documented and intentional:

- **Drug name normalization is text-only** (uppercase + trim only). A production system would use a MedDRA or WHO drug dictionary for entity resolution.
- **No automated scheduling** — Databricks Free Edition does not support job scheduling. The pipeline runs manually.
- **Single data source** — only FDA FAERS. A production pharmacovigilance system would also ingest EudraVigilance (EU) and WHO VigiAccess for global coverage.
- **Haldane is a convention** — +0.5 to each cell is the common continuity correction, not the only defensible one; it affects only sparse pairs and never `chi_sq`.
- **Single quarter verified** — the signal-fix validation has been run on 2026Q1 only.
- **Disproportionality is not causality.** Signals are hypotheses for clinical review, not confirmed adverse reactions.
- **No confidence intervals** — PRR and ROR are computed as point estimates (chi-square significance testing via Evans Criteria is implemented; 95% CI is not).
- **`case_count` counts reports, not patients.** The same literature case submitted by several manufacturers, related reports filed together, or follow-ups filed under new case IDs can inflate n. Case-ID deduplication (D4) cannot detect these (D9).

---

## Results & Validation

Verified on 2026Q1 against the pre-fix baseline (Delta time travel, `gold_signals` v1 → v3):

- **Independent verification passes:** `06_validate_fix.py` V1–V10 10/10 PASS, and the independent SQL recompute matched all 373,012 pairs (PRR, ROR, χ² and Haldane mismatches all 0).
- **One row per case in Silver:** `silver_cases` fell from 470,891 to 397,224 rows (73,667 fan-out rows removed, 15.6%), equal to the 397,224 distinct cases.
- **Exclusive pairs no longer hidden:** null PRR went from 2,978 pairs to 0, and 106 exclusive pairs moved from `NO_SIGNAL` to `SIGNAL`. They are shown in a separate "Exclusive reports" panel, not the main leaderboard.
- **Stricter classification:** `SIGNAL` went from 56,491 to 49,984 (−6,507, −11.5%). 6,613 pairs (11.7% of 56,491) were lost, all because they failed the χ² ≥ 4 criterion.
- **Case-level review of the top 10 rescued signals:** 3 rest on fewer than three likely patients (report duplication), see [docs/validation-results.md](docs/validation-results.md).
- **Label review of the top 10 rescued pairs (2026-10-04):** 7 known/labeled, 1 confounding by indication, 2 likely artifact. This describes these ten pairs only; it does not validate the method.
- **Dedup was a latent bug:** it removed 0 rows in 2026Q1 (0 superseded case versions), but prevents double-counting when quarters are added.

Baseline-vs-after metrics, check results (V1–V10 and `sql/verify_signal_fix.sql`), the signal transition matrix, and limitations are recorded in **[docs/validation-results.md](docs/validation-results.md)**.

---

## Documents

| Document | Purpose |
|----------|---------|
| [BRD.md](BRD.md) | Business requirements — problem statement, functional requirements, stakeholders, risks |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Technical design — data flow, Medallion layers, signal math, design decisions |
| [SETUP.md](SETUP.md) | Environment setup — Databricks account, Unity Catalog Volume upload, GitHub, Power BI |
| [DECISIONS.md](DECISIONS.md) | Architecture and correctness decisions, recorded as they were made, with confirmation evidence |
| [DEVELOPER.md](DEVELOPER.md) | Dev environment notes and known issues/resolutions, numbered as encountered |
| [docs/validation-results.md](docs/validation-results.md) | Verified before/after results, checks V1–V10 and the case-level review (2026Q1) |
| [docs/data_dictionary.md](docs/data_dictionary.md) | Column-level schema reference for every Bronze, Silver, and Gold table |

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

The FAERS data used is public domain, published by the U.S. Food and Drug Administration.

---

## Author

**Will Daluyen**

- LinkedIn: [linkedin.com/in/wdaluyen](https://www.linkedin.com/in/wdaluyen/)
- GitHub: [github.com/WDXII](https://github.com/WDXII)
- Portfolio: [placeholder]
- Email: william.daluyen@gmail.com

*Portfolio project — built to demonstrate Databricks, PySpark, and clinical data engineering skills.*

AI-assisted (Claude, Claude Code). The verification SQL (`sql/verify_signal_fix.sql`) was written by Will and locked read-only to the coding agent (D6, commit `d0f5097`); clinical interpretation is the author's own.