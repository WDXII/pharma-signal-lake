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

1. **Ingests** raw FAERS quarterly data (7 source files, ~600K–900K cases per quarter)
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

ROR is an odds-ratio-based complement to PRR, more statistically stable at low case counts.

```
ROR = (A / (B - A)) / ((C - A) / (N - B - C + A))
```

### Evans Criteria (Signal Classification)

| Classification | Rule |
|---|---|
| `SIGNAL` | case_count ≥ 3 **AND** PRR ≥ 2.0 |
| `WEAK_SIGNAL` | case_count ≥ 3 **AND** PRR ≥ 1.5 |
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

> **Note:** Raw FAERS data files are not included in this repository due to their large size (~1.8 GB per quarter). See [Getting the Data](#getting-the-data) below.

---

## Repository Structure

```
pharma-signal-lake/
│
├── README.md                  ← You are here
├── BRD.md                     ← Business Requirements Document
├── ARCHITECTURE.md            ← Technical architecture and design decisions
├── SETUP.md                   ← Step-by-step environment setup guide
├── .gitignore
│
├── notebooks/
│   ├── 00_config.py           ← All configuration — paths, constants, settings
│   ├── 01_bronze_ingest.py    ← Raw ingestion → Bronze Delta tables
│   ├── 02_silver_clean.py     ← Cleansing and integration → Silver Delta tables
│   ├── 03_gold_signals.py     ← PRR/ROR signal detection → Gold Delta tables
│   ├── 04_export.py           ← Export Gold → CSV for Power BI
│   ├── 99_run_pipeline.py     ← Run full pipeline end-to-end
│   └── validation_queries.sql ← Spark SQL for data quality checks
│
├── data/
│   ├── .gitignore             ← Prevents raw FAERS files from being committed
│   └── README.md              ← Instructions for placing FAERS data files here
│
└── powerbi/
    └── pharma_signal_lake.pbix  ← Power BI report file
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

See **[SETUP.md § Step 5](SETUP.md#6-step-5--upload-data-to-dbfs)** for upload instructions.

---

## Running the Pipeline

### Option A — Run the Full Pipeline in One Click

1. Open `notebooks/99_run_pipeline.py` in Databricks
2. Uncomment all `%run` lines
3. Click **Run All**

### Option B — Run Notebooks Individually

Run in this order. Each notebook depends on the previous layer being complete.

```
00_config.py          ← Always run first (re-initializes variables)
01_bronze_ingest.py   ← Reads raw files → Bronze Delta
02_silver_clean.py    ← Cleans → Silver Delta
03_gold_signals.py    ← Computes PRR/ROR → Gold Delta
04_export.py          ← Exports Gold → CSV
```

### Adding a New Quarter

1. Download the new quarter's FAERS files
2. Upload to Unity Catalog Volume at `/Volumes/workspace/default/pharma_raw/{QUARTER}/`
3. In `00_config.py`, change one line:
   ```python
   QUARTER = "2025Q4"   # ← update this
   ```
4. Run the pipeline — new data is appended as a new partition. Existing quarters are untouched.

---

## Power BI Dashboard

The dashboard has three pages, each reading from a Gold export CSV:

| Page | Source Table | Key Visuals |
|------|-------------|-------------|
| **Signal Leaderboard** | `gold_signals.csv` | Top drug-reaction pairs ranked by PRR; case count, ROR columns; signal flag filter |
| **Patient Demographics** | `gold_demographics.csv` | Age group bar chart; sex donut chart; top 15 reporter countries bar chart |
| **Case Outcomes** | `gold_outcomes.csv` | Outcome distribution (death, hospitalization, etc.); reporter type distribution |

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Delta Lake over Parquet | ACID transactions + time travel. Re-running the pipeline never corrupts existing data. |
| Medallion Architecture | Each layer has a clear contract. Bronze is always recoverable. Silver and Gold are independently testable. |
| `primaryid` as join key | `caseid` is not unique — a case can have multiple versions. `primaryid` includes the version number. |
| PS filter in Silver | Signal detection only uses Primary Suspect drugs. Including concomitant drugs would dilute PRR/ROR. |
| Uppercase + trim for drug names | FAERS has no drug name vocabulary control. Normalization prevents fragmentation (e.g., `"ASPIRIN"` vs `"Aspirin "`). |
| Evans Criteria ≥ 3 cases | A PRR of 10 on 1 case is statistically meaningless. The case threshold prevents false positives. |
| Power BI via CSV export | No Databricks connector license required. Works entirely within the Free Edition and Power BI Desktop constraints. |

---

## Limitations

This is a portfolio project. The following simplifications are documented and intentional:

- **Drug name normalization is basic** (uppercase + trim only). A production system would use a MedDRA or WHO drug dictionary for entity resolution.
- **No automated scheduling** — Databricks Free Edition does not support job scheduling. The pipeline runs manually.
- **Single data source** — only FDA FAERS. A production pharmacovigilance system would also ingest EudraVigilance (EU) and WHO VigiAccess for global coverage.
- **No confidence intervals** — PRR and ROR are computed as point estimates. Production implementations typically include 95% CI and chi-square significance testing.

---

## Documents

| Document | Purpose |
|----------|---------|
| [BRD.md](BRD.md) | Business requirements — problem statement, functional requirements, stakeholders, risks |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Technical design — data flow, Medallion layers, signal math, design decisions |
| [SETUP.md](SETUP.md) | Environment setup — Databricks account, Unity Catalog Volume upload, GitHub, Power BI |

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

The FAERS data used is public domain, published by the U.S. Food and Drug Administration.

---

## Author

**[Your Name]**
[LinkedIn](https://linkedin.com/in/yourprofile) · [GitHub](https://github.com/yourusername)

*Portfolio project — built to demonstrate Databricks, PySpark, and clinical data engineering skills.*
