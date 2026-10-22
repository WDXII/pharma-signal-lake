# SETUP.md

**Project:** PharmaSignalLake  
**Version:** 1.1  
**Date:** May 2026 (Revised 2026-09-29)  
**Purpose:** Environment setup guide — from zero to a running pipeline

> **Revision 1.1** — Updated Step 6's notebook list and sync instructions to include
> `05_profile.py`, `06_validate_fix.py`, and the `src/`/`tests/` sync paths added since
> the original setup (see `DEVELOPER.md` Issue 10).

---

## Table of Contents

1. [Prerequisites Checklist](#1-prerequisites-checklist)
2. [Step 1 — Create a Databricks Free Edition Account](#2-step-1--create-a-databricks-free-edition-account)
3. [Step 2 — Set Up Your Workspace](#3-step-2--set-up-your-workspace)
4. [Step 3 — Create the Compute Cluster](#4-step-3--create-the-compute-cluster)
5. [Step 4 — Download the FAERS Data](#5-step-4--download-the-faers-data)
6. [Step 5 — Upload Data to Unity Catalog Volume](#6-step-5--upload-data-to-unity-catalog-volume)
7. [Step 6 — Set Up the Notebook Folder Structure](#7-step-6--set-up-the-notebook-folder-structure)
8. [Step 7 — Connect GitHub for Version Control](#8-step-7--connect-github-for-version-control)
9. [Step 8 — Set Up Power BI Desktop](#9-step-8--set-up-power-bi-desktop)
10. [Step 9 — Verify Your Environment](#10-step-9--verify-your-environment)
11. [Free Edition Limitations and How to Work Around Them](#11-free-edition-limitations-and-how-to-work-around-them)
12. [Troubleshooting](#12-troubleshooting)

---

## 1. Prerequisites Checklist

Complete all items before starting Step 1.

| Item | Requirement | Where to Get It |
|------|-------------|-----------------|
| Email address | Any personal email; do NOT use a corporate email | — |
| GitHub account | Free account | https://github.com |
| Power BI Desktop | Free desktop application | https://powerbi.microsoft.com/desktop |
| Web browser | Chrome or Edge recommended for Databricks | — |
| Storage space | ~2 GB free on your local machine | — |
| Internet connection | Required throughout; FAERS ZIP files are ~300–500 MB | — |
| Python + pytest (optional) | Not needed to run the pipeline or the unit tests — tests run inside Databricks via `tests/run_unit_tests.py` (no local Java/Spark on most machines; see DEVELOPER.md Issue 2).

**No paid subscriptions are required.** Databricks Free Edition, GitHub Free, and Power BI Desktop are all zero-cost.

---

## 2. Step 1 — Create a Databricks Free Edition Account

> **Note:** The old Databricks Community Edition was retired on January 1, 2026. The replacement is Databricks Free Edition. Do not search for "Community Edition" — use "Free Edition."

### Steps

1. Go to: https://www.databricks.com/try-databricks
2. Click **Start for free**
3. Fill in your name, email, and create a password
4. Select your cloud provider. For Free Edition, choose **AWS** (it is pre-configured and requires no AWS account of your own)
5. Verify your email address via the confirmation link
6. Log in to your new workspace at the URL provided (format: `https://dbc-xxxxxxxx.cloud.databricks.com`)

### What You Get

- A pre-provisioned workspace on AWS
- Serverless compute with a daily usage quota
- Unity Catalog Volumes for storing data files (DBFS root is disabled on Free Edition)
- Full notebook environment
- Unity Catalog (managed metadata)
- No credit card required

---

## 3. Step 2 — Set Up Your Workspace

### Create the Folder Structure in Workspace

1. In the left sidebar, click **Workspace**
2. Under your username folder, click the **⋮** (three dots) menu → **Create** → **Folder**
3. Name it: `PharmaSignalLake`
4. Inside `PharmaSignalLake`, create a subfolder: `notebooks`

Your workspace path will be:
```
/Workspace/Users/williamdataworks@gmail.com/PharmaSignalLake/files/
```

This is where all your `.py` notebooks will live.

---

## 4. Step 3 — Create the Compute Cluster

On Databricks Free Edition, compute is **serverless** — there is no persistent cluster to manage. When you run a notebook, compute spins up automatically and shuts down after a period of inactivity.

### Steps

1. In the left sidebar, click **Compute**
2. You should see a **Serverless** option listed — this is your default compute on Free Edition
3. No action needed — serverless is attached automatically when you open a notebook

### Daily Quota

Free Edition has a daily Databricks Unit (DBU) quota. Processing one FAERS quarter uses approximately 20–40% of the daily quota depending on file sizes.

**Best practice:** Run the pipeline in a single session when possible. If you hit the quota mid-run, your notebook will pause. Resume the next day by re-running from the last completed notebook (each notebook is independently idempotent).

---

## 5. Step 4 — Download the FAERS Data

### Where to Download

Official FDA FAERS quarterly data page:
```
https://www.fda.gov/drugs/questions-and-answers-fdas-adverse-event-reporting-system-faers/fda-adverse-event-reporting-system-faers-latest-quarterly-data-files
```

### Steps

1. Go to the URL above
2. Find the section **FAERS ASCII Files**
3. Download the most recent available quarter (e.g., `faers_ascii_2026Q1.zip`)
4. Save the ZIP file to your local machine
5. Extract the ZIP — you will find a folder containing seven `.txt` files:

```
faers_ascii_2026Q1/
├── DEMO26Q1.txt
├── DRUG26Q1.txt
├── INDI26Q1.txt
├── OUTC26Q1.txt
├── REAC26Q1.txt
├── RPSR26Q1.txt
└── THER26Q1.txt
```

### File Sizes (2026Q1 — Observed Actuals)

| File | Size |
|------|------|
| DEMO26Q1.txt | 56 MB |
| DRUG26Q1.txt | 157 MB |
| REAC26Q1.txt | 48 MB |
| INDI26Q1.txt | 52 MB |
| OUTC26Q1.txt | 6 MB |
| RPSR26Q1.txt | 0.25 MB |
| THER26Q1.txt | 14 MB |

Total extracted: approximately 333 MB for 2026Q1. File sizes vary by quarter.

---

## 6. Step 5 — Upload Data to Unity Catalog Volume

Unity Catalog Volumes are the storage layer that your notebooks read from. DBFS root is disabled on Free Edition due to Unity Catalog enforcement.

### Create the Volume (One-Time Setup)

1. In the left sidebar, click **Catalog**
2. Navigate to **workspace** → **default** (or create a schema if needed)
3. Click **Create** → **Volume**
4. Name it: `pharma_raw`
5. Volume type: **Managed**
6. Click **Create**

Your Volume path will be: `/Volumes/workspace/default/pharma_raw`

### Upload Files

1. In **Catalog**, navigate to your Volume: **workspace** → **default** → **pharma_raw**
2. Click **Upload** (or drag and drop files)
3. Create a folder for the quarter: `2026Q1`
4. Upload all seven `.txt` files into that folder
5. Confirm files are visible at `/Volumes/workspace/default/pharma_raw/2026Q1/`

### Alternative: Upload via Notebook

```python
# List files to confirm upload
display(dbutils.fs.ls("/Volumes/workspace/default/pharma_raw/2026Q1/"))
```

### Naming Convention for Additional Quarters

When you add a second quarter later:
```
/Volumes/workspace/default/pharma_raw/2025Q4/
    DEMO25Q4.txt
    DRUG25Q4.txt
    ...
```

No other changes are intended — `00_config.py` handles the rest. This has been verified on 2026Q1 only; a second-quarter run is planned after the initial release.

---

## 7. Step 6 — Sync Notebooks to Databricks

Notebooks are maintained locally and synced to Databricks using the VS Code Databricks extension with the `databricks.yml` Asset Bundle config already included in the repository. **Do not create notebooks manually in the Databricks UI.**

### Notebooks in This Project

| Notebook | Purpose |
|----------|---------|
| `00_config.py` | All configuration variables — paths, file names, settings |
| `01_bronze_ingest.py` | Read raw files → write Bronze Delta tables |
| `02_silver_clean.py` | Clean and integrate → write Silver Delta tables |
| `03_gold_signals.py` | Compute PRR/ROR → write Gold Delta tables |
| `04_export.py` | Export Gold tables to CSV for Power BI |
| `05_profile.py` | Read-only: before/after row-count and data-quality profiling. Not run by `99_run_pipeline.py` |
| `06_validate_fix.py` | Read-only: independent validation of signal-detection correctness. Not run by `99_run_pipeline.py` |
| `99_run_pipeline.py` | Run notebooks 01-04 in order via `%run` |

`src/signal_math.py` holds the pure PRR/ROR/chi_sq/dedup functions that `02_silver_clean.py`
and `03_gold_signals.py` import, and `tests/` holds their pytest unit tests (run inside
Databricks via `tests/run_unit_tests.py` — see Section 11, no local Python/Spark needed).

### How to Sync

1. Open the project folder in VS Code (`pharma-signal-lake/`)
2. Install the **Databricks** VS Code extension (see DEVELOPER.md Step 2)
3. Connect the extension to your workspace using a PAT (see DEVELOPER.md Steps 1 and 3)
4. In the Databricks panel, click **Sync State: STOPPED** to start continuous sync
5. `notebooks/`, `src/`, and `tests/` (per `databricks.yml`'s `sync.paths`) are automatically pushed to Databricks on every save

Notebooks land at:
```
/Workspace/Users/<your-email>/PharmaSignalLake/files/notebooks/
```
`src/` and `tests/` land as sibling folders under `.../files/`. If a newly added top-level
sync folder doesn't appear remotely on its own, see DEVELOPER.md Issue 10 for the workaround.

> **No Python environment required.** The sync uses the Databricks API directly — no local PySpark or `databricks-connect` installation is needed. See DEVELOPER.md Issue 2 for details.

---

## 8. Step 7 — Connect GitHub for Version Control

Connecting GitHub ensures your work is never lost if your Free Edition account becomes inactive.

### Steps

1. Create a GitHub repository named `pharma-signal-lake` (public or private — your choice)
2. Clone the repository locally or initialize it in the project folder
3. All notebooks are maintained locally and synced to Databricks via the VS Code extension (see Step 6)
4. Commit and push from **VS Code Source Control** after each working session

### Best Practice: Commit After Every Session

After each working session, commit from VS Code Source Control with a descriptive message. This protects your work and builds a visible commit history on GitHub — which recruiters and hiring managers look at.

> **Note:** Databricks has a built-in Git integration (Settings → Linked accounts → GitHub), but this project uses VS Code + the Databricks extension for the development workflow. Commits are made from VS Code, not from within the Databricks UI.

---

## 9. Step 8 — Set Up Power BI Desktop

### Download and Install

1. Go to: https://powerbi.microsoft.com/desktop
2. Download Power BI Desktop (free, Windows only)
3. Install with default settings
4. Open Power BI Desktop — no account required for desktop-only use

### Connecting to Gold Data

After `04_export.py` has run, your Gold CSV files will be available at:
```
/Volumes/workspace/default/pharma_raw/exports/gold_signals.csv
/Volumes/workspace/default/pharma_raw/exports/gold_demographics.csv
/Volumes/workspace/default/pharma_raw/exports/gold_outcomes.csv
```

### Downloading Exports from Unity Catalog Volume to Local

1. In Databricks, go to **Catalog** → **workspace** → **default** → **pharma_raw** → **exports**
2. Click each CSV file → **Download**
3. Save to a local folder, e.g.: `C:\PharmaSignalLake\exports\`

### Loading into Power BI

1. Open Power BI Desktop
2. **Home** → **Get Data** → **Text/CSV**
3. Browse to your local exports folder
4. Load `gold_signals.csv`, `gold_demographics.csv`, and `gold_outcomes.csv`
5. Leave the tables unrelated. If Power BI created a relationship, delete it in Model view. The tables share only `source_quarter`, which holds one value, so a relationship would filter nothing useful (see ARCHITECTURE.md, section 11). To stop it happening again: **File** → **Options and settings** → **Options** → **Current File** → **Data Load** → untick **Autodetect new relationships after data is loaded**

### Checks after loading

Open **Data view** (or a Table visual) and compare the row counts. These are the 2026Q1 values; a second quarter changes them.

| Table | Expected rows |
|---|---|
| `gold_signals` | 373,012 |
| `gold_demographics` | 34 |
| `gold_outcomes` | 11 |

If a count differs, check the column types first (a number loaded as text breaks the measures). The expected measure values per page are in `powerbi/README.md`.

---

## 10. Step 9 — Verify Your Environment

Run through this checklist before writing any pipeline code.

### Databricks Verification

Open a new notebook and run the following cells one by one:

**Cell 1 — Check PySpark is working:**
```python
print(spark.version)
```
Expected output: a version string like `3.5.x`

**Cell 2 — Check Unity Catalog Volume files are visible:**
```python
display(dbutils.fs.ls("/Volumes/workspace/default/pharma_raw/2026Q1/"))
```
Expected output: a table listing all 7 `.txt` files

**Cell 3 — Check you can read a file:**
```python
df = spark.read.option("header", True).option("sep", "$").csv(
    "/Volumes/workspace/default/pharma_raw/2026Q1/DEMO26Q1.txt"
)
print(f"Row count: {df.count()}")
df.printSchema()
```
Expected output: row count > 0 and a schema showing all DEMO columns

**Cell 4 — Check Delta Lake is available:**
```python
df.limit(10).write.format("delta").mode("overwrite").save(
    "/Volumes/workspace/default/pharma_raw/test_delta/"
)
print("Delta write successful")
```
Expected output: `Delta write successful`

### Verify a Signal-Correctness Run (run order)

Run these in order. Steps 1–3 must happen **before** the fixed pipeline is run, because they capture the unfixed state.

| # | Action | Record / screenshot |
|---|--------|---------------------|
| 1 | `DESCRIBE HISTORY` on `gold_signals` | The current version number — call it `<V>` (time-travel fallback) |
| 2 | `sql/verify_signal_fix.sql` queries **A–E** | Save the output as BEFORE |
| 3 | `05_profile.py` with widget `run_label = baseline` (on the current, unfixed tables; also snapshots `_gold_signals_baseline`) | Printed metrics and the Delta version it reports |
| 4 | `99_run_pipeline.py` with the fixed code | Row counts per stage; confirm no stage silently skipped |
| 5 | `05_profile.py` with `run_label = after` | Printed metrics |
| 6 | `06_validate_fix.py` | The V1–V10 PASS/FAIL table, the transition matrix, and the RESUME NUMBERS block (it raises if any check FAILs) |
| 7 | `sql/verify_signal_fix.sql` queries **A–I** with `<V>` from step 1 | Save the output as AFTER |
| 8 (optional) | Case-level review: run P1–P3 from `sql/case_review_rescued_signals.sql` for the top rescued signals | Save the output; record findings in `docs/validation-results.md` |

Unit tests (independent of the steps above): run `tests/run_unit_tests.py` in Databricks; expect all tests to pass.

Fill in [docs/validation-results.md](docs/validation-results.md) from these outputs. Notebooks 05 and 06 only read pipeline tables and are not part of `99_run_pipeline.py`. Attach them to Serverless/interactive compute, not a SQL Warehouse.

### Power BI Verification

1. Open Power BI Desktop
2. Load one of the exported CSVs
3. Switch to **Report view**
4. Create one basic bar chart from the data
5. If it renders without error, your setup is complete

---

## 11. Free Edition Limitations and How to Work Around Them

| Limitation | Impact | Workaround |
|------------|--------|------------|
| No job scheduling | Cannot automate pipeline runs | Use `99_run_pipeline.py` to run all notebooks manually in one click |
| Daily compute quota | Pipeline may pause mid-run | Run one notebook at a time; Delta tables preserve state between sessions |
| No persistent cluster | Cluster restarts on inactivity | All state is in Delta tables in Unity Catalog Volumes — re-attach to serverless and continue |
| No production-grade Unity Catalog | Limited governance features | Sufficient for portfolio; document what you would do differently at enterprise scale |
| No direct Power BI connector | Cannot use live Databricks connection | Export Gold to CSV; load into Power BI Desktop locally |
| Session timeout | Variables in memory are lost on restart | `00_config.py` re-initializes all variables; run it first each session |

---

## 12. Troubleshooting

### "File not found" error when reading raw files

- Verify the file was uploaded to the exact path: `/Volumes/workspace/default/pharma_raw/2026Q1/`
- File names are case-sensitive. `DEMO26Q1.txt` ≠ `demo26q1.txt`
- Run `dbutils.fs.ls("/Volumes/workspace/default/pharma_raw/2026Q1/")` to confirm what is there

### Row count is zero after reading a file

- The FAERS delimiter is `$` — if you omit `option("sep", "$")`, PySpark will fail to parse columns
- Check the file is not empty: download it locally and open in a text editor to confirm it has data

### Delta write fails with "path already exists"

- This is expected on a re-run. All tables (Bronze, Silver, and Gold) use `.mode("overwrite")` partitioned by `source_quarter` — re-running for the same quarter is safe and idempotent

### Delta write fails after adding columns

- `gold_signals` gained `chi_sq` and `haldane_applied`. Writing new columns over an existing table fails with a schema-mismatch error unless the write sets `.option("overwriteSchema", "true")`. `03_gold_signals.py` already does; if you write the table from elsewhere, add it

### DIVIDE_BY_ZERO under ANSI mode

- A division guard is wrong. Do **not** disable ANSI mode (`spark.sql.ansi.enabled`) — that only hides the bad denominator (DECISIONS.md D5, DEVELOPER.md Issue 7). Find the expression that divides and fix the math so the denominator cannot be zero (the Haldane correction handles zero contingency-table cells)
- Similarly, multiplying several BIGINT marginals in raw SQL can raise `ARITHMETIC_OVERFLOW`; cast to DOUBLE first

### Compute shows "quota exceeded"

- You have used your daily DBU allowance
- Wait until midnight UTC and retry
- Consider running notebooks at off-peak hours to maximize available quota

### GitHub commit fails

- Ensure your GitHub personal access token has `repo` scope
- Re-link your GitHub account under **Settings** → **Linked accounts**

### Power BI shows blank visuals

- Confirm the CSV exported from Gold contains rows: open it in Excel first
- Check that column names in Power BI match the field names from the Gold table
- Verify the CSV delimiter is comma (`,`) not `$` — the export notebook should write standard CSVs

---

*End of SETUP.md — Version 1.1 — PharmaSignalLake*
