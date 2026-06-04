# SETUP.md

**Project:** PharmaSignalLake  
**Version:** 1.0  
**Date:** May 2026  
**Purpose:** Environment setup guide — from zero to a running pipeline

---

## Table of Contents

1. [Prerequisites Checklist](#1-prerequisites-checklist)
2. [Step 1 — Create a Databricks Free Edition Account](#2-step-1--create-a-databricks-free-edition-account)
3. [Step 2 — Set Up Your Workspace](#3-step-2--set-up-your-workspace)
4. [Step 3 — Create the Compute Cluster](#4-step-3--create-the-compute-cluster)
5. [Step 4 — Download the FAERS Data](#5-step-4--download-the-faers-data)
6. [Step 5 — Upload Data to DBFS](#6-step-5--upload-data-to-dbfs)
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
- DBFS (Databricks File System) for storing data files
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
/Users/your-email@example.com/PharmaSignalLake/notebooks/
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

### File Sizes (Approximate — 1 Quarter)

| File | Approx. Size |
|------|-------------|
| DEMO26Q1.txt | ~150 MB |
| DRUG26Q1.txt | ~600 MB |
| REAC26Q1.txt | ~300 MB |
| INDI26Q1.txt | ~200 MB |
| OUTC26Q1.txt | ~80 MB |
| RPSR26Q1.txt | ~60 MB |
| THER26Q1.txt | ~400 MB |

Total extracted: approximately 1.8 GB for one quarter.

---

## 6. Step 5 — Upload Data to DBFS

DBFS (Databricks File System) is the storage layer that your notebooks read from. You must upload the FAERS files here.

### Steps

1. In the left sidebar, click **Data** (the cylinder icon)
2. Click **Add Data** → **Upload Files**
3. In the upload dialog, set the DBFS target path to:
   ```
   /FileStore/pharma_signal_lake/raw/2026Q1/
   ```
4. Upload all seven `.txt` files one by one (or select all and upload together if your browser allows)
5. After upload, confirm the files are visible by going to **Catalog** → **DBFS** → **FileStore** → **pharma_signal_lake** → **raw** → **2026Q1**

### Alternative: Upload via Notebook

You can also upload via a notebook cell using `dbutils.fs.cp` if you have the files accessible as a URL or mounted storage. This is covered in the `00_config.py` notebook comments.

### Naming Convention for Additional Quarters

When you add a second quarter later:
```
/FileStore/pharma_signal_lake/raw/2025Q4/
    DEMO25Q4.txt
    DRUG25Q4.txt
    ...
```

No other changes required — `00_config.py` handles the rest.

---

## 7. Step 6 — Set Up the Notebook Folder Structure

Create the following empty notebooks inside your `PharmaSignalLake/notebooks/` workspace folder.

| Notebook Name | Purpose |
|---------------|---------|
| `00_config` | All configuration variables — paths, file names, settings |
| `01_bronze_ingest` | Read raw files → write Bronze Delta tables |
| `02_silver_clean` | Clean and integrate → write Silver Delta tables |
| `03_gold_signals` | Compute PRR/ROR → write Gold Delta tables |
| `04_export` | Export Gold tables to CSV for Power BI |
| `99_run_pipeline` | Run all notebooks in order via `%run` |

### How to Create a Notebook

1. Right-click on the `notebooks` folder in the Workspace pane
2. Select **Create** → **Notebook**
3. Set **Default Language** to **Python**
4. Name the notebook (e.g., `00_config`)
5. Click **Create**

Repeat for each of the six notebooks listed above.

---

## 8. Step 7 — Connect GitHub for Version Control

Connecting GitHub ensures your work is never lost if your Free Edition account becomes inactive.

### Steps

1. In the top-right corner of your Databricks workspace, click your profile icon
2. Go to **Settings** → **Linked accounts**
3. Under **GitHub**, click **Connect**
4. Authorize Databricks to access your GitHub account (read/write to repositories)
5. Create a new GitHub repository named `pharma-signal-lake` (public or private — your choice)
6. In Databricks, open a notebook → click the **Git** button in the top toolbar
7. Link the notebook to your GitHub repository

### Best Practice: Commit After Every Session

After each working session:

1. Open each notebook you modified
2. Click **Git** → **Commit and push**
3. Write a brief commit message (e.g., `"Complete bronze ingestion notebook"`)

This protects your work and builds a visible commit history on GitHub — which recruiters and hiring managers look at.

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
/FileStore/pharma_signal_lake/exports/gold_signals.csv
/FileStore/pharma_signal_lake/exports/gold_demographics.csv
/FileStore/pharma_signal_lake/exports/gold_outcomes.csv
```

### Downloading Exports from DBFS to Local

1. In Databricks, go to **Data** → **DBFS** → navigate to `/FileStore/pharma_signal_lake/exports/`
2. Click each CSV file → **Download**
3. Save to a local folder, e.g.: `C:\PharmaSignalLake\exports\`

### Loading into Power BI

1. Open Power BI Desktop
2. **Home** → **Get Data** → **Text/CSV**
3. Browse to your local exports folder
4. Load `gold_signals.csv`, `gold_demographics.csv`, and `gold_outcomes.csv`
5. In the data model, create relationships if needed (they share `source_quarter`)

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

**Cell 2 — Check DBFS files are visible:**
```python
display(dbutils.fs.ls("/FileStore/pharma_signal_lake/raw/2026Q1/"))
```
Expected output: a table listing all 7 `.txt` files

**Cell 3 — Check you can read a file:**
```python
df = spark.read.option("header", True).option("sep", "$").csv(
    "/FileStore/pharma_signal_lake/raw/2026Q1/DEMO26Q1.txt"
)
print(f"Row count: {df.count()}")
df.printSchema()
```
Expected output: row count > 0 and a schema showing all DEMO columns

**Cell 4 — Check Delta Lake is available:**
```python
df.limit(10).write.format("delta").mode("overwrite").save(
    "/FileStore/pharma_signal_lake/test_delta/"
)
print("Delta write successful")
```
Expected output: `Delta write successful`

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
| No persistent cluster | Cluster restarts on inactivity | All state is in Delta tables on DBFS — re-attach to serverless and continue |
| No production-grade Unity Catalog | Limited governance features | Sufficient for portfolio; document what you would do differently at enterprise scale |
| No direct Power BI connector | Cannot use live Databricks connection | Export Gold to CSV; load into Power BI Desktop locally |
| Session timeout | Variables in memory are lost on restart | `00_config.py` re-initializes all variables; run it first each session |

---

## 12. Troubleshooting

### "File not found" error when reading raw files

- Verify the file was uploaded to the exact path: `/FileStore/pharma_signal_lake/raw/2026Q1/`
- File names are case-sensitive on DBFS. `DEMO26Q1.txt` ≠ `demo26q1.txt`
- Run `dbutils.fs.ls("/FileStore/pharma_signal_lake/raw/2026Q1/")` to confirm what is there

### Row count is zero after reading a file

- The FAERS delimiter is `$` — if you omit `option("sep", "$")`, PySpark will fail to parse columns
- Check the file is not empty: download it locally and open in a text editor to confirm it has data

### Delta write fails with "path already exists"

- This is expected on a re-run. Always use `.mode("overwrite")` for Bronze and Silver writes
- Gold tables use `.mode("append")` if adding new quarters — confirm the mode is correct

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

*End of SETUP.md — Version 1.0 — PharmaSignalLake*
