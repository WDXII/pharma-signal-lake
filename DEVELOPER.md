# DEVELOPER.md — PharmaSignalLake

Developer notes, known environment issues, and resolutions encountered during development.
This file is a living log — add new entries as issues are found and resolved.

---

## Table of Contents

1. [Environment Notes](#1-environment-notes)
2. [VS Code + Databricks Sync Setup](#2-vs-code--databricks-sync-setup)
3. [Known Issues and Resolutions](#3-known-issues-and-resolutions)
   - [Issue 1 — DBFS Root Disabled on Free Edition (Unity Catalog)](#issue-1--dbfs-root-disabled-on-free-edition-unity-catalog)

---

## 1. Environment Notes

| Item | Detail |
|------|--------|
| Platform | Databricks Free Edition (AWS) |
| Governance | Unity Catalog enforced by default — cannot be disabled on Free Edition |
| Compute | Serverless (no persistent cluster) |
| Databricks Runtime | 15.x (Spark 3.5) |
| Raw file storage | Unity Catalog Managed Volume (see Issue 1) |
| Delta table storage | Unity Catalog Managed Volume |
| Power BI | Desktop (local CSV import — no live connector) |

---

## 2. VS Code + Databricks Sync Setup

Edit notebooks locally in VS Code and sync them to Databricks without manual copy-paste.
This is the recommended development workflow for this project.

### Step 1 — Generate a Personal Access Token (PAT) in Databricks

A PAT is how the VS Code extension authenticates to your workspace.

1. Log in to your Databricks workspace
2. Click your profile icon (top right) → **Settings**
3. In the left sidebar, click **Developer**
4. Under **Access tokens**, click **Manage**
5. Click **Generate new token**
6. Set a description (e.g., `vscode-databricks` or `local-dev`) and an expiry (90 days is reasonable)
   - Use a generic name — one PAT works for all projects in your workspace
7. Click **Generate**
8. **Copy the token immediately** — it is only shown once

Store the token somewhere safe (password manager). If you lose it, generate a new one.

---

### Step 2 — Install the Databricks Extension in VS Code

1. Open VS Code
2. Go to the **Extensions** panel (`Ctrl+Shift+X`)
3. Search for `Databricks`
4. Install the extension published by **Databricks** (official, not third-party)
5. Reload VS Code if prompted

---

### Step 3 — Connect the Extension to Your Workspace

1. Click the **Databricks icon** in the VS Code left sidebar (appears after install)
2. Click **Configure Databricks**
3. Enter your workspace URL — format: `https://dbc-xxxxxxxx.cloud.databricks.com`
   - Find this in your browser address bar when logged into Databricks
4. When prompted for authentication, choose **Personal Access Token**
5. Paste the PAT you generated in Step 1
6. Click **Connect** — the extension will verify the connection

If successful, you will see your workspace name appear in the Databricks panel.

---

### Step 4 — Sync Your Local Notebooks to Databricks

The project uses a **Databricks Asset Bundle (DAB)** config (`databricks.yml`) to control what gets synced and where. This replaces the manual right-click upload approach.

**`databricks.yml` is already configured** with:
- `sync.paths: [notebooks]` — only the `notebooks/` folder is synced (docs, README, etc. are excluded)
- `root_path: /Workspace/Users/williamdataworks@gmail.com/PharmaSignalLake` — DAB automatically appends `/files`, so notebooks land at `.../PharmaSignalLake/files/notebooks/`

**How DAB constructs the remote path:**

| Part | Value |
|------|-------|
| `root_path` | `.../PharmaSignalLake` |
| DAB always appends | `/files` |
| `sync.paths` adds | `/notebooks` |
| **Final remote path** | `.../PharmaSignalLake/files/notebooks/` |

**To start syncing:**

In the VS Code Databricks panel, click **Sync State: STOPPED** to start continuous sync. After that, any save to a `.py` file in `notebooks/` is automatically pushed to Databricks.

Alternatively, run from the terminal:
```powershell
databricks bundle sync
```

---

### Step 5 — Run Notebooks from VS Code (Optional)

The extension allows you to run notebooks and see output inside VS Code:

1. Open a `.py` notebook file in VS Code
2. Click the **Run** button in the Databricks panel, or right-click → **Run on Databricks**
3. Select your serverless compute
4. Output streams back to VS Code

**Note:** Running from VS Code requires a Python environment with `databricks-connect`. See Issue 2 below before attempting this. Running notebooks directly in the **Databricks web UI** is simpler and has no local Python requirement.

---

### Recommended Workflow

```
Edit .py file in VS Code
        │
        ▼
Auto-synced to Databricks (sync must be running in VS Code panel)
        │
        ▼
Run notebook in Databricks web UI
        │
        ▼
Commit and push to GitHub from VS Code Source Control
```

---

### Troubleshooting

| Problem | Fix |
|---------|-----|
| Extension shows "Connection failed" | Verify workspace URL has no trailing slash; re-paste the PAT |
| PAT rejected | Token may have expired — generate a new one in Databricks Settings → Developer |
| Sync State shows STOPPED | Click it in the Databricks panel to start — it does not auto-start |
| Notebooks land at wrong path (e.g. `/files/notebooks/files`) | Check `databricks.yml` — `root_path` must NOT end in `/notebooks`; DAB appends `/files` automatically |
| Notebook runs but uses old code | Confirm sync is running (not STOPPED) before running the notebook |

---

## 3. Known Issues and Resolutions

---

### Issue 1 — DBFS Root Disabled on Free Edition (Unity Catalog)

**Status:** Resolved  
**Date encountered:** June 2026  
**Affected files:** `notebooks/00_config.py`, `ARCHITECTURE.md`

#### Symptom

Running any notebook that references `/FileStore/...` or `dbfs:/FileStore/...` paths raised:

```
AnalysisException: DBFS_DISABLED
SQLSTATE: 56038
DBFS root access is not allowed in Unity Catalog-enabled workspaces.
```

#### Root Cause

Databricks Free Edition provisions workspaces with **Unity Catalog enabled by default**. Unity Catalog enforces stricter data governance and explicitly **disables DBFS root access** — the `/FileStore/` path (also written as `dbfs:/FileStore/`) is part of the DBFS root and is inaccessible in this mode.

The original `00_config.py` was written assuming DBFS paths, which is the correct assumption for legacy Databricks workspaces and Community Edition (retired January 2026). It is the wrong assumption for any workspace provisioned after Unity Catalog became the default.

#### Resolution

Created a Unity Catalog **Managed Volume** to serve as the raw file storage layer:

| Setting | Value |
|---------|-------|
| Catalog | `workspace` |
| Schema | `default` |
| Volume name | `pharma_raw` |
| Full volume path | `/Volumes/workspace/default/pharma_raw` |

Uploaded all 7 FAERS `.txt` files for 2026Q1 to:
```
/Volumes/workspace/default/pharma_raw/2026Q1/
    DEMO26Q1.txt
    DRUG26Q1.txt
    INDI26Q1.txt
    OUTC26Q1.txt
    REAC26Q1.txt
    RPSR26Q1.txt
    THER26Q1.txt
```

Updated `00_config.py` — all five base paths now point to the Volume:

```python
# Unity Catalog Volume — DBFS root is disabled on Free Edition (Unity Catalog default)
BASE_RAW    = f"/Volumes/workspace/default/pharma_raw/{QUARTER}"
BASE_BRONZE = "/Volumes/workspace/default/pharma_raw/bronze"
BASE_SILVER = "/Volumes/workspace/default/pharma_raw/silver"
BASE_GOLD   = "/Volumes/workspace/default/pharma_raw/gold"
BASE_EXPORT = "/Volumes/workspace/default/pharma_raw/exports"
```

All downstream path variables (`RAW_FILES`, `BRONZE`, `SILVER`, `GOLD`, `EXPORTS`) derive from these base paths and required no changes.

#### How to Create the Volume (for a new workspace)

If you need to recreate this setup from scratch:

1. In Databricks, go to **Catalog** (left sidebar)
2. Expand **workspace** catalog → **default** schema
3. Click **Create** → **Volume**
4. Set **Volume name** to `pharma_raw`, type **Managed**
5. Click **Create**
6. Go to **Data** → **Add Data** → **Upload to Volume**
7. Set target path to `/Volumes/workspace/default/pharma_raw/2026Q1/`
8. Upload all 7 FAERS `.txt` files

#### Adding a Second Quarter

When adding a new quarter, upload files to a new subfolder within the same volume:

```
/Volumes/workspace/default/pharma_raw/2025Q4/
    DEMO25Q4.txt
    ...
```

Then update `QUARTER` in `00_config.py`. No other changes needed.

#### Why Not Use a Different Catalog or Schema?

The `workspace` catalog is the default catalog provisioned by Databricks Free Edition. Using it avoids any additional catalog setup. In a production environment with multiple environments (dev/staging/prod), you would create a dedicated catalog (e.g., `pharma_dev`) and isolate schemas per environment.

#### Reference

- [Databricks Unity Catalog Volumes documentation](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [DBFS deprecation in Unity Catalog workspaces](https://docs.databricks.com/en/dbfs/index.html)

---

### Issue 2 — databricks-connect Is Not Required for This Workflow

**Status:** Resolved (by not installing it)
**Date encountered:** June 2026

#### Symptom

The VS Code Databricks panel shows two warnings:
```
⚠ Activate an environment with Python 3.12 or greater
⚠ Can't install databricks-connect without an active Python environment
```

Attempting to install `databricks-connect` on Python 3.13 fails because it pins `numpy<2`, and numpy 1.26.x has no pre-built wheel for Python 3.13 — it requires a C compiler (MSVC or GCC) to build from source, which is not installed.

#### Root Cause

`databricks-connect` is only needed for one specific workflow: executing Spark code **locally on your laptop** and sending it to a remote Databricks cluster. This is not how this project works.

#### Resolution

**Do nothing.** The warnings are advisory and can be ignored.

This project's workflow is:
1. Edit `.py` files locally in VS Code
2. Auto-sync to Databricks via the extension (no Python env required)
3. Run notebooks in the **Databricks web UI** (no Python env required)

No `.venv`, no `databricks-connect`, no local Python environment is needed. The `.venv` folder was created during troubleshooting and subsequently deleted.

#### If You Want Local Notebook Execution in the Future

You would need Python 3.12 (not 3.13) installed, since `databricks-connect 15.4` requires `numpy<2` which has no Python 3.13 wheel. Install Python 3.12 from python.org, then:

```powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
pip install databricks-connect==15.4.*
```

This is optional and not part of the standard development workflow for this project.

---

### Issue 3 — `%run` Magic Commands Must Use `# MAGIC` Prefix

**Status:** Resolved  
**Date encountered:** June 2026  
**Affected files:** `notebooks/01_bronze_ingest.py`, `02_silver_clean.py`, `03_gold_signals.py`, `04_export.py`, `99_run_pipeline.py`

#### Symptom

Running any notebook after `00_config.py` raised:

```
NameError: name 'QUARTER' is not defined
```

Despite `00_config.py` running successfully and all variables appearing to be defined there.

#### Root Cause

Databricks `.py` notebook source files use a special comment syntax to encode magic commands. A line like `%run ./00_config` must be written as:

```python
# MAGIC %run ./00_config
```

Without the `# MAGIC` prefix, Databricks treats the line as a plain Python comment — it is never executed, so `00_config` is never loaded and none of its variables (`QUARTER`, `RAW_FILES`, `BRONZE`, etc.) exist in the notebook's scope.

All five downstream notebooks had the `%run` line commented out as a plain comment (`# %run ./00_config`), not as a magic command.

#### Resolution

Changed the `%run` line in all five notebooks from:

```python
# %run ./00_config
```

to:

```python
# MAGIC %run ./00_config
```

#### Rule to Remember

In Databricks `.py` source files, every magic command (`%run`, `%sql`, `%md`, `%pip`, etc.) must be prefixed with `# MAGIC `. Without it, the line is inert Python source and will never execute.

---

### Issue 4 — FAERS Filenames Use 2-Digit Years; Config Was Generating 4-Digit

**Status:** Resolved  
**Date encountered:** June 2026  
**Affected files:** `notebooks/00_config.py`

#### Symptom

Bronze ingestion failed with `PATH_NOT_FOUND` for all 7 FAERS files:

```
Path does not exist: dbfs:/Volumes/workspace/default/pharma_raw/2026Q1/DEMO2026Q1.txt
```

#### Root Cause

FDA FAERS filenames use **2-digit years**, not 4-digit. For quarter `2026Q1`, the actual files are:
```
DEMO26Q1.txt   DRUG26Q1.txt   REAC26Q1.txt   INDI26Q1.txt
OUTC26Q1.txt   RPSR26Q1.txt   THER26Q1.txt
```

The original `RAW_FILES` config built paths using `QUARTER` directly (`DEMO{QUARTER}.txt` → `DEMO2026Q1.txt`), which doesn't match the FDA naming convention.

Confirmed by running `dbutils.fs.ls("/Volumes/workspace/default/pharma_raw/2026Q1/")` — all 7 files were present with 2-digit year names.

#### Note on `dbfs:/Volumes/` Path Display

`dbutils.fs.ls()` displays Volume paths with a `dbfs:` prefix (`dbfs:/Volumes/...`). This is Databricks' internal representation and is correct — it does NOT indicate DBFS root access. `spark.read` correctly resolves these paths.

#### Resolution

Added `QUARTER_SHORT = QUARTER[2:]` in `00_config.py` to strip the century digits (`"2026Q1"` → `"26Q1"`), then used `QUARTER_SHORT` in all `RAW_FILES` path definitions.

#### Rule to Remember

FDA FAERS filenames always use 2-digit years regardless of the century. When building file paths, use `QUARTER[2:]` to match the FDA convention. This applies to all quarters: `2024Q3` → `24Q3`, `2025Q1` → `25Q1`, etc.

---

### Issue 5 — `source_quarter` Dropped in `silver_drug_reactions` Intermediate Select

**Status:** Resolved  
**Date encountered:** June 2026  
**Affected files:** `notebooks/02_silver_clean.py`

#### Symptom

`silver_drug_reactions` failed with:

```
[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter
with name `source_quarter` cannot be resolved.
```

`silver_cases` and `silver_therapy` succeeded; only `silver_drug_reactions` failed.

#### Root Cause

In `build_silver_drug_reactions()`, the intermediate `.select()` on the `drug` DataFrame (after filtering to PS and uppercasing) explicitly listed columns but omitted `source_quarter`. Since `source_quarter` was not carried forward into the join, it was unavailable when the final `.select()` referenced it.

#### Resolution

Added `"source_quarter"` to the intermediate `.select()` on the `drug` DataFrame so it is present in the joined result.

#### Rule to Remember

When chaining `.select()` → `.join()` → `.select()`, every column needed in the final output must survive each intermediate projection. Audit trail columns (`source_quarter`, `load_timestamp`) are easy to silently drop if not explicitly included at every select step.

---

### Issue 6 — `Column.desc().nulls_last()` Is Not Valid PySpark Syntax

**Status:** Resolved  
**Date encountered:** June 2026  
**Affected files:** `notebooks/03_gold_signals.py`

#### Symptom

`gold_signals` failed with:

```
'Column' object is not callable
```

`gold_demographics` and `gold_outcomes` succeeded.

#### Root Cause

`col("prr").desc()` returns a PySpark `Column` object. Calling `.nulls_last()` on that result fails because `nulls_last` is not a method on `Column` — it is not callable. PySpark does not chain null-ordering as a separate method after `desc()`.

#### Resolution

Replaced `.desc().nulls_last()` with the combined method `.desc_nulls_last()`:

```python
# Wrong
.orderBy(col("prr").desc().nulls_last())

# Correct
.orderBy(col("prr").desc_nulls_last())
```

#### Rule to Remember

PySpark null-ordering is set via combined methods directly on `Column`:
- `col("x").asc_nulls_first()`
- `col("x").asc_nulls_last()`
- `col("x").desc_nulls_first()`
- `col("x").desc_nulls_last()`

There is no separate `.nulls_first()` or `.nulls_last()` method. Chaining `.desc().nulls_last()` will always raise `'Column' object is not callable`.

---

### Issue 7 — Photon ANSI Mode Throws DIVIDE_BY_ZERO Inside `when()` Guards

**Status:** Resolved  
**Date encountered:** June 2026  
**Affected files:** `notebooks/03_gold_signals.py`

#### Symptom

`gold_signals` failed during the Delta write with:

```
[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0
and return NULL instead. If necessary set "spark.sql.ansi.enabled" to "false".
SQLSTATE: 22012
```

#### Root Cause

Databricks Serverless uses the **Photon** execution engine, which enforces **ANSI SQL mode** by default. In ANSI mode, division by zero raises a hard error at the physical execution layer — even when the division expression is wrapped inside a `when()` condition that should logically prevent it from executing.

The PRR and ROR `when()` guards in the code are logically correct (they check denominators before dividing), but Photon evaluates all expressions at the row level and can still encounter the division before the condition short-circuits.

#### Resolution

Added `spark.conf.set("spark.sql.ansi.enabled", "false")` at the top of `03_gold_signals.py`, immediately after `%run ./00_config`. This makes Spark return `NULL` on divide-by-zero instead of throwing — which is the intended behavior for our division guards.

#### Why Not Use `try_divide`?

`try_divide(a, b)` is the ANSI-safe alternative, but it requires restructuring nested expressions like `(A/B) / ((C-A)/(N-B))` into multiple intermediate columns. Disabling ANSI mode for this notebook is simpler and appropriate here — the division guards are correct, we just need NULL-safe arithmetic.

#### Rule to Remember

Databricks Serverless + Photon enforces ANSI SQL mode by default. Any notebook performing division must either use `try_divide`, pre-filter to exclude zero denominators, or disable ANSI mode with `spark.conf.set("spark.sql.ansi.enabled", "false")`. The `when()` guard alone is not sufficient in Photon ANSI mode.

---

*Add new issues below this line using the same format.*
