# DEVELOPER.md — PharmaSignalLake

Developer notes, known environment issues, and resolutions encountered during development.
This file is a living log — add new entries as issues are found and resolved.

---

## Table of Contents

1. [Environment Notes](#1-environment-notes)
2. [VS Code + Databricks Sync Setup](#2-vs-code--databricks-sync-setup)
3. [Known Issues and Resolutions](#3-known-issues-and-resolutions)
   - [Issue 1 — DBFS Root Disabled on Free Edition (Unity Catalog)](#issue-1--dbfs-root-disabled-on-free-edition-unity-catalog)
   - [Issue 2 — databricks-connect Is Not Required for This Workflow](#issue-2--databricks-connect-is-not-required-for-this-workflow)
   - [Issue 3 — `%run` Magic Commands Must Use `# MAGIC` Prefix](#issue-3--run-magic-commands-must-use--magic-prefix)
   - [Issue 4 — FAERS Filenames Use 2-Digit Years; Config Was Generating 4-Digit](#issue-4--faers-filenames-use-2-digit-years-config-was-generating-4-digit)
   - [Issue 5 — `source_quarter` Dropped in `silver_drug_reactions` Intermediate Select](#issue-5--source_quarter-dropped-in-silver_drug_reactions-intermediate-select)
   - [Issue 6 — `Column.desc().nulls_last()` Is Not Valid PySpark Syntax](#issue-6--columndescnulls_last-is-not-valid-pyspark-syntax)
   - [Issue 7 — Photon ANSI Mode Throws DIVIDE_BY_ZERO Inside `when()` Guards](#issue-7--photon-ansi-mode-throws-divide_by_zero-inside-when-guards)
   - [Issue 8 — Notebook-Level `count` Assignment Shadows PySpark `count` Function](#issue-8--notebook-level-count-assignment-shadows-pyspark-count-function)
   - [Issue 9 — Deduplication on `primaryid` Was a No-Op](#issue-9--deduplication-on-primaryid-was-a-no-op)
   - [Issue 10 — New `sync.paths` Entries Didn't Populate Until the Remote Folder Existed](#issue-10--new-syncpaths-entries-didnt-populate-until-the-remote-folder-existed)
   - [Issue 11 — Case-Insensitive Column Name Collision Silently Corrupted PRR/ROR](#issue-11--case-insensitive-column-name-collision-silently-corrupted-prrror)
   - [Issue 12 — `%run` Mixed With Other Statements in a Cell Silently Falls Back to IPython `%run`](#issue-12--run-mixed-with-other-statements-in-a-cell-silently-falls-back-to-ipython-run)
   - [Issue 13 — BRD.md Still Documented the Pre-Fix Behavior as the Requirement](#issue-13--brdmd-still-documented-the-pre-fix-behavior-as-the-requirement)
   - [Issue 14 — silver_cases Fanned Out on OUTC/RPSR Despite a Comment Claiming Otherwise](#issue-14--silver_cases-fanned-out-on-outcrpsr-despite-a-comment-claiming-otherwise)
   - [Issue 15 — Notebook Attached to a SQL Warehouse Instead of Interactive/Serverless Compute](#issue-15--notebook-attached-to-a-sql-warehouse-instead-of-interactiveserverless-compute)

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

### Issue 8 — Notebook-Level `count` Assignment Shadows PySpark `count` Function

**Status:** Resolved  
**Date encountered:** June 2026  
**Affected files:** `notebooks/03_gold_signals.py`

#### Symptom

Signal detection stats section failed with:

```
TypeError: 'int' object is not callable
```

immediately after the Gold layer summary printed successfully.

#### Root Cause

The execution block used `count` as a local variable name to capture the return value of `write_gold()`:

```python
count = write_gold(df_signals, "signals")   # count = 373012 (int)
count = write_gold(df_demo, "demographics") # count = 34 (int)
count = write_gold(df_outc, "outcomes")     # count = 11 (int)
```

These assignments are at **notebook scope** (not inside a function), so they overwrite the `count` name imported from `pyspark.sql.functions`. When the summary section later called `count("*")` in `.agg()`, `count` was the integer `11`, not the aggregation function.

#### Resolution

Renamed the local variable from `count` to `n_rows` in all three execution block assignments.

#### Rule to Remember

In Databricks notebooks, all cell code runs in a shared global namespace. Assigning a variable at notebook level (outside a function) will shadow any imported name with the same identifier for all subsequent cells. Never use the same name as an imported function for a local result variable at notebook scope. Common collisions: `count`, `sum`, `max`, `min`, `round`, `filter`, `map`.

---

### Issue 9 — Deduplication on `primaryid` Was a No-Op

**Status:** Resolved
**Date encountered:** September 2026
**Affected files:** `notebooks/02_silver_clean.py`

#### Symptom

No runtime error. `silver_cases` always produced exactly one row per `primaryid` even though the code's intent — and its own docstring — was to deduplicate cases, keeping only the latest version. The bug was invisible in the 2026Q1 baseline because every `caseid` that quarter happened to have exactly one `caseversion` (confirmed via `05_profile.py`: distinct `primaryid` == distinct `caseid` == 397,224, 0 superseded). It would silently let stale, superseded case versions flow into Silver and Gold for any quarter where a reporter actually submitted a case update.

#### Root Cause

`primaryid = caseid + caseversion`, so every `primaryid` in Bronze is already unique — there can never be two rows sharing the same `primaryid`. The dedup window was:

```python
Window.partitionBy("primaryid").orderBy(col("caseversion").cast(IntegerType()).desc())
```

Partitioning by a column that is already unique per row means every partition has exactly one row, so `row_number() == 1` for every row — the filter `filter(col("rn") == 1)` never removes anything. The entity that actually needs deduplicating is the **case** (`caseid`), not the **case version record** (`primaryid`).

A second, related gap: child tables (`DRUG`, `REAC`, `OUTC`, `RPSR`, `THER`) were built independently from Bronze with no reference to which `primaryid`s belonged to a surviving case version, so even a correct DEMO dedup would not have prevented superseded-version rows from reaching `silver_drug_reactions` and `silver_therapy`.

#### Resolution

- Repartitioned the dedup window by `caseid`, ordering by `caseversion` (parsed via `try_cast(caseversion AS INT)` for ANSI safety) descending with nulls last, keeping one row per `caseid`.
- Added an explicit `left_semi` join of every child table (`DRUG`, `REAC`, `OUTC`, `RPSR`, `THER`) against the surviving `primaryid`s before any further processing. (`INDI` is not read anywhere in this notebook, so it needed no change.)
- Added before/after row-count prints for DEMO and every child table so a future run makes the effect of the fix visible.

#### Rule to Remember

Deduplicate on the actual entity key, not on a column that already encodes uniqueness by construction. If a composite key (like `primaryid = caseid + caseversion`) is used as the dedup partition column, the window will always be a no-op — check whether the column you're partitioning by could already be unique per row before trusting a `row_number() == 1` filter to remove anything. When a dedup fix has zero numeric impact on a sample dataset, that means the sample didn't happen to exercise the bug — it does not mean the bug wasn't real.

---

### Issue 10 — New `sync.paths` Entries Didn't Populate Until the Remote Folder Existed

**Status:** Resolved (workaround)
**Date encountered:** September 2026
**Affected files:** `databricks.yml`

#### Symptom

`src/` and `tests/` were added to `sync.paths` in `databricks.yml` (alongside the existing `notebooks`) while `Sync State: WATCHING_FOR_CHANGES` was already active. The two new folders did not appear under `/Workspace/Users/.../PharmaSignalLake/files/` on their own — only `notebooks/` was present, even though `src/` and `tests/` already existed locally with files in them.

#### Resolution

Manually created empty `src` and `tests` folders in the Databricks Workspace UI. Once they existed remotely, the already-running sync picked them up and populated them with the local files on the next sync cycle.

#### Root Cause

Not fully confirmed — the DAB/VS Code extension sync process for Workspace Files appears not to auto-create a *new* top-level `sync.paths` folder remotely while a sync session is already running; it will populate an existing remote folder, but won't necessarily create one from scratch mid-session purely from an edited `databricks.yml`. Stopping and restarting sync, or running `databricks bundle sync` from a terminal, might also have resolved this without the manual folder creation — not tested, since the manual creation was tried first and worked.

#### Rule to Remember

When adding a brand-new folder to `sync.paths` in `databricks.yml` while sync is already active, don't assume it will appear remotely on its own. If it doesn't show up under the Workspace file browser after saving `databricks.yml`, try restarting sync (stop/start in the Databricks panel) or `databricks bundle sync` first; manually creating the empty folder in the Workspace UI is a working fallback if those don't do it.

---

### Issue 11 — Case-Insensitive Column Name Collision Silently Corrupted PRR/ROR

**Status:** Resolved
**Date encountered:** September 2026
**Affected files:** `src/signal_math.py`

#### Symptom

`tests/test_signals.py` (run via `tests/run_unit_tests.py`) failed two of five tests, both with plain `AssertionError` and no exception from Spark:

- `test_hand_computed_2x2_table`: `row["drug_total"] == 8` failed (expected value from 8 real synthetic cases).
- `test_c_equals_a_triggers_haldane`: `row["prr"] == pytest.approx(3.5714, ...)` failed, even though the preceding `row["haldane_applied"] is True` and `row["prr"] is not None` assertions both passed — so PRR was a real, non-null number, just the wrong one.

#### Root Cause

`compute_signals()` built its contingency table by casting the raw aggregation columns (`"A"`, `"B"`, `"C"`) to `DoubleType` and *keeping those names*, then separately computing the 2x2 cells into new columns named `"a"`, `"b"`, `"c"`, `"d"` — differing from `"A"`/`"B"`/`"C"` only by case. Spark's default `spark.sql.caseSensitive = false` treats case-only-differing column names as ambiguous. Classic Spark usually raises `AnalysisException: Reference '...' is ambiguous` for this. Under Spark Connect — which is what Databricks Serverless compute uses (confirmed by the fixture's `spark = <pyspark.sql.connect.session.SparkSession ...>` in the test failure output) — `col("b")` and `col("B")` were instead silently resolved to whichever column the analyzer picked, without raising, producing a real but wrong number instead of an error.

#### Resolution

Renamed every intermediate column in `compute_signals()` to be mutually distinct even under case-insensitive comparison: the raw aggregation counts are now `pair_n`, `drug_n`, `reaction_n`, `total_n` (previously `A`/`B`/`C`/`N`), and the 2x2 cells are `cell_a`, `cell_b`, `cell_c`, `cell_d` (previously `a`/`b`/`c`/`d`), with Haldane-adjusted versions `cell_a_h`.. `cell_d_h`. No two columns in the DataFrame now share a name under case-insensitive comparison.

#### Rule to Remember

Never name two columns in the same DataFrame such that they differ only by case (e.g. `"A"` and `"a"`) — Spark's default case-insensitive analyzer makes this genuinely ambiguous, and depending on the execution engine (this project saw it specifically under Spark Connect / Databricks Serverless) a `col()` reference to one of them can silently resolve to the wrong one instead of raising an error. This is worse than a crash: the pipeline keeps running and produces plausible-looking wrong numbers. Catch it with unit tests against hand-computed values, not by trusting that Spark will complain.

---

### Issue 12 — `%run` Mixed With Other Statements in a Cell Silently Falls Back to IPython `%run`

**Status:** Resolved
**Date encountered:** September 2026
**Affected files:** `notebooks/99_run_pipeline.py`

#### Symptom

Running `99_run_pipeline.py` ("Run All") completed with no errors and printed `✓ Silver complete`, `✓ Gold complete`, etc. for every stage. But Stage 2's output showed nothing from `02_silver_clean.py` itself — none of its `[SILVER_CASES]`/`[DEDUP]`/row-count output — only this warning immediately before the hollow "✓ Silver complete" line:

```
Warning you are using the ipython `%run` line magic. To use the databricks
`%run` cell magic make sure that the magic is at the very start of the cell.
```

The pipeline reported success at every stage while (almost certainly) not actually running Bronze, Silver, Gold, or Export.

#### Root Cause

Commit 1b (fixing Issue 3 — the missing `# MAGIC` prefix) added the prefix to each `%run` line, but each one was still sitting in the *middle* of a cell alongside other Python statements (`stage_start = datetime.now()`, `print(...)`), e.g.:

```python
stage_start = datetime.now()
print("\n[STAGE 2] Silver cleansing and integration...")

# MAGIC %run ./02_silver_clean

print(f"  ✓ Silver complete ({(datetime.now()-stage_start).seconds}s)")
```

Databricks requires `%run` to be the **only** content of its cell. When it isn't, Databricks doesn't error — it silently falls back to IPython's own `%run` line magic, which is a different, unsupported mechanism for this project (it doesn't chain Databricks notebook state the way the native `%run` cell magic does). The fallback produced the warning, did not reliably execute the target notebook's logic, and the unconditional `print(f"  ✓ ... complete")` line after it ran regardless — so every stage reported success whether or not it actually ran.

This is a second, more subtle bug than Issue 3: Issue 3 was about the `# MAGIC` prefix being entirely missing; this one is about `%run`'s cell-isolation requirement, which the prefix fix alone doesn't satisfy.

#### Resolution

Restructured every stage in `99_run_pipeline.py` so `# MAGIC %run ./0N_...` is alone in its own `# COMMAND ----------` cell, with the `stage_start`/print statements split into separate cells before and after it.

#### Rule to Remember

`%run` (as `# MAGIC %run`) must be the **only** statement in its cell — not just correctly prefixed with `# MAGIC`. Putting any other Python code (even a comment-adjacent `print()`) in the same cell causes a silent fallback to IPython's `%run` line magic instead of a hard error, which is easy to miss because the surrounding code still runs and prints success. When orchestrating multiple notebooks with `%run`, always give each one its own dedicated cell, and don't trust a "✓ complete" print that sits in the same cell as (or right after, but not gated on) the `%run` call — check for the target notebook's own output, not just the orchestrator's banner.

---

### Issue 13 — BRD.md Still Documented the Pre-Fix Behavior as the Requirement

**Status:** Resolved
**Date encountered:** September 2026
**Affected files:** `BRD.md`

#### Symptom

No runtime error — a documentation drift, surfaced only by asking "does this branch's work align with the BRD?" `BRD.md` v2.0 still specified: DEMO deduplication on `primaryid` (FR-06); Evans Criteria as `case_count >= 3 AND PRR >= 2` with no chi-squared term (FR-16, Section 5 Scope, Glossary); and three-tier signal classification — signal, weak signal, or no signal (Section 5 Scope). Every one of these is the exact pre-fix behavior that D4, D3, and D1 respectively identified as wrong and corrected. FR-14/FR-15 (the PRR/ROR formulas) were also silently incomplete — they never mentioned the Haldane-Anscombe correction (D2), even though it changes both formulas whenever a contingency-table cell is zero.

#### Root Cause

`DECISIONS.md` was updated as each correctness decision was made (D1-D4), but `BRD.md` — the project's canonical requirements document — was never revisited to match. The two documents drifted apart: `DECISIONS.md` correctly recorded the old design as rejected, while `BRD.md` kept describing that same rejected design as the formal requirement. Nothing in the working process forced a cross-check between "a decision changed behavior" and "the requirements doc that specified the old behavior."

#### Resolution

Updated `BRD.md` to v2.1: FR-06 now specifies dedup on `caseid` with child tables filtered to the surviving `primaryid`s (D4); FR-14/FR-15 now document the Haldane-Anscombe correction (D2); FR-16 now requires chi-squared (Yates) ≥ 4 in addition to case count and PRR (D3); Section 5 Scope and the Evans Criteria glossary entry now describe binary `SIGNAL`/`NO_SIGNAL` classification (D1); added a Haldane-Anscombe Correction glossary entry. Cross-referenced from `DECISIONS.md` D1-D4.

#### Rule to Remember

When a `DECISIONS.md` entry changes behavior that a formal requirements document already specifies, update that requirements document in the same pass — don't treat the decisions log alone as sufficient. A requirements doc that silently disagrees with the decisions log is worse than having no requirements doc at all, because it still looks authoritative. Periodically ask "does this match the BRD?" as its own explicit check, not just "does this match the decisions log?" — the two can drift apart from each other even when each is internally consistent.

---

### Issue 14 — `silver_cases` Fanned Out on OUTC/RPSR Despite a Comment Claiming Otherwise

**Status:** Resolved
**Date encountered:** September 2026
**Affected files:** `notebooks/02_silver_clean.py`, `notebooks/03_gold_signals.py`

#### Symptom

`06_validate_fix.py`'s `V1` check failed: `silver_cases` had 470,891 rows for only 397,224 distinct cases (`rows=470,891 distinct_caseid=397,224 distinct_primaryid=397,224`). This was already a known number from `05_profile.py`'s baseline ("fan-out 73,667 from OUTC x RPSR"), and on 2026-09-28 the team had explicitly decided to accept it rather than fix it, scoping `V1` to check dedup-level uniqueness instead. Re-examining it turned up something that decision hadn't accounted for: `build_silver_cases()`'s own comment already said *"We aggregate to a comma-separated list to avoid fan-out on the join"* — but the code directly below it never did that aggregation.

#### Root Cause

`build_silver_cases()` joined DEMO with OUTC and RPSR after only `dropDuplicates(["primaryid", "outc_cod"])` / `dropDuplicates(["primaryid", "rpsr_cod"])` — which removes exact duplicate rows but does nothing about a case legitimately having multiple *distinct* outcome or report-source codes. A case with 2 outcome codes and 3 report-source codes produced a 2×3=6-row cartesian fan-out from the two plain `left` joins, instead of the single aggregated row the code's own comment described.

#### Resolution

- `02_silver_clean.py`: aggregate `outc_cod` and `rpsr_cod` per `primaryid` into a comma-separated list (`collect_set` + `concat_ws`) before the join, so `silver_cases` is one row per case.
- `03_gold_signals.py`: `build_gold_outcomes()` now `explode(split(col, ","))`s each list back into one row per (case, code) before its existing per-code `groupBy`/`countDistinct`, so the `gold_outcomes` output — and the Power BI Case Outcomes page — is numerically unchanged.
- Recorded as `DECISIONS.md` D7, which explicitly supersedes the 2026-09-28 "keep the grain" decision.
- `docs/data_dictionary.md` updated: `silver_cases`' grain and `outc_cod`/`rpsr_cod` descriptions now reflect one row per case with multi-valued code columns.

#### Rule to Remember

A comment describing what the code does is a claim, not a guarantee — when a table's row count doesn't match its expected grain, check whether the surrounding comment already promised the fix that's missing, before assuming the mismatch is an accepted design tradeoff. Also: when an earlier decision is reversed, don't just fix the code — record the reversal itself (which decision, why it no longer holds) so the decisions log stays a true history instead of silently going stale the way the original BRD did (Issue 13).

---

### Issue 15 — Notebook Attached to a SQL Warehouse Instead of Interactive/Serverless Compute

**Status:** Resolved (environment/workflow, not code)
**Date encountered:** September 2026
**Affected files:** None — this is a per-notebook compute attachment setting in the Databricks UI, not a file in the repo

#### Symptom

Opening `02_silver_clean.py` in Databricks showed a banner instead of running:

```
Non-SQL cells will fail during execution
This notebook is attached to a SQL warehouse, which only supports executing SQL
cells and rendering Markdown cells. Cells using other languages will fail during
execution. If you would like to execute all cells, please attach to an
interactive cluster.
```

Every `%run`, `import`, and PySpark cell in the notebook would fail if run in this state.

#### Root Cause

The notebook's attached compute was a **SQL Warehouse** rather than an interactive/serverless cluster. SQL Warehouses only execute SQL and Markdown cells — they exist for the SQL Editor / DBSQL dashboards / ad hoc SQL (e.g. running `sql/verify_signal_fix.sql`), not for PySpark notebooks. Databricks appears to default a newly opened notebook to whatever compute was last used in the workspace, so switching between the SQL Editor and a notebook can leave the notebook pointed at the wrong compute type without an explicit choice being made.

#### Resolution

In the notebook's compute selector (top of the notebook), switch from the SQL Warehouse to the Serverless compute (or an interactive cluster, if available), then re-run from the top.

#### Rule to Remember

Before running any PySpark notebook in this project, check what it's attached to — a SQL Warehouse and an interactive/serverless cluster are different compute types in Databricks, and attaching to the wrong one produces this specific banner rather than a Python error. If a session has recently used the SQL Editor (e.g. for `sql/verify_signal_fix.sql`, D6), check the compute selector on the next PySpark notebook opened afterward before assuming a run failure is a code bug.

---

*Add new issues below this line using the same format.*
