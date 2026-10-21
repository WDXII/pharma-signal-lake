# DEVELOPER.md — PharmaSignalLake

Developer notes: environment, local setup, and a log of problems found and fixed. Add new issues at the end of Section 3 in the same format.

> Condensed once on 2026-10-06 after reader feedback. Issue numbers, findings and rules are unchanged; long code dumps and repeated explanations were removed. The pre-trim text is in this file's git history. Append-only again from here.

## 1. Environment

| Item | Detail |
|---|---|
| Platform | Databricks Free Edition (AWS), Unity Catalog enforced |
| Compute | Serverless (no persistent cluster). Runtime version is not recorded in the run logs (PROJECT-LOG T6) |
| Storage | Unity Catalog Managed Volume for raw files and Delta tables (Issue 1) |
| Power BI | Desktop, local CSV import (no live connector) |

## 2. VS Code + Databricks Sync

Edit notebooks locally and sync them to Databricks. No local Python environment is needed (Issue 2).

1. **Step 1 — Create a PAT:** Databricks → profile icon → Settings → Developer → Access tokens → Manage → Generate new token. Use a generic name (`vscode-databricks`), 90-day expiry, copy it immediately (shown once), store it in a password manager. Never commit it.
2. **Step 2 — Install** the official **Databricks** extension in VS Code.
3. **Step 3 — Connect:** Databricks icon → Configure Databricks → workspace URL (`https://dbc-xxxxxxxx.cloud.databricks.com`, no trailing slash) → Personal Access Token → paste.
4. **Step 4 — Sync:** `databricks.yml` (Databricks Asset Bundle) controls what syncs. `sync.paths` lists `notebooks`, `src`, `tests`, `sql`. DAB appends `/files` to `root_path`, so notebooks land at `<root_path>/files/notebooks/`. Start sync with **Sync State: STOPPED** in the panel (it does not auto-start), or run `databricks bundle sync`.
5. **Step 5 — Run notebooks in the Databricks web UI**, then commit and push from VS Code.

| Problem | Fix |
|---|---|
| "Connection failed" | Remove trailing slash from the URL; re-paste the PAT |
| PAT rejected | Expired; generate a new one |
| Notebook runs old code | Sync is STOPPED; start it first |
| Notebooks land at `/files/notebooks/files` | `root_path` must not end in `/notebooks` |

## 3. Known Issues and Resolutions

| # | Issue | Status |
|---|---|---|
| 1 | DBFS root disabled on Free Edition | Resolved |
| 2 | `databricks-connect` not needed | Resolved |
| 3 | `%run` needs the `# MAGIC` prefix | Resolved |
| 4 | FAERS filenames use 2-digit years | Resolved |
| 5 | `source_quarter` dropped in an intermediate select | Resolved |
| 6 | `.desc().nulls_last()` is not valid PySpark | Resolved |
| 7 | ANSI mode `DIVIDE_BY_ZERO` (fix superseded by D5) | Superseded |
| 8 | `count` variable shadowed the PySpark function | Resolved |
| 9 | Dedup on `primaryid` was a no-op | Resolved |
| 10 | New `sync.paths` folders did not appear | Workaround |
| 11 | Case-only column name collision corrupted PRR/ROR | Resolved |
| 12 | `%run` sharing a cell silently no-ops | Resolved |
| 13 | BRD still documented pre-fix behavior | Resolved |
| 14 | `silver_cases` fan-out despite a comment saying otherwise | Resolved |
| 15 | Notebook attached to a SQL Warehouse | Resolved |
| 16 | Validation query 2.1 could never fail | Resolved |
| 17 | Zero-cell nulls hid exclusive signals | Resolved |
| 18 | Baseline not snapshotted; time travel blocked | Workaround |
| 19 | `sql/` missing from `sync.paths` | Resolved |
| 20 | Unit-test runner failed on `__pycache__` writes | Resolved |
| 21 | `case_count` counts reports, not patients | Documented limitation |
| 22 | `haldane_applied` is not "exclusive" | Resolved |
| 23 | Docs claimed more than was verified | Docs corrected |
| 24 | Stage timers used `timedelta.seconds` | Resolved |
| 25 | Run summary could disagree with its own run | Resolved |

---

### Issue 1 — DBFS Root Disabled on Free Edition
*Resolved · June 2026 · `00_config.py`, `ARCHITECTURE.md`*
- **Symptom:** Any `/FileStore/` or `dbfs:/FileStore/` path raised `DBFS_DISABLED` (SQLSTATE 56038).
- **Cause:** Free Edition enables Unity Catalog, which blocks DBFS root. The original config assumed DBFS.
- **Fix:** Created Managed Volume `workspace.default.pharma_raw`, uploaded the 7 FAERS files to `/Volumes/workspace/default/pharma_raw/2026Q1/`, and pointed the five base paths in `00_config.py` (`BASE_RAW`, `BASE_BRONZE`, `BASE_SILVER`, `BASE_GOLD`, `BASE_EXPORT`) at the Volume. Derived paths needed no change.
- **Recreate:** Catalog → `workspace` → `default` → Create → Volume (`pharma_raw`, Managed) → Add Data → Upload to Volume at `.../2026Q1/`. For a new quarter, use a new subfolder and set `QUARTER` (second-quarter run untested, Issue 23).
- **Rule:** On Free Edition, use Unity Catalog Volumes, never DBFS paths.

### Issue 2 — `databricks-connect` Is Not Required
*Resolved by not installing it · June 2026*
- **Symptom:** VS Code warns "Activate an environment with Python 3.12 or greater" and can't install `databricks-connect` (Python 3.13 has no `numpy<2` wheel).
- **Cause:** It is only for running Spark locally against a remote cluster, which this project does not do.
- **Fix:** Ignore the warnings. Edit locally, sync, run in the web UI. No venv. (Optional later: Python 3.12 + `databricks-connect==15.4.*`.)
- **Rule:** No local Python environment is needed. There is also no Java locally, so no local Spark (Section 4).

### Issue 3 — `%run` Needs the `# MAGIC` Prefix
*Resolved · June 2026 · notebooks 01–04, 99*
- **Symptom:** `NameError: name 'QUARTER' is not defined` after `00_config.py` ran fine.
- **Cause:** `# %run ./00_config` is just a comment, so the config never loaded.
- **Fix:** Changed to `# MAGIC %run ./00_config` in all five notebooks.
- **Rule:** In Databricks `.py` source, every magic command (`%run`, `%sql`, `%md`, `%pip`) needs the `# MAGIC ` prefix.

### Issue 4 — FAERS Filenames Use 2-Digit Years
*Resolved · June 2026 · `00_config.py`*
- **Symptom:** `PATH_NOT_FOUND` for `DEMO2026Q1.txt` and the other six files.
- **Cause:** FDA names files `DEMO26Q1.txt`; config built names from `QUARTER` directly.
- **Fix:** `QUARTER_SHORT = QUARTER[2:]`, used in all `RAW_FILES` paths.
- **Note:** `dbutils.fs.ls()` shows Volume paths as `dbfs:/Volumes/...`. That is display only, not DBFS root access.
- **Rule:** FAERS file names always use the 2-digit year (`2024Q3` → `24Q3`).

### Issue 5 — `source_quarter` Dropped in `silver_drug_reactions`
*Resolved · June 2026 · `02_silver_clean.py`*
- **Symptom:** `UNRESOLVED_COLUMN` for `source_quarter`; only `silver_drug_reactions` failed.
- **Cause:** The intermediate `.select()` on `drug` omitted it.
- **Fix:** Added `"source_quarter"` to that select.
- **Rule:** In `.select()` → `.join()` → `.select()` chains, audit columns (`source_quarter`, `load_timestamp`) must be carried through every projection.

### Issue 6 — `.desc().nulls_last()` Is Not Valid PySpark
*Resolved · June 2026 · `03_gold_signals.py`*
- **Symptom:** `'Column' object is not callable` in `gold_signals`.
- **Cause:** `nulls_last` is not a `Column` method.
- **Fix:** `col("prr").desc_nulls_last()`.
- **Rule:** Null ordering uses combined methods: `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`.

### Issue 7 — ANSI Mode `DIVIDE_BY_ZERO` (Superseded)
*Resolved, then superseded by D5 · June 2026 · `03_gold_signals.py`*
- **Symptom:** `DIVIDE_BY_ZERO` (SQLSTATE 22012) during the Delta write.
- **Cause (as diagnosed then):** Serverless/Photon enforces ANSI, and the division ran despite the `when()` guard. Later found: a ROR guard really did let `C − A = 0` through (D5).
- **Fix then:** Disabled ANSI. **Superseded (Sept 2026):** ANSI is back on and Haldane (D2) removes zero denominators. Do not repeat the original fix.
- **Rule:** Never disable ANSI. It turned a data-shape problem into null PRR for the most exclusive pairs (Issue 17). Fix the math or use `try_divide` and handle the null.

### Issue 8 — `count` Variable Shadowed the PySpark Function
*Resolved · June 2026 · `03_gold_signals.py`*
- **Symptom:** `TypeError: 'int' object is not callable` right after the Gold summary printed.
- **Cause:** `count = write_gold(...)` at notebook scope overwrote `pyspark.sql.functions.count`.
- **Fix:** Renamed to `n_rows`.
- **Rule:** Notebook cells share one namespace. Never name a variable `count`, `sum`, `max`, `min`, `round`, `filter` or `map`.

### Issue 9 — Dedup on `primaryid` Was a No-Op
*Resolved · Sept 2026 · `02_silver_clean.py`, `src/signal_math.py`*
- **Symptom:** No error. `silver_cases` always had one row per `primaryid`. 2026Q1 hid it (397,224 = 397,224, 0 superseded).
- **Cause:** `primaryid = caseid + caseversion` is unique, so `Window.partitionBy("primaryid")` gives one row per partition and the `rn == 1` filter removes nothing. Child tables also ignored which `primaryid`s survived.
- **Fix:** Partition by `caseid`, order by `try_cast(caseversion AS INT)` descending, nulls last (now `dedup_cases()` in `src/signal_math.py`, called from `02_silver_clean.py`). `left_semi` join `DRUG`, `REAC`, `OUTC`, `RPSR`, `THER` to surviving `primaryid`s (`INDI` is unused there). Added before/after row-count prints.
- **Rule:** Dedup on the entity key. Zero impact on a sample means the sample did not exercise the bug, not that the bug was unreal.

### Issue 10 — New `sync.paths` Folders Did Not Appear
*Workaround · Sept 2026 · `databricks.yml`*
- **Symptom:** After adding `src/` and `tests/` to `sync.paths` during a running sync, only `notebooks/` appeared remotely.
- **Cause:** Not confirmed. Sync likely won't create a new top-level remote folder mid-session.
- **Fix:** Created empty `src` and `tests` folders in the Workspace UI; the next sync filled them. Restarting sync or `databricks bundle sync` might also work (untested).
- **Rule:** After adding a folder, restart sync or run `databricks bundle sync`. Creating the empty folder manually is the fallback.

### Issue 11 — Case-Only Column Name Collision Corrupted PRR/ROR
*Resolved · Sept 2026 · `src/signal_math.py`*
- **Symptom:** 2 of 5 unit tests failed with plain `AssertionError`: `drug_total` was not 8, and `prr` was non-null but not 3.5714.
- **Cause:** Columns `A`/`B`/`C` and `a`/`b`/`c`/`d` differ only by case. Classic Spark raises an ambiguity error; under Spark Connect (Serverless) `col("b")` silently resolved to the wrong one.
- **Fix:** Renamed to `pair_n`, `drug_n`, `reaction_n`, `total_n`, `cell_a`…`cell_d`, `cell_a_h`…`cell_d_h`.
- **Rule:** Never name two columns that differ only by case. This fails silently with plausible wrong numbers. Catch it with unit tests against hand-computed values.

### Issue 12 — `%run` Sharing a Cell Silently No-Ops
*Resolved · Sept 2026 · `99_run_pipeline.py`*
- **Symptom:** Run All printed `✓ Silver complete` etc. for every stage, but none of `02_silver_clean.py`'s own output appeared, only the warning "you are using the ipython `%run` line magic".
- **Cause:** After Issue 3, each `# MAGIC %run` still shared a cell with other statements. Databricks then falls back to IPython's `%run` without erroring, and the unconditional success `print` ran regardless.
- **Fix:** Each `# MAGIC %run ./0N_...` now sits alone in its own `# COMMAND ----------` cell; `stage_start` and `print` are in separate cells.
- **Rule:** `%run` must be the only statement in its cell. Don't trust a "✓ complete" banner; look for the target notebook's own output.

### Issue 13 — BRD Still Documented Pre-Fix Behavior
*Resolved · Sept 2026 · `BRD.md`*
- **Symptom:** BRD v2.0 still specified `primaryid` dedup (FR-06), Evans without χ² (FR-16, Scope, Glossary), a three-tier classification (Scope), and PRR/ROR formulas with no Haldane correction (FR-14/15).
- **Cause:** DECISIONS was updated per decision; the BRD was never revisited.
- **Fix:** BRD v2.1: FR-06, FR-14, FR-15, FR-16, Scope, Glossary updated (plus a Haldane-Anscombe entry), cross-referenced from D1–D4.
- **Rule:** Update the requirements doc in the same pass as the decision. Periodically ask "does this match the BRD?" as its own check.

### Issue 14 — `silver_cases` Fan-Out Despite a Comment Saying Otherwise
*Resolved · Sept 2026 · `02_silver_clean.py`, `03_gold_signals.py`*
- **Symptom:** `V1` failed: 470,891 rows for 397,224 distinct cases. The 73,667-row fan-out was known from the `05_profile.py` baseline and had been accepted on 09-28, yet the function's own comment said it aggregated.
- **Cause:** Only `dropDuplicates(["primaryid", "outc_cod"])` and the `rpsr_cod` equivalent preceded the joins. A case with 2 outcome and 3 report-source codes produced 6 rows.
- **Fix:** `02`: aggregate `outc_cod` and `rpsr_cod` per `primaryid` (`collect_set` + `concat_ws`). `03`: `build_gold_outcomes()` explodes them back, so `gold_outcomes` and Power BI are unchanged. Recorded as D7 (supersedes the 09-28 call); `docs/data_dictionary.md` updated.
- **Rule:** A comment is a claim, not a guarantee. When a row count misses its grain, check whether a comment already promised the missing fix. When reversing a decision, record the reversal.

### Issue 15 — Notebook Attached to a SQL Warehouse
*Resolved (workflow) · Sept 2026 · no repo files*
- **Symptom:** Banner "Non-SQL cells will fail during execution" instead of a run.
- **Cause:** The notebook was attached to a SQL Warehouse (SQL and Markdown only), likely inherited from the last-used compute after the SQL Editor.
- **Fix:** Switch the compute selector to Serverless or an interactive cluster and re-run.
- **Rule:** Check the compute attachment before blaming code, especially right after using the SQL Editor.

### Issue 16 — Validation Query 2.1 Could Never Fail
*Resolved · Sept 2026 · `notebooks/validation_queries.sql`*
- **Symptom:** Query 2.1 ("no duplicate `primaryid`") returned 0 on every run, even with superseded versions (Issue 9) and fan-out (Issue 14). Section 0 also used disabled DBFS paths.
- **Cause:** `primaryid` is unique by construction, so the check tested something that cannot be violated.
- **Fix:** 2.1 compares `COUNT(*)` with `COUNT(DISTINCT caseid)` and states in a comment what would make it fail. Section 0 uses `/Volumes/workspace/default/pharma_raw/{bronze,silver,gold}/`. Queries 3.3/3.4 changed from `signal_flag != 'NO_SIGNAL'` to `= 'SIGNAL'` (D1). Header points to `sql/verify_signal_fix.sql` (Will-owned, D6).
- **Rule:** For every check, name the broken output that would make it fail. If you can't, it is decoration. Check uniqueness on `caseid`.

### Issue 17 — Zero-Cell Nulls Hid Exclusive Signals
*Resolved (D2) · Sept 2026 · `03_gold_signals.py`, `src/signal_math.py`*
- **Symptom:** Pairs with `case_count = event_total` (`C − A = 0`) had null PRR and fell to `NO_SIGNAL`. Baseline: 2,978 null-PRR pairs, 106 with `case_count ≥ 3` (query E).
- **Cause:** Zero denominators made the guards return null; null fails every `>=`, so the default `NO_SIGNAL` won. Disabling ANSI (Issue 7) had hidden it.
- **Fix:** Haldane correction for PRR/ROR plus a `haldane_applied` column; ANSI on (D2, D5). Verified 2026-09-29: null PRR 2,978 → 0, the 106 pairs now `SIGNAL`.
- **Rule:** A null metric must never fall silently into a default class. Give every branch an explicit null outcome and count nulls per class.

### Issue 18 — Baseline Not Snapshotted; Time Travel Blocked
*Workaround · 2026-09-29 · Delta table properties, no repo files*
- **Symptom:** `VERSION AS OF 1` failed with `DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION`.
- **Cause:** The baseline was never copied out before the re-run (T3). Default `delta.deletedFileRetentionDuration` is 168 hours; v1 was months old.
- **Fix:** Temporarily raised `delta.deletedFileRetentionDuration` and `delta.logRetentionDuration` to 180 days, restored v1 to `_baseline` paths, then UNSET both. The widen (v4, 08:20 UTC) and reset (v5, 09:35 UTC) hold data identical to v3. This only worked because the old files were not yet physically removed. Cells are in `docs/validation-results.md` Appendix A.
- **Rule:** Time travel is not a backup. Snapshot the baseline to its own table or path, and record the Delta version, before any comparison re-run.

### Issue 19 — `sql/` Missing from `sync.paths`
*Resolved · 2026-09-29 · `databricks.yml`*
- **Symptom:** `sql/verify_signal_fix.sql` existed locally and in git but not in the workspace.
- **Cause:** `sync.paths` listed only `notebooks`, `src`, `tests`.
- **Fix:** Added `sql`. If it doesn't appear, see Issue 10.
- **Rule:** Add any new top-level folder to `sync.paths` in the same commit, then confirm it appears in the Workspace browser.

### Issue 20 — Unit-Test Runner Failed on `__pycache__` Writes
*Resolved · 2026-09-29 · `tests/run_unit_tests.py`*
- **Symptom:** On Serverless, `ImportError while loading conftest`, `OSError: [Errno 95] ... tests/__pycache__`, then pytest exit code 4. No test ran.
- **Cause:** Workspace files don't allow bytecode or pytest cache writes.
- **Fix:** Before the run: `sys.dont_write_bytecode = True`, then `pytest.main(["-v", "-p", "no:cacheprovider", TESTS_DIR])`.
- **Rule:** Exit code 4 means the tests never ran. Never report a test count without seeing the "N passed" line.

### Issue 21 — `case_count` Counts Reports, Not Patients
*Documented limitation (D9), no pipeline change · 2026-09-29/30*
- **Symptom:** The case review (P1/P2) of the top 10 rescued signals found many reports per pair sharing a fingerprint, literature reference or sender. Simvastatin / Glycogen Storage Disease Type IX: 5 reports, 5 senders, 4 citing the same article. Cariprazine: 3 reports with the same event and FDA dates. Zolgensma: 5 reports that read as about 2 patients. 3 of the 10 rest on fewer than three likely patients.
- **Cause:** `case_count` is `countDistinct(primaryid)`. D4 only catches updated versions of one case, not the same literature case sent by several manufacturers, related reports, or follow-ups under new case IDs. Exclusive pairs top any PRR/χ² ranking, so inflation shows there first.
- **Fix:** None in the pipeline. Documented in README and `docs/validation-results.md` §7; P1–P3 is the manual check.
- **Rule:** Never read a report count as a patient count. P1's fingerprint uses `concat_ws`, which skips NULLs, so missing fields collapse together; review rows (P2) before calling anything a duplicate.

### Issue 22 — `haldane_applied` Is Not "Exclusive"
*Resolved (D8 amended 2026-09-30) · `DECISIONS.md`, `BRD.md` FR-21, `README.md`, `sql/case_review_rescued_signals.sql`*
- **Symptom:** D8 first filtered the "Exclusive reports" panel on `haldane_applied = true`, showing 159 `SIGNAL` pairs instead of 106.
- **Cause:** `haldane_applied` is true when any cell is 0. The 53 `SIGNAL` pairs with `b = 0` (every report for the drug has this event) are Haldane-adjusted but not exclusive. Cross-tab in D8; query saved as P4 and re-run 2026-10-06 with the same six rows.
- **Fix:** Panel filter is `is_exclusive` (106 pairs). Main leaderboard is `SIGNAL` and not `is_exclusive` (49,878), with `haldane_applied` as a tooltip. Power BI derives it; no pipeline change.
- **Rule:** Name a filter after the property it tests. Cross-tabulate a flag against the concept it represents before building a visual on it.

### Issue 23 — Docs Claimed More Than Was Verified
*Docs corrected (BRD v2.4); second-quarter behavior untested · 2026-10-05*
- **Symptom:** BRD v2.3 said extensibility was "verified with a second quarter" and would "scale to any number of quarters without code changes". README and ARCHITECTURE said a new quarter "is appended as a new partition; existing quarters are untouched". The BRD promised Bronze schema validation, which is not in code (PROJECT-LOG I4).
- **Cause:** Design intent was never re-checked against a recorded run. `01`–`03` write with `mode("overwrite")` partitioned by `source_quarter` and no `replaceWhere`, which does not by itself preserve other quarters.
- **Fix:** BRD v2.4: "designed for; verified on 2026Q1 only", schema validation "planned, not yet implemented (I4)". README, ARCHITECTURE, SETUP: "Verified on 2026Q1 only"; append claim removed. Second-quarter run and write-mode check deferred to after release (T9).
- **Rule:** A doc may state only what a recorded run showed. Write intent as "designed to" and name the verifying run.

### Issue 24 — Stage Timers Used `timedelta.seconds`
*Resolved · 2026-10-05, closed by Issue 25 · `99_run_pipeline.py`*
- **Symptom:** Stage timings printed `(datetime.now()-stage_start).seconds`. Correct on the 2026Q1 run, but wrong in principle.
- **Cause:** `.seconds` is only the seconds component: it drops fractions and wraps after 24 hours.
- **Fix:** Stage timers use `.total_seconds():.0f`. The remaining total-duration `.seconds` line was removed by Issue 25. Verified in a Databricks run on 2026-10-06 (five stages OK, total 2m 39.2s, UTC; OK path only).
- **Rule:** For elapsed time use `total_seconds()`, never `.seconds`.

### Issue 25 — Run Summary Could Disagree With Its Own Run
*Resolved · 2026-10-06 (verified on Databricks, OK path only) · `99_run_pipeline.py`*
- **Symptom:** After a partial rerun the summary showed a finish time before the start. Times were naive local time with no zone, and there was no per-stage breakdown.
- **Cause:** `pipeline_start` and `pipeline_end` lived in different cells, so a partial rerun mixed executions; durations were wall-clock differences. This cause is inferred from the notebook structure; the partial rerun was not reproduced, so read it as likely, not confirmed.
- **Fix:** An in-memory `stage_log` (D10). The first cell resets `stage_log`, `pipeline_start` and `pipeline_t0` together. `stage_begin(name)` and `stage_end(status)` live in their own normal cell, never in a `%run` cell (Issue 12). Entries hold `stage`, `start_utc`, `end_utc`, `duration_s`, `status` (UTC timestamps, `perf_counter` durations). The summary prints a stage table plus "Times are UTC", and warns (not fails) on inconsistent times. Nothing is persisted.
- **Known limit:** `%run` cannot be wrapped in `try/except`, so a failed stage is logged `FAILED` only when `stage_begin` or the summary next runs, with that later time as its end. The notebook's own error output shows the real failure time.
- **Rule:** Build a summary only from data recorded in that same execution, reset in the cell that records the start. Use UTC timestamps and a monotonic clock for durations.

---

*Add new issues below this line using the same format.*

### Issue 26 — Power BI Card Abbreviated the Case Count to "397K"
*Logged · 2026-10-13 · Power BI Desktop, measure `Cases` (not a pipeline change)*
- **Symptom:** The Cases card on the Signal Leaderboard page showed an abbreviated value with a "K" suffix instead of 397,224.
- **Cause:** A card visual applies automatic display units to a numeric measure, so large values are abbreviated.
- **Fix:** `Cases = FORMAT( MAX ( gold_signals[total_reports] ), "#,0")`. `FORMAT` returns text, so the card shows the full number with a thousands separator. Setting the card's Display units to None is the alternative that keeps the measure numeric.
- **Rule:** Use a `FORMAT` measure only for a display-only card, and never in a calculation. Check every card against the verified count (397,224) before taking a screenshot.
- **Addendum (2026-10-21):** The numeric cards need the Display units alternative named above, set per field; the exact steps are in Issue 34. The verified count for the `Cases` card is now 397,209 (N, cases with a primary-suspect drug), not 397,224; see the build steps, section 4.
- **Addendum (2026-10-21, later):** The `FORMAT` fix above is superseded. `Cases` is now numeric: `Cases = MAX ( gold_signals[total_reports] )`, format Whole number, with the card's Display units set per field (Custom, `#,0`; Issue 34). The card reads 397,209 in full, so no `FORMAT` is needed. The Rule above now applies only to text label measures; see DECISIONS D14.

### Issue 29 — Power BI Error: "Cannot convert value '' of type Text to type Number" on `Haldane %`
*Logged · 2026-10-20 · Power BI Desktop, measure `Haldane %` (not a pipeline change)*
- **Symptom:** A visual using `[Haldane %]` failed with `Cannot convert value '' of type Text to type Number`.
- **Cause:** The measure was written as `FORMAT( DIVIDE ( [Haldane Pairs], [Total Pairs] ), "0.00%")`. `FORMAT` returns text, and a blank `DIVIDE` result becomes `''`, which a visual expecting a number cannot convert. The build steps define it without `FORMAT`.
- **Fix:** `Haldane % = DIVIDE ( [Haldane Pairs], [Total Pairs] )`, then Measure tools > Format > Percentage, 2 decimals.
- **Rule:** A measure that feeds a visual returns a number; format it in the model, not with `FORMAT`. Use `FORMAT` only in a display-only measure (Issue 26) or a separate label measure.

### Issue 30 — Clicking a Table Row Filtered the KPI Cards
*Logged · 2026-10-20 · Power BI Desktop, Signal Leaderboard page*
- **Symptom:** Clicking a drug row in the leaderboard changed the six KPI cards from their verified whole-quarter values.
- **Cause:** Power BI cross-filters every visual on a page by default. The build steps make the Drug Name slicer the only drug filter and say nothing about row clicks driving the cards.
- **Fix:** Edit interactions: set the six cards to None from both tables and from the Drug Name slicer. Added to the build steps.
- **Rule:** KPI cards show fixed totals. Turn off cross-filtering to them, then click a row and confirm 397,209 / 373,012 / 49,984 / 49,878 / 106 / 7,197.

### Issue 31 — Top 10 on the Leaderboard Needs a Per-Row Key
*Logged · 2026-10-20 · Power BI Desktop, Main leaderboard table*
- **Symptom:** There was no way to filter the leaderboard to the top 10 rows by `prr` directly. Dragging `prr` into the filter pane offers only numeric filters, not Top N.
- **Cause:** A Top N filter ranks the items in the field it is applied to. On `drugname` or `pt` it keeps the top 10 drugs or reactions with all their rows, not 10 rows. Filters on two fields combine independently, not as a pair.
- **Fix:** Calculated column (New column, not New measure; a measure has no row context and fails with "no current row"): `Pair = [drugname] & " | " & [pt]`. Apply a Top N filter on `Pair`, Top 10, By value = `prr` (Maximum), alongside the existing `signal_flag` and `is_exclusive` filters.
- **Rule:** Top N needs a field that is unique per row; build it as a calculated column. Ranking by `prr` surfaces 3-to-5-report pairs because the Evans floor is 3 reports (D3); decide whether to rank by `case_count` or add a higher minimum, and record it in DECISIONS.

### Issue 32 — Rank Measure Replaces the `Pair` Column for the Top 10 Leaderboard
*Logged · 2026-10-21 · Power BI Desktop, Main leaderboard table (supersedes the fix in Issue 31)*
- **Symptom:** Issue 31's fix (a `Pair` calculated column plus a Top N filter) added a text column across 373,012 rows only to rank 10 rows, and the leaderboard had no rank column.
- **Cause:** A Top N filter needs a per-row field, but a visual-level filter can also be set on a measure.
- **Fix:** `Rank = RANKX ( ALLSELECTED ( gold_signals ), CALCULATE ( MAX ( gold_signals[prr] ) ) * 1000000 + CALCULATE ( MAX ( gold_signals[case_count] ) ), , DESC, DENSE )`. Add it as the first column (header `#`) and filter the table with Rank is less than or equal to 10. The `case_count` term breaks PRR ties so the numbers run 1 to 10. Delete the `Pair` column and remove the Top N filter on it. Build steps updated.
- **Rule:** Apply the Rank filter before relying on the visual: RANKX over `ALLSELECTED ( gold_signals )` with only the SIGNAL and is_exclusive filters covers about 50,000 rows and is very slow. Ranking by `prr` surfaces thin 3-to-5-report pairs (D3); decide whether to rank by `case_count` and record it in DECISIONS.
- **Amended:** The ranking order in this entry's `Rank` measure was changed from PRR to report count. See Issue 33 and DECISIONS D13. The `Pair` column removal and the `Rank <= 10` filter stand.

### Issue 33 — Leaderboard Rank Switched from PRR to Report Count
*Logged · 2026-10-21 · Power BI Desktop, `Rank` measure (amends the measure in Issue 32; no pipeline change)*
- **Symptom:** With the Issue 32 `Rank` measure (ordered by `prr`, `case_count` breaking ties), 7 of the top 10 rows had 7 reports or fewer, four had exactly 3, and three were Haldane-corrected with PRRs of 53,000 to 77,000.
- **Cause:** PRR is unstable on very few reports, and the Evans floor is only 3 (D3). FR-21 says Haldane-corrected PRR is not an interpretable magnitude. Ranking by PRR put these pairs first.
- **Fix:** Swap the two terms: `CALCULATE ( MAX ( gold_signals[case_count] ) ) * 1000000 + CALCULATE ( MAX ( gold_signals[prr] ) )`. The subtitle reads "Ranked by report count." The top 10 is now Dupixent, Tymlos and Paragard pairs with 2,270 to 5,080 reports. Build steps and the HTML mockup updated; decision recorded as D13.
- **Rule:** Do not rank a leaderboard by a ratio measure alone when its minimum count is low. Rank by volume and show the ratio beside it. The `Rank <= 10` filter must be applied first, or `RANKX` runs over about 49,878 rows.

### Issue 34 — Numeric KPI Cards Abbreviated Their Counts; Display Units Set Per Field
*Logged · 2026-10-21 · Power BI Desktop, KPI cards on Signal Leaderboard (not a pipeline change; extends Issue 26)*
- **Symptom:** The `Signal Pairs` card showed "50K" instead of 49,984. The `Cases` card did not, because Issue 26 made it a `FORMAT` text measure. Check the other numeric cards (`Total Pairs`, `Leaderboard Pairs`, `Exclusive Signal Pairs`, `Haldane Pairs`) the same way.
- **Cause:** The card's Display units default to Auto, and the measure's own format (Whole number, thousands separator) does not override it. Wrapping the measure in `FORMAT` hides the problem but turns the measure into text. A measure that took `MIN` over a text `[Signal Pairs]` then failed and the visual would not accept it.
- **Fix:** Keep the measure numeric (build steps, section 4). Select the card > Format visual > Visual > Callout value. Set "Apply settings to" to the specific field (for example `Signal Pairs`), not All; Display units is not offered under All. Set Display units to Custom, then Format code `#,0`. Repeat per card. Build steps, section 6 (card styling step 4) and section 11, updated.
- **Rule:** A measure that feeds a card, another measure or a filter stays numeric; set how a card shows it with that field's Display units, not with `FORMAT`. `FORMAT` stays only for `Cases` (Issue 26) and for text label measures, which are never used in `MIN` or other arithmetic (Issue 29).

### Issue 35 — Leaderboard Note Text: Built as a Measure and Placed in a Text Box, Not the Visual Title
*Logged · 2026-10-21 · Power BI Desktop, Signal Leaderboard note (not a pipeline change)*
- **Symptom:** The note under the leaderboard ("Top 10 of 49,878 leaderboard pairs...") first existed as a calculated column in a one-row table. Then the visual Title's Field value picker would not offer the text measure, and a Card truncated the long text with "...".
- **Cause:** A calculated column is computed at refresh, so it cannot follow filters. A visual Title is a single line and truncates long text. A Card cuts text at its height.
- **Fix:** A measure, `Preview Note Measure`, in table `_TEXT`, shown in a text box (Add a value > Preview Note Measure). `Top N` is `MIN ( 10, [Leaderboard Pairs] )`, so the note follows the data. The calculated column was deleted.
- **Rule:** Long dynamic text is a measure shown in a text box, sized tall enough. Use `FORMAT` only inside such label measures (Issues 26, 29, 34). Test a measure in a Card on the canvas, because measure values do not show in Data view. Check that the Title or Subtitle picker offers the measure before planning around it.

### Issue 36 — Leaderboard Note Followed the Row Selection ("Top 0 of 0")
*Logged · 2026-10-22 · Power BI Desktop, Signal Leaderboard page (not a pipeline change)*
- **Symptom:** Clicking a row in the Exclusive reports panel changed the note under the leaderboard to "Top 0 of 0 leaderboard pairs". Earlier the note had shown blank once ("Top of leaderboard pairs"); that was first treated as a render glitch that did not reproduce.
- **Cause:** The note is a measure in a text box, and a text box follows cross-filtering like any other visual. Selecting an exclusive row filters `gold_signals` to one drug and reaction. `[Leaderboard Pairs]` counts SIGNAL pairs that are not exclusive, so for that pair it returns blank. The blank was the same cause; the `COALESCE ( [Leaderboard Pairs], 0 )` added later as hardening turned it into 0.
- **Fix:** Two layers. (1) Edit interactions on the Signal Leaderboard page set both tables to None for the six cards, the note text box and each other (six `NoFilter` entries in `page.json`). (2) `Preview Note Measure` computes `_total` as `COALESCE ( CALCULATE ( [Leaderboard Pairs], REMOVEFILTERS ( gold_signals ) ), 0 )`: `REMOVEFILTERS` drops the click selection, `[Leaderboard Pairs]` re-applies its own conditions (SIGNAL, not exclusive), and the `COALESCE` stays as a fallback. The numeric measure is unchanged (D14). Re-tested by selecting a leaderboard row and an exclusive row: the note stays "Top 10 of 49,878" and the six cards do not change.
- **Rule:** A text box showing a measure follows cross-filtering like any visual. Click-test every dynamic text box, not only the charts, and set Edit interactions to None wherever a selection must not change a figure. `COALESCE` hides a blank without fixing why it is blank. If a Drug Name slicer is added (D12), decide on purpose whether the note should follow it, because `REMOVEFILTERS ( gold_signals )` also ignores the slicer.

### Issue 37 — Exclusive Panel Said "Few Reports" While Its Top Rows Had 277 and 253
*Logged · 2026-10-22 · Power BI Signal Leaderboard page, HTML dashboard mock (not a pipeline change)*
- **Symptom:** The panel text and the card subtitle said the exclusive pairs "rest on few reports" or are a "few-report screening flag". The top four rows have 277, 253, 58 and 39 reports.
- **Cause:** The wording was written from the typical pair without checking the whole distribution. Of the 106 pairs, 83 have 5 or fewer reports and the median is 4, but four pairs are far larger. The two ParaGard pairs are reported 94.2% and 98.4% by consumers or lawyers (docs/validation-results.md), consistent with stimulated reporting, so a high count does not mean many patients (D9, Issue 21).
- **Fix:** The panel text now says reports are not patients, that duplicates and consumer or legal reporting can inflate the count, and that these are screening flags, not confirmed signals. The card subtitle is "screening flag, not confirmed". The HTML dashboard mock now uses the real 106 exclusive pairs and the real top 10 leaderboard rows from `gold_signals.csv`.
- **Rule:** Check a sentence about a set against its distribution, not only its typical value, before it goes on a visual. A count of reports is not a count of patients, so wording near it should say so.

---

## 4. Running Unit Tests

`src/signal_math.py` is tested by `tests/test_signals.py` (hand-computed 2×2 table, `C == A`, low case count, low χ², dedup).

**Run them in Databricks, not locally** (no Java here, so no local `SparkSession`; Issue 2).

1. Sync the bundle (`sync.paths` includes `notebooks`, `src`, `tests`; if `src`/`tests` don't appear, see Issue 10).
2. Open `tests/run_unit_tests.py` and attach it to Serverless or interactive compute, not a SQL Warehouse (Issue 15).
3. Run All. It runs `%pip install pytest`, restarts Python, and calls `pytest.main()`. `tests/conftest.py` uses the notebook's Spark session via `SparkSession.builder.getOrCreate()`.
4. Expect every test to pass, and confirm the "N passed" line (Issue 20).

Do not add `tests/run_unit_tests.py` to `99_run_pipeline.py`.
