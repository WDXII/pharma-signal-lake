# Validation Results — Signal-Correctness Fix

> Verified on 2026Q1, Databricks run of 2026-09-29. Every number below is copied from the
> Databricks run output (`sql/verify_signal_fix.sql`, `06_validate_fix.py`, ad hoc queries in
> Appendix A). Remaining `[NUMBER?]` cells are values that were not recorded from the run.
> Branch: `fix/signal-correctness` · Decisions: [DECISIONS.md](../DECISIONS.md) D1–D8

---

## 1. Summary

### What was wrong

1. **Deduplication was a no-op.** `Window.partitionBy("primaryid")` cannot remove anything because `primaryid` already encodes `caseversion`; superseded case versions could flow into Silver and Gold (DEVELOPER.md Issue 9).
2. **Zero cells produced null PRR/ROR.** When a 2×2 cell was 0 (e.g. every report of an event involves the drug), PRR/ROR were null and the pair fell silently into `NO_SIGNAL` — the most exclusive drug–event pairs were the ones lost (D2, DEVELOPER.md Issue 17).
3. **Evans criteria were incomplete.** Classification used case count and PRR only (no chi-squared) and carried a `WEAK_SIGNAL` tier with no literature basis (D1, D3).

Found during validation: `silver_cases` fanned out on OUTC/RPSR despite a comment claiming otherwise (D7, Issue 14).

### What changed

- Dedup on `caseid`, keep max `caseversion`; child tables semi-joined to survivors (D4).
- Haldane correction (+0.5 to all four cells when any is 0) for PRR/ROR only (D2).
- Full Evans: `case_count ≥ 3 AND PRR ≥ 2.0 AND chi_sq (Yates) ≥ 4.0`; binary `SIGNAL` / `NO_SIGNAL` (D1, D3).
- ANSI mode left on; division safety comes from the math (D5).
- OUTC/RPSR aggregated per `primaryid` so `silver_cases` is one row per case (D7).
- New columns on `gold_signals`: `chi_sq`, `haldane_applied`.

### Headline numbers

| Metric | Value |
|--------|-------|
| Superseded DEMO versions removed (D4) | 0 (latent bug fix — see §5.1) |
| Child rows removed (superseded versions) | [NUMBER?] |
| `silver_cases` fan-out rows removed (D7) | 73,667 (15.6% of 470,891) |
| SIGNAL before (v1) | 56,491 (plus 5,562 `WEAK_SIGNAL`, tier removed by D1) |
| SIGNAL after (v3, binary) | 49,984 |
| SIGNAL net change | −6,507 (−11.5%) |
| Pairs rescued NO_SIGNAL → SIGNAL (D2 Haldane) | 106 |
| SIGNAL pairs lost to the χ² gate (D3) | 6,613 (11.7% of 56,491) |
| Null PRR pairs (D2) | 2,978 → 0 |

---

## 2. Run Metadata

| Item | Value |
|------|-------|
| Baseline data date | 2026-06-13 (`gold_signals` v1 / `silver_cases` v1; pipeline last ran then, versions v0–v1) |
| After-fix run date | 2026-09-29 (`gold_signals` v3 / `silver_cases` v3) |
| Quarter | 2026Q1 (`QUARTER` in `00_config.py`) |
| `gold_signals` Delta version — before fix | 1 |
| `gold_signals` Delta version — after fix | 3 |
| Baseline recovery | Delta time travel, not captured beforehand (Issue 18). `delta.deletedFileRetentionDuration` and `delta.logRetentionDuration` were temporarily raised to 180 days, v1 was restored to `_baseline` paths, and both properties were UNSET afterward. This added versions 4–5, with data identical to v3 (Appendix A.4) |
| Verifier | `sql/verify_signal_fix.sql`, blob `9ce44bb`, commit `d0f5097`, unmodified |
| Compute | [NUMBER?] |
| Pipeline code commit validated | [NUMBER?] |

---

## 3. Baseline vs After — Profile Metrics

Baseline = v1 (2026-06-13); After = v3 (2026-09-29). Values come from `sql/verify_signal_fix.sql` and `06_validate_fix.py` output. The `05_profile.py` `_profile_results` metrics that were not recorded from the run are left as `[NUMBER?]`. A dash in the Delta column means no delta was recorded.

| Layer | Metric | Baseline | After | Delta |
|-------|--------|----------|-------|-------|
| bronze | `bronze_demo_rows` | 397,224 | 397,224 | 0 |
| bronze | `bronze_demo_distinct_primaryid` | 397,224 | 397,224 | 0 |
| bronze | `bronze_demo_distinct_caseid` | 397,224 | 397,224 | 0 |
| bronze | `bronze_demo_caseids_multi_version` | [NUMBER?] | [NUMBER?] | — |
| bronze | `bronze_demo_superseded_rows` | 0 | 0 | 0 |
| bronze | `bronze_{drug,reac,indi,outc,rpsr,ther}_rows_on_superseded_versions` | [NUMBER?] | [NUMBER?] | — |
| silver | `silver_cases_rows` | 470,891 | 397,224 | −73,667 |
| silver | `silver_cases_distinct_primaryid` | 397,224 | 397,224 | 0 |
| silver | `silver_cases_distinct_caseid` | 397,224 | 397,224 | 0 |
| silver | `silver_drug_reactions_rows` | [NUMBER?] | [NUMBER?] | — |
| silver | `silver_drug_reactions_orphan_primaryid` | [NUMBER?] | 0 | — |
| silver | `silver_therapy_rows` | [NUMBER?] | [NUMBER?] | — |
| silver | `silver_therapy_orphan_primaryid` | [NUMBER?] | [NUMBER?] | — |
| gold | `gold_signals_pairs` | [NUMBER?] | 373,012 | — |
| gold | `gold_signal_flag_SIGNAL` | 56,491 | 49,984 | −6,507 |
| gold | `gold_signal_flag_WEAK_SIGNAL` | 5,562 | absent (D1) | — |
| gold | `gold_signal_flag_NO_SIGNAL` | 310,959 | 323,028 | — |
| gold | `gold_null_prr` | 2,978 | 0 | — |
| gold | `gold_null_ror` | [NUMBER?] | [NUMBER?] | — |
| gold | `gold_null_prr_case_count_ge_min` | [NUMBER?] | 0 | — |
| gold | `gold_exclusive_pairs_case_count_ge_min` | 106 | 106 | 0 |
| gold | `gold_exclusive_pairs_case_count_lt_min` | [NUMBER?] | [NUMBER?] | — |
| gold | `gold_pairs_with_zero_cell` | [NUMBER?] | [NUMBER?] | — |

Notes:
- "Exclusive pairs, case_count ≥ 3" is query E's population (`case_count ≥ 3 AND case_count = event_total`): 106 pairs before, all with null PRR and `NO_SIGNAL`; 106 pairs after, all `SIGNAL` with null PRR 0.
- Min PRR among SIGNAL rows: 2 before, 2.0001 after. Min `case_count` among SIGNAL rows: 3 in both.
- Extra rows not recorded: pairs with `haldane_applied = true` [NUMBER?]; signal rate (SIGNAL ÷ pairs) [NUMBER?].

---

## 4. Checks

### 4.1 `06_validate_fix.py` (V1–V10)

Result: **10/10 PASS**. Observed values other than V1 and V9 were not recorded from the run.

| Check | What it proves | Observed | Result |
|-------|----------------|----------|--------|
| V1 | `silver_cases`: rows == distinct `caseid` == distinct `primaryid` | 397,224 / 397,224 / 397,224 | PASS |
| V2 | No orphan `primaryid` in `silver_drug_reactions` / `silver_therapy` | [NUMBER?] | PASS |
| V3 | `signal_flag` ∈ {SIGNAL, NO_SIGNAL}, no nulls | [NUMBER?] | PASS |
| V4 | Every SIGNAL meets all three Evans criteria | [NUMBER?] violations | PASS |
| V5 | No NO_SIGNAL row meets all three criteria | [NUMBER?] violations | PASS |
| V6 | No null PRR where `case_count ≥ 3` | [NUMBER?] | PASS |
| V7 | `haldane_applied` true exactly when a raw cell is 0 | [NUMBER?] mismatches | PASS |
| V8 | `total_reports` == distinct `primaryid` in `silver_drug_reactions` | [NUMBER?] vs [NUMBER?] | PASS |
| V9 | Independent plain-SQL recompute matches (rel. tol. 0.001) | 373,012 / 373,012 | PASS |
| V10 | No `case_count` > `drug_total` or `event_total` | [NUMBER?] | PASS |

### 4.2 `sql/verify_signal_fix.sql` (written by Will, read-only for the agent — D6)

Queries A–E run BEFORE the fix (v1, via time travel — Appendix A); A–I run AFTER. Blob `9ce44bb`, commit `d0f5097`, unmodified.

| Query | Result BEFORE (v1) | Result AFTER (v3) | Pass? |
|-------|--------------------|-------------------|-------|
| A (`bronze_demo`) | n/a (Bronze is unchanged by the fix) | rows 397,224 · distinct `primaryid` 397,224 · distinct `caseid` 397,224 · superseded 0 | PASS (ceiling for dedup is 0) |
| B (`silver_cases`) | rows 470,891 · distinct `primaryid` 397,224 · distinct `caseid` 397,224 | 397,224 · 397,224 · 397,224 | PASS |
| C (drug-reaction rows without a case) | n/a | 0 | PASS |
| D (classification) | NO_SIGNAL 310,959 (min n 1, min PRR 0.0042, null PRR 2,978) · SIGNAL 56,491 (min n 3, min PRR 2, null 0) · WEAK_SIGNAL 5,562 (min n 3, min PRR 1.5, null 0) | NO_SIGNAL 323,028 (min n 1, min PRR 0.0042, null 0) · SIGNAL 49,984 (min n 3, min PRR 2.0001, null 0) | PASS (only SIGNAL / NO_SIGNAL after) |
| E (exclusive pairs, n ≥ 3) | NO_SIGNAL 106, null PRR 106 | SIGNAL 106, null PRR 0 | PASS |
| F (independent recompute) | n/a | 373,012 pairs compared · PRR / ROR / χ² / Haldane mismatches all 0 | PASS |
| G (Evans check on SIGNAL rows) | n/a | 0 SIGNAL rows failing Evans | PASS |
| H (transition matrix, V = 1) | n/a | see §5 | n/a |
| I (top rescued, V = 1) | n/a | see §6 | n/a |

### 4.3 Unit tests

`tests/run_unit_tests.py` in Databricks: [NUMBER?] passed / [NUMBER?] failed.

---

## 5. Findings per Fix and Transition Matrix

### 5.1 D4 — Dedup on `caseid`

**Deduplication removed 0 rows in 2026Q1.** Query A shows 397,224 DEMO rows, 397,224 distinct `primaryid`, 397,224 distinct `caseid` and 0 superseded versions, so every case in this quarter has exactly one version. This is a latent bug fix: it prevents double-counting when further quarters are added and cases have been updated. It is not an impact number for this run.

### 5.2 D7 — One row per case in Silver

`silver_cases` had 470,891 rows for 397,224 distinct cases before the fix (query B, v1). After aggregating OUTC/RPSR per `primaryid` it has 397,224 rows, and rows = distinct `primaryid` = distinct `caseid`. 73,667 fan-out rows were removed (15.6% of 470,891). Query C returns 0 drug-reaction rows without a case.

### 5.3 D2 — Haldane correction

Before the fix, 2,978 pairs had a null PRR, and all 106 exclusive pairs with `case_count ≥ 3` (query E) were classified `NO_SIGNAL` with null PRR. After the fix, null PRR is 0 and all 106 of those pairs are `SIGNAL`. Rule: a null metric must never fall silently into a default class (Issue 17).

### 5.4 D3 — Full Evans (χ²) and D1 — binary classification

6,613 pairs that were SIGNAL in v1 are NO_SIGNAL in v3 (11.7% of 56,491). Ad hoc query H2 attributes all 6,613 to "failed χ² only", and 0 of them are Haldane-affected. The 5,562 v1 `WEAK_SIGNAL` pairs are all `NO_SIGNAL` in v3 because the tier was removed (D1). SIGNAL net change is −6,507 (−11.5%).

### 5.5 Transition matrix (query H, V = 1)

Rows = old `signal_flag` (v1); columns = new (v3). Combinations that do not appear in the query H output are shown as 0. `ABSENT` = pair missing on that side; no ABSENT rows were returned.

| Old ↓ / New → | SIGNAL | NO_SIGNAL | ABSENT |
|---------------|--------|-----------|--------|
| SIGNAL | 49,878 | 6,613 | 0 |
| WEAK_SIGNAL | 0 | 5,562 | 0 |
| NO_SIGNAL | 106 | 310,853 | 0 |
| ABSENT | 0 | 0 | — |

| Transition | Pairs | Cause |
|------------|-------|-------|
| SIGNAL → SIGNAL | 49,878 | Meet all three Evans criteria under the fixed code; unchanged class |
| SIGNAL → NO_SIGNAL | 6,613 | Failed χ² ≥ 4 only (H2); 0 Haldane-affected (D3) |
| WEAK_SIGNAL → NO_SIGNAL | 5,562 | `WEAK_SIGNAL` tier removed; the flag is binary (D1) |
| NO_SIGNAL → SIGNAL | 106 | Null PRR replaced by Haldane-corrected PRR (D2); same count as query E |
| NO_SIGNAL → NO_SIGNAL | 310,853 | Unchanged class |

Lost-SIGNAL reasons (H2): failed χ² only 6,613 · PRR fell below 2 (Haldane) 0 · failed both 0 · χ² null 0. H2 returned a single row, "failed chi-sq only", so no other reason category appears.

---

## 6. Top Rescued Signals (query I)

**Top 10 rescued signals (NO_SIGNAL → SIGNAL, D2 Haldane), ordered by χ² descending (query I).** The table is in query I's output order. All ten have `case_count = event_total`, so they are exclusive pairs and the Haldane correction applies. PRR is not reported for exclusive pairs because Haldane-corrected magnitudes are not interpretable.

| Drug | Reaction (PT) | case_count | χ² |
|------|---------------|-----------|----|
| RHOPHYLAC | RHESUS ANTIBODIES POSITIVE | 4 | 76025.1133 |
| ADSTILADRIN | INSTILLATION SITE DISCHARGE | 10 | 51202.903 |
| MACI | GRAFT DELAMINATION | 3 | 45970.6944 |
| CABLIVI | ADAMTS13 ACTIVITY ABNORMAL | 5 | 37407.0662 |
| CARIPRAZINE | CONGENITAL EPIBLEPHARON | 3 | 33098.0833 |
| PARAGARD T 380A | FOREIGN BODY IN UROGENITAL TRACT | 277 | 21438.2764 |
| PARAGARD T 380A | REPRODUCTIVE COMPLICATION ASSOCIATED WITH DEVICE | 253 | 19572.821 |
| XIAFLEX | FRACTURE OF PENIS | 7 | 19484.7615 |
| ZOLGENSMA | RED BLOOD CELL SCHISTOCYTES PRESENT | 5 | 18701.0581 |
| SIMVASTATIN | GLYCOGEN STORAGE DISEASE TYPE IX | 5 | 15029.7855 |

> **Clinical plausibility — Will's assessment:** [TO BE WRITTEN BY WILL]

---

## 7. Limitations

- **Drug name normalization is text-only** (uppercase + trim). The same product appears under several names, splitting counts and weakening PRR. Example seen in Query 5.3 output: `DURVALUMAB` and `IMFINZI` are listed separately. No RxNorm/WHO Drug mapping.
- **Single quarter.** Verified on one FAERS quarter (2026Q1); results may not generalize, and dedup across quarters is untested (D4 removed 0 rows this quarter).
- **Haldane is a convention, not the only option.** +0.5 to every cell is the common continuity correction, but other approaches (exact methods, Bayesian shrinkage such as EBGM) would give different values for sparse pairs. It is applied to PRR/ROR only; `chi_sq` uses raw cells.
- **No confidence intervals.** PRR/ROR are point estimates; the χ² gate is the only significance filter.
- **Disproportionality is not causality.** Signals are hypotheses for clinical review, not confirmed adverse reactions.

---

## Appendix A — Ad hoc queries run (outside the verifier)

These cells were run in Databricks outside `sql/verify_signal_fix.sql`. The verifier file itself was not modified (D6). B, D and E are the verifier's queries with `VERSION AS OF 1` added after the table path.

### A.1 B, D, E before the fix (v1)

```sql
-- B (before): silver_cases at version 1
SELECT COUNT(*)                  AS rows,
       COUNT(DISTINCT primaryid) AS distinct_primaryid,
       COUNT(DISTINCT caseid)    AS distinct_caseid
FROM delta.`/Volumes/workspace/default/pharma_raw/silver/silver_cases` VERSION AS OF 1;

-- D (before): gold_signals at version 1
SELECT signal_flag,
       COUNT(*)          AS pairs,
       MIN(case_count)   AS min_cases,
       MIN(prr)          AS min_prr,
       SUM(CASE WHEN prr IS NULL THEN 1 ELSE 0 END) AS null_prr
FROM delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals` VERSION AS OF 1
GROUP BY signal_flag
ORDER BY signal_flag;

-- E (before): gold_signals at version 1
SELECT signal_flag,
       COUNT(*)                                      AS pairs,
       SUM(CASE WHEN prr IS NULL THEN 1 ELSE 0 END)  AS null_prr
FROM delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals` VERSION AS OF 1
WHERE case_count >= 3
  AND case_count = event_total
GROUP BY signal_flag;
```

### A.2 H2 — reason for each SIGNAL → NO_SIGNAL transition

```sql
SELECT CASE
         WHEN n.chi_sq IS NULL              THEN 'chi_sq null'
         WHEN n.chi_sq < 4 AND n.prr >= 2   THEN 'failed chi-sq only'
         WHEN n.chi_sq >= 4 AND n.prr < 2   THEN 'PRR fell below 2 (Haldane)'
         ELSE 'failed both'
       END AS reason,
       COUNT(*) AS pairs,
       SUM(CASE WHEN n.haldane_applied THEN 1 ELSE 0 END) AS haldane_pairs
FROM delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals` n
JOIN delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals` VERSION AS OF 1 o
  ON o.drugname = n.drugname AND o.pt = n.pt
WHERE o.signal_flag = 'SIGNAL' AND n.signal_flag = 'NO_SIGNAL'
GROUP BY 1;
```

### A.3 Retention widen (to recover the v1 baseline via time travel)

Run because time travel to v1 failed with `DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION` (Issue 18). Applied to both `gold_signals` and `silver_cases`.

```sql
ALTER TABLE delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals`
SET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = 'interval 180 days',
  'delta.logRetentionDuration'         = 'interval 180 days');

ALTER TABLE delta.`/Volumes/workspace/default/pharma_raw/silver/silver_cases`
SET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = 'interval 180 days',
  'delta.logRetentionDuration'         = 'interval 180 days');
```

### A.4 Retention reset

Both properties were UNSET after the v1 data was restored to the `_baseline` paths. Each `ALTER` added a table version, giving versions 4–5 with data identical to v3.

```sql
ALTER TABLE delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals`
UNSET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration',
  'delta.logRetentionDuration');

ALTER TABLE delta.`/Volumes/workspace/default/pharma_raw/silver/silver_cases`
UNSET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration',
  'delta.logRetentionDuration');
```

> A.3 and A.4 are reconstructed from the run description (property names, 180 days, UNSET), not pasted from the notebook cell. Confirm against the notebook before publishing.
