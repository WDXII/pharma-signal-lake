# Validation Results — Signal-Correctness Fix

> Verified on 2026Q1, Databricks run of 2026-09-29. Every number is copied from run output (`sql/verify_signal_fix.sql`, `06_validate_fix.py`, ad hoc queries in Appendix A).
> Branch: `fix/signal-correctness` · Decisions: [DECISIONS.md](../DECISIONS.md) D1–D9
> Condensed 2026-10-06 for readability. All numbers, section numbers and appendix numbers are unchanged.

---

## 1. Summary

### What was wrong

1. **Deduplication was a no-op.** `primaryid` already encodes `caseid` + `caseversion`, so partitioning by it removed nothing (D4, DEVELOPER Issue 9).
2. **Zero cells produced null PRR/ROR.** When a 2×2 cell was 0 (every report of an event involves the drug), the pair fell silently into `NO_SIGNAL`. The most exclusive pairs were the ones lost (D2, Issue 17).
3. **Evans criteria were incomplete.** Classification used case count and PRR only, and carried a `WEAK_SIGNAL` tier with no literature basis (D1, D3).

Found during validation: `silver_cases` fanned out on OUTC/RPSR despite a comment claiming otherwise (D7, Issue 14).

### What changed

Fixes D1–D5 and D7 (see DECISIONS.md), plus two new `gold_signals` columns: `chi_sq` and `haldane_applied`. Classification is now binary under full Evans: `case_count ≥ 3` AND `PRR ≥ 2.0` AND `chi_sq (Yates) ≥ 4.0`.

### Headline numbers

| Metric | Value |
|--------|-------|
| Superseded DEMO versions removed (D4) | 0 (latent fix, see §5.1) |
| Child rows removed (superseded versions) | 0 |
| `silver_cases` fan-out rows removed (D7) | 73,667 (15.6% of 470,891) |
| SIGNAL before (v1) | 56,491 (plus 5,562 `WEAK_SIGNAL`; 62,053 combined) |
| SIGNAL after (v3, binary) | 49,984 |
| SIGNAL net change | −6,507 (−11.5%) |
| Pairs rescued NO_SIGNAL → SIGNAL (D2) | 106 |
| SIGNAL pairs lost to the χ² gate (D3) | 6,613 (11.7% of 56,491) |
| Null PRR pairs (D2) | 2,978 → 0 |

---

## 2. Run Metadata

| Item | Value |
|------|-------|
| Baseline | 2026-06-13: `gold_signals` v1 / `silver_cases` v1 (pipeline last ran then, versions v0–v1) |
| After-fix run | 2026-09-29: `gold_signals` v3 / `silver_cases` v3. Final table version 5, data identical to v3 |
| Quarter | 2026Q1 (`QUARTER` in `00_config.py`) |
| Baseline recovery | Delta time travel, not captured beforehand (Issue 18). Retention widened to 180 days on both tables 2026-09-29 08:20 UTC (v4), v1 restored to `_gold_signals_baseline` / `_silver_cases_baseline`, then both properties UNSET 09:35 UTC (v5), after all verification queries. v4–v5 data is identical to v3 (Appendix A.3–A.5) |
| `05_profile.py` (after) | 2026-09-29 09:37 UTC |
| Verifier | `sql/verify_signal_fix.sql`, blob `9ce44bb`, commit `d0f5097`, unmodified |
| Compute | Serverless |
| Pipeline code validated | `7f552ea` (fix(silver): D7 fan-out, 2026-09-29 12:14 +0800). The v3 run wrote at 12:22–12:24 +0800 |

Only later commit touching notebooks/src before this doc: `55a640f` (`validation_queries.sql` + DEVELOPER.md only; not pipeline code).

---

## 3. Baseline vs After — Profile Metrics

From the `[PROFILE METRIC DELTAS] baseline -> after` output of `06_validate_fix.py` (source: `_profile_results`, written by `05_profile.py`). Baseline = v1 (2026-06-13). After = v3 data, profiled 2026-09-29 09:37 UTC when the table was at v5.

> The baseline profile was **not** run before the re-run (Issue 18). Baseline values were profiled afterwards against the recovered v1 and are recorded as `gold_signals_delta_version` = 1.

| Layer | Metric | Baseline | After | Delta |
|-------|--------|----------|-------|-------|
| bronze | `bronze_demo_rows`, `_distinct_primaryid`, `_distinct_caseid` | 397,224 each | 397,224 each | 0 |
| bronze | `bronze_demo_caseids_multi_version`, `bronze_demo_superseded_rows` | 0 | 0 | 0 |
| bronze | `bronze_{drug,reac,indi,outc,rpsr,ther}_rows_on_superseded_versions` | 0 each | 0 each | 0 |
| silver | `silver_cases_rows` | 470,891 | 397,224 | −73,667 |
| silver | `silver_cases_distinct_primaryid`, `_distinct_caseid` | 397,224 each | 397,224 each | 0 |
| silver | `silver_drug_reactions_rows` | 1,956,087 | 1,956,087 | 0 |
| silver | `silver_therapy_rows` | 404,946 | 404,946 | 0 |
| silver | `silver_drug_reactions_orphan_primaryid`, `silver_therapy_orphan_primaryid` | 0 | 0 | 0 |
| gold | `gold_signals_pairs` | 373,012 | 373,012 | 0 |
| gold | `gold_signal_flag_SIGNAL` | 56,491 | 49,984 | −6,507 |
| gold | `gold_signal_flag_WEAK_SIGNAL` | 5,562 | absent (D1) | — |
| gold | `gold_signal_flag_NO_SIGNAL` | 310,959 | 323,028 | +12,069 |
| gold | `gold_null_prr` | 2,978 | 0 | −2,978 |
| gold | `gold_null_ror` | 7,197 | 0 | −7,197 |
| gold | `gold_null_prr_case_count_ge_min` | 106 | 0 | −106 |
| gold | `gold_exclusive_pairs_case_count_ge_min` | 106 | 106 | 0 |
| gold | `gold_exclusive_pairs_case_count_lt_min` | 2,872 | 2,872 | 0 |
| gold | `gold_pairs_with_zero_cell` | 7,197 | 7,197 | 0 |
| meta | `gold_signals_delta_version` | 1 | 5 | +4 |

Notes:
- "Exclusive, `case_count ≥ 3`" is query E's population (`case_count ≥ 3 AND case_count = event_total`): 106 pairs before (all null PRR, `NO_SIGNAL`), 106 after (all `SIGNAL`, null PRR 0).
- `gold_pairs_with_zero_cell` (7,197) equals the baseline null-ROR count and the post-fix `haldane_applied = true` count.
- Min PRR among SIGNAL rows: 2 before, 2.0001 after. Min `case_count` among SIGNAL rows: 3 in both.
- Signal rate after: 13.4% (49,984 / 373,012).

---

## 4. Checks

### 4.1 `06_validate_fix.py` (V1–V10)

Result: **10/10 PASS** (`VALIDATION SUMMARY: 10/10 passed`).

| Check | What it proves | Observed | Result |
|-------|----------------|----------|--------|
| V1 | `silver_cases`: rows = distinct `caseid` = distinct `primaryid` | 397,224 / 397,224 / 397,224 | PASS |
| V2 | No orphan `primaryid` in `silver_drug_reactions` / `silver_therapy` | 0 / 0 | PASS |
| V3 | `signal_flag` ∈ {SIGNAL, NO_SIGNAL}, no nulls | 0 violating rows | PASS |
| V4 | Every SIGNAL meets all three Evans criteria | 0 violations | PASS |
| V5 | No NO_SIGNAL row meets all three criteria | 0 violations | PASS |
| V6 | No null PRR where `case_count ≥ 3` | 0 violating rows | PASS |
| V7 | `haldane_applied` true exactly when a raw cell is 0 | 0 mismatched rows | PASS |
| V8 | `total_reports` = distinct `primaryid` in `silver_drug_reactions` | 397,209 vs 397,209 | PASS |
| V9 | Independent plain-SQL recompute matches (rel. tol. 0.001) | gold 373,012 · recomputed 373,012 · joined 373,012 · 0 mismatched | PASS |
| V10 | No `case_count` > `drug_total` or `event_total` | 0 violating rows | PASS |

397,224 cases are in `silver_cases`; 397,209 contribute to the signal denominator (N). The other 15 have drug and reaction rows but no primary-suspect (PS) drug, so they cannot form a pair under FR-11 (Appendix A.6).

### 4.2 `sql/verify_signal_fix.sql` (written by Will, read-only for the agent, D6)

Queries A–E were run BEFORE the fix (v1, via time travel, Appendix A) and A–I AFTER. Blob `9ce44bb`, commit `d0f5097`, unmodified.

| Query | BEFORE (v1) | AFTER (v3) | Pass? |
|-------|-------------|------------|-------|
| A (`bronze_demo`) | n/a (Bronze unchanged by the fix) | rows 397,224 · distinct `primaryid` 397,224 · distinct `caseid` 397,224 · superseded 0 | PASS (ceiling for dedup is 0) |
| B (`silver_cases`) | rows 470,891 · distinct `primaryid` 397,224 · distinct `caseid` 397,224 | 397,224 · 397,224 · 397,224 | PASS |
| C (drug-reaction rows without a case) | n/a | 0 | PASS |
| D (classification) | NO_SIGNAL 310,959 (min n 1, min PRR 0.0042, null PRR 2,978) · SIGNAL 56,491 (min n 3, min PRR 2, null 0) · WEAK_SIGNAL 5,562 (min n 3, min PRR 1.5, null 0) | NO_SIGNAL 323,028 (min n 1, min PRR 0.0042, null 0) · SIGNAL 49,984 (min n 3, min PRR 2.0001, null 0) | PASS (only SIGNAL / NO_SIGNAL after) |
| E (exclusive pairs, n ≥ 3) | NO_SIGNAL 106, null PRR 106 | SIGNAL 106, null PRR 0 | PASS |
| F (independent recompute) | n/a | 373,012 pairs compared · PRR / ROR / χ² / Haldane mismatches all 0 | PASS |
| G (Evans check on SIGNAL rows) | n/a | 0 SIGNAL rows failing Evans | PASS |
| H (transition matrix) | n/a | see §5.5 | n/a |
| I (top rescued) | n/a | see §6 | n/a |

### 4.3 Unit tests

`tests/run_unit_tests.py` in Databricks (Serverless, pytest 8.4.2, Python 3.12.3): 5 passed / 0 failed. Validated at 0862a50; later commits do not change src/ or tests/.

---

## 5. Findings per Fix and Transition Matrix

- **5.1 D4, dedup on `caseid`.** Removed 0 rows in 2026Q1. Query A shows 397,224 DEMO rows, distinct `primaryid` and distinct `caseid`, and 0 superseded versions, so every case has one version. This is a latent fix that prevents double-counting when later quarters contain case updates. It is not an impact number for this run.
- **5.2 D7, one row per case.** `silver_cases` had 470,891 rows for 397,224 cases (query B, v1). After aggregating OUTC/RPSR per `primaryid`: 397,224 rows, rows = distinct `primaryid` = distinct `caseid`. 73,667 fan-out rows removed (15.6%). Query C: 0 drug-reaction rows without a case.
- **5.3 D2, Haldane.** Before: 2,978 null-PRR pairs, including all 106 exclusive pairs with `case_count ≥ 3` (query E), all `NO_SIGNAL`. After: null PRR 0, and all 106 are `SIGNAL` (Issue 17).
- **5.4 D3 and D1, full Evans and binary flag.** 6,613 v1 SIGNAL pairs are NO_SIGNAL in v3 (11.7% of 56,491). Ad hoc query H2 attributes all 6,613 to "failed χ² only", none Haldane-affected. The 5,562 v1 `WEAK_SIGNAL` pairs are all `NO_SIGNAL` in v3 because the tier was removed. SIGNAL net change −6,507 (−11.5%).

### 5.5 Transition matrix (query H)

Old `signal_flag` (v1) → new (v3). No pair is missing on either side, and every combination not listed is 0.

| Transition | Pairs | Cause |
|------------|-------|-------|
| SIGNAL → SIGNAL | 49,878 | Meets all three Evans criteria under the fixed code |
| SIGNAL → NO_SIGNAL | 6,613 | Failed χ² ≥ 4 only (H2); 0 Haldane-affected (D3) |
| WEAK_SIGNAL → NO_SIGNAL | 5,562 | Tier removed; flag is binary (D1) |
| NO_SIGNAL → SIGNAL | 106 | Null PRR replaced by Haldane-corrected PRR (D2); same count as query E |
| NO_SIGNAL → NO_SIGNAL | 310,853 | Unchanged |

Lost-SIGNAL reasons (H2): failed χ² only 6,613 · PRR fell below 2 (Haldane) 0 · failed both 0 · χ² null 0. H2 returned a single row.

---

## 6. Top Rescued Signals (query I)

Top 10 rescued signals (NO_SIGNAL → SIGNAL via D2 Haldane), in query I's order (χ² descending). All ten have `case_count = event_total`, so they are exclusive pairs. PRR is not reported for them because Haldane-corrected magnitudes are not interpretable.

### Case-level review

`case_count` counts reports, not patients, and exclusive pairs fill the top of any PRR or χ² ranking, so the ten pairs were checked at case level. Queries P1 (duplicate fingerprint), P2 (case listing) and P3 (reporter occupation) in [`sql/case_review_rescued_signals.sql`](../sql/case_review_rescued_signals.sql) were run on 2026-09-29/30. P1's fingerprint (age, age unit, sex, event date, reporter country) uses `concat_ws`, which skips NULLs, so reports missing those fields collapse into one fingerprint. A low fingerprint count is a prompt for P2 row review, not proof of duplication. "Likely patients" is inferred from case data and is not a verified patient count. Reports (n) equals query I's `case_count`.

| Drug | Reaction (PT) | n | χ² | Fingerprints | Senders | Lit. | Likely patients (inferred) | Case-level evidence (data facts only) | Final group (Will) |
|------|---------------|---|----|--------------|---------|------|----------------------------|---------------------------------------|--------------------|
| RHOPHYLAC | RHESUS ANTIBODIES POSITIVE | 4 | 76025.1133 | 2 | 1 | 0 | duplicates not determinable | CSL BEHRING; all 4 `rept_cod` PER, all F, US; 3 of 4 have no age and no `event_dt` | Known/labeled effect |
| ADSTILADRIN | INSTILLATION SITE DISCHARGE | 10 | 51202.903 | 8 | 1 | 0 | not assessed (P2 not run) | P1 only: 1 sender, 0 literature references | Known/labeled effect |
| MACI | GRAFT DELAMINATION | 3 | 45970.6944 | 3 | 1 | 0 | not assessed (P2 not run) | P1 only: 1 sender, 0 literature references | Known/labeled effect |
| CABLIVI | ADAMTS13 ACTIVITY ABNORMAL | 5 | 37407.0662 | 5 | 1 | 0 | not assessed (P2 not run) | P1 only: 1 sender, 0 literature references | Confounding by indication |
| CARIPRAZINE | CONGENITAL EPIBLEPHARON | 3 | 33098.0833 | 2 | 1 | 0 | 1 (maternal exposure) | `rept_cod` DIR, sender FDA-CTU, `event_dt` 20221229, `fda_dt` 20260210, age 0 YR, sex M/F/F; case IDs 26376050, 26376054, 26376067 | Likely artifact |
| PARAGARD T 380A | FOREIGN BODY IN UROGENITAL TRACT | 277 | 21438.2764 | 235 | 1 | 0 | not assessed (P2 not run) | P1: 1 sender, 0 literature. P3: 94.2% consumer or lawyer reporters, consistent with stimulated reporting | Known/labeled effect |
| PARAGARD T 380A | REPRODUCTIVE COMPLICATION ASSOCIATED WITH DEVICE | 253 | 19572.821 | 217 | 1 | 0 | not assessed (P2 not run) | P1: 1 sender, 0 literature. P3: 98.4% consumer or lawyer reporters, consistent with stimulated reporting | Known/labeled effect |
| XIAFLEX | FRACTURE OF PENIS | 7 | 19484.7615 | 6 | 1 | 0 | not assessed (P2 not run) | P1 only: 1 sender, 0 literature references | Known/labeled effect |
| ZOLGENSMA | RED BLOOD CELL SCHISTOCYTES PRESENT | 5 | 18701.0581 | 2 | 1 | 0 | ~2 | NOVARTIS, all EXP. US: 2 reports, 6 MON F, `event_dt` 20190923, case IDs 26265712 and 16923856. JP: 3 reports, 1 YR F, `event_dt` 20200710, `fda_dt` 20260313, case IDs 18053158, 19879160, 20941170 | Known/labeled effect |
| SIMVASTATIN | GLYCOGEN STORAGE DISEASE TYPE IX | 5 | 15029.7855 | 2 | 5 | 4 | 1 | 5 reports from 5 senders (MICRO LABS LIMITED, ORGANON, CIPLA, AUROBINDO, ACCORD), all EXP, US. 4 cite the same article (Huynh T, Nguyen H, Nguyen M. J Lipid Atheroscler 2026;15(1):183-92); the AUROBINDO report has no `lit_ref` but the same age and sex (46 YR, M) | Likely artifact |

**ParaGard reporter mix (P3, `occp_cod`):**

| PT | CN | LW | HP | MD | PH | Total | Consumer + lawyer | Lawyer | Health professional (HP + MD + PH) |
|----|----|----|----|----|----|-------|-------------------|--------|------------------------------------|
| FOREIGN BODY IN UROGENITAL TRACT | 219 | 42 | 11 | 4 | 1 | 277 | 94.2% | 15.2% | 5.8% |
| REPRODUCTIVE COMPLICATION ASSOCIATED WITH DEVICE | 141 | 108 | 2 | 2 | 0 | 253 | 98.4% | 42.7% | 1.6% |

CN consumer · LW lawyer · HP other health professional · MD physician · PH pharmacist. Percentages are derived from the counts.

**Patient-level finding:** 3 of the 10 pairs (Simvastatin, Cariprazine, Zolgensma) rest on fewer than three likely patients. P2 covers 4 of the 10 pairs and gives a likely-patient estimate for 3; Rhophylac could not be determined and the other 6 were not assessed (§7).

**Reconciliation (D8, amended 2026-09-30):** exclusive pairs number 2,978, equal to the pre-fix null-PRR count. `haldane_applied` pairs number 7,197 = 4,166 + 2,872 + 53 + 106. Of the 159 `SIGNAL` pairs with `haldane_applied`, 106 are exclusive; the other 53 stay in the main leaderboard (full table in DECISIONS.md D8).

### Additional pairs reviewed (Dupixent)

Two exclusive pairs outside the top ten by χ² were checked at case level with the same method: P1 (fingerprint) and P2 (case listing, run 2026-10-21; query P2b in [`sql/case_review_rescued_signals.sql`](../sql/case_review_rescued_signals.sql)). They are not part of the "7 known/labeled, 1 confounding by indication, 2 likely artifacts" grouping or of the patient-level counts above.

| Drug | Reaction (PT) | n | χ² | Fingerprints | Senders | Lit. | Case-level evidence (data facts only) | Final group (Will) |
|------|---------------|---|----|--------------|---------|------|---------------------------------------|--------------------|
| DUPIXENT | REBOUND ECZEMA | 58 | 641.7006 | 52 | 2 | 0 | See combined listing below | Confounding by indication |
| DUPIXENT | REBOUND ATOPIC DERMATITIS | 39 | 427.4938 | 35 | 1 | 0 | See combined listing below | Confounding by indication |

Rebound after stopping treatment is consistent with recurrence of the treated condition. The label (read 2026-10-21) does not list it, and a dechallenge check has not been run.

**Combined P2 listing (97 reports):** 97 distinct `primaryid`; no `caseid` is shared between the two pairs. 54 of the 97 `event_dt` values end in 0101 (year-only dates), and every fingerprint collapse is explained by missing fields or year-only event dates, so no duplicates are evident. 96 reports are from SANOFI AVENTIS and 1 from SANOFI PASTEUR. Reporters are mostly HP and MD, with 3 consumers and no lawyers. Label search: see A.10.

### Clinical plausibility — my assessment

*Screening-level review, not a medical or causality assessment*

As of October 4, 2026, I reviewed the current labels for the **top ten of the 106 exclusive pairs from 2026 Q1**, selected by rank, along with case-level data for four pairs and the reporter mix for the two ParaGard pairs. An **exclusive pair** means that every report of that event in this dataset has this drug as its primary suspect. These pairs can rank high even with a small number of reports, so I treated exclusivity as a screening flag, not as evidence of a new safety issue.

I grouped the ten pairs as **7 known/labeled, 1 confounding by indication, and 2 likely artifacts**. Several of the labels were revised after the 2026 Q1 reports, so this reflects what the labels say now. A label match shows that an event is already recognized for the product; **it does not explain why a pair has 5, 10, or 277 reports**. Some of the matches are also weaker than others. Rhophylac's match is laboratory-test interference, Zolgensma's match is through TMA rather than the term itself, the second ParaGard term is only a partial match, and MACI is listed in postmarketing information.

For Cablivi, I treated the ADAMTS13 finding as **confounding by indication** because it is closely related to the underlying disease being treated. The current label uses ADAMTS13 activity as a marker of the underlying aTTP disease, and a keyword search found no listing of it as an adverse reaction. For the two ParaGard pairs, about 94% and 98% of the reports came from consumers or lawyers, which is consistent with possible stimulated reporting.

Simvastatin and Cariprazine look most like artifacts based on the case-level information I reviewed. The five Simvastatin reports appear to trace back to one published case, while the three Cariprazine reports look tied to one maternal exposure. Neither event term was found by keyword search of the current label.

One limitation of my pipeline is that it counts **reports, not unique patients**. Likely-patient estimates exist for three pairs: Simvastatin (1), Cariprazine (1 exposure), and Zolgensma (~2). Rhophylac could not be determined, and the other six pairs were not assessed at the patient level.

For these ten pairs, my overall view is that exclusivity is useful for screening, but it is not proof of a new safety risk.

---

## 7. Limitations

- **Drug names are normalized by text only** (uppercase + trim). One product can appear under several names, splitting counts and weakening PRR. Example from Query 5.3: `DURVALUMAB` and `IMFINZI` are listed separately. No RxNorm/WHO Drug mapping.
- **Single quarter.** Verified on 2026Q1 only; results may not generalize, and cross-quarter dedup is untested (D4 removed 0 rows this quarter).
- **Haldane is a convention.** +0.5 to every cell is the common continuity correction; exact methods or Bayesian shrinkage (e.g. EBGM) would give different values for sparse pairs. It applies to PRR/ROR only; `chi_sq` uses raw cells.
- **No confidence intervals.** PRR/ROR are point estimates; the χ² gate is the only significance filter.
- **Disproportionality is not causality.** Signals are hypotheses for clinical review.
- **`case_count` counts reports, not patients.** The same literature case sent by several manufacturers, related reports filed together, or follow-ups under new case IDs can inflate n. Case-ID dedup (D4) cannot detect these (D9).
- **Patient-level review is partial.** P2 was run for 4 of the 10 pairs and gives a likely-patient estimate for 3 (Simvastatin, Cariprazine, Zolgensma). Rhophylac could not be determined from missing fields; the other 6 are "not assessed (P2 not run)". Counts are inferred from case data.
- **Label review is a keyword check against current labels.** Each label was searched by keyword (Ctrl+F) for the event term and related terms, one label per product. Zolgensma, Cablivi and Xiaflex labels were revised after the 2026Q1 reports, so the review shows what the labels say now.

---

## Appendix A — Ad hoc queries run (outside the verifier)

These cells were run in Databricks outside `sql/verify_signal_fix.sql`, which was not modified (D6). B, D and E below are the verifier's queries with `VERSION AS OF 1` added after the table path.

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

### A.3–A.5 Baseline recovery: retention widen, restore v1, retention reset

Time travel to v1 had failed with `DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION` (Issue 18). Three Python cells, all on `gold_signals` and `silver_cases`:

- **A.3 Widen:** 2026-09-29 08:20 UTC (v4).
- **A.4 Restore:** output `gold/_gold_signals_baseline` 373,012 rows; `silver/_silver_cases_baseline` 470,891 rows.
- **A.5 Reset (UNSET):** 2026-09-29 09:35 UTC (v5), after all verification queries. Final version is v5, data identical to v3.

```python
base = "/Volumes/workspace/default/pharma_raw/"

# A.3 widen retention
for t in ["gold/gold_signals", "silver/silver_cases"]:
    spark.sql(f"""ALTER TABLE delta.`{base}{t}` SET TBLPROPERTIES (
      'delta.deletedFileRetentionDuration' = 'interval 180 days',
      'delta.logRetentionDuration'         = 'interval 180 days')""")

# A.4 restore v1 to the baseline tables
for src, dst, v in [
    ("gold/gold_signals",   "gold/_gold_signals_baseline",   1),
    ("silver/silver_cases", "silver/_silver_cases_baseline", 1),
]:
    df = spark.read.format("delta").option("versionAsOf", v).load(base + src)
    df.write.format("delta").mode("overwrite").save(base + dst)
    print(f"{dst}: {spark.read.format('delta').load(base + dst).count():,} rows (from {src} v{v})")

# A.5 reset retention
for t in ["gold/gold_signals", "silver/silver_cases"]:
    spark.sql(f"""ALTER TABLE delta.`{base}{t}` UNSET TBLPROPERTIES (
      'delta.deletedFileRetentionDuration', 'delta.logRetentionDuration')""")
```

### A.6 V8 gap: why 15 cases have no drug-reaction row

Result: `no_drug_rows` 0 · `no_ps_drug` 15 · `no_reac_rows` 0 · `cases` 15.

```sql
SELECT
  SUM(CASE WHEN d.primaryid IS NULL THEN 1 ELSE 0 END)                    AS no_drug_rows,
  SUM(CASE WHEN d.primaryid IS NOT NULL AND d.has_ps = 0 THEN 1 ELSE 0 END) AS no_ps_drug,
  SUM(CASE WHEN r.primaryid IS NULL THEN 1 ELSE 0 END)                    AS no_reac_rows,
  COUNT(*)                                                                AS cases
FROM (
  SELECT c.primaryid
  FROM delta.`/Volumes/workspace/default/pharma_raw/silver/silver_cases` c
  LEFT ANTI JOIN delta.`/Volumes/workspace/default/pharma_raw/silver/silver_drug_reactions` dr
    ON c.primaryid = dr.primaryid
) x
LEFT JOIN (
  SELECT primaryid, MAX(CASE WHEN role_cod = 'PS' THEN 1 ELSE 0 END) AS has_ps
  FROM delta.`/Volumes/workspace/default/pharma_raw/bronze/bronze_drug` GROUP BY primaryid
) d ON x.primaryid = d.primaryid
LEFT JOIN (
  SELECT DISTINCT primaryid FROM delta.`/Volumes/workspace/default/pharma_raw/bronze/bronze_reac`
) r ON x.primaryid = r.primaryid;
```

### A.7–A.9 Case-review queries P1–P3

SQL for all three is in [`sql/case_review_rescued_signals.sql`](../sql/case_review_rescued_signals.sql). A fourth query, P4, is the `signal_flag` × `haldane_applied` × `is_exclusive` cross-tab in DECISIONS.md D8.

- **A.7 P1, duplicate fingerprint per rescued pair:** 10 rows, one per pair; `cases` equals query I's n for every pair. Fingerprints, senders and literature counts are in the §6 evidence table.
- **A.8 P2, case listing for low-fingerprint pairs:** 17 rows (Cariprazine 3, Simvastatin 5, Zolgensma 5, Rhophylac 4). The facts read from them are in the §6 evidence table.
- **A.8b P2b, case listing for the two Dupixent exclusive pairs (run 2026-10-21):** 97 rows (REBOUND ECZEMA 58, REBOUND ATOPIC DERMATITIS 39). The facts read from them are in the "Additional pairs reviewed" table in §6.
- **A.9 P3, reporter occupation for the ParaGard pairs:** counts for both pairs (277 and 253 cases) are in the §6 reporter-mix table.

### A.10 Label review (summary of the tracker)

Labels read on DailyMed on 2026-10-04. Keyword search by Ctrl+F on the full label.

| Drug | Label (DailyMed) | Label updated | Where the term matches | Keyword search |
|------|------------------|---------------|------------------------|----------------|
| RHOPHYLAC | CSL Behring | 2025-08-01 | W&P 5.2 Interference with Laboratory Tests (Rh antibodies detectable after administration; matched by meaning) | not run |
| ADSTILADRIN | per DailyMed | 2026-03-24 | 6.1 Table 1: instillation site discharge 33% Grade 1-2 (CS-003, n=157) | not run |
| MACI | per DailyMed | 2026-03-09 | 6.2 Postmarketing; 2.3 Postsurgical Rehabilitation | "delaminat": 3 hits (2.3 x1, 6.2 x2), none in 6.1 |
| CABLIVI | per DailyMed | 2026-04-24 | Section 14 Clinical Studies and 2.1 dosing use ADAMTS13 as a disease-activity marker; not listed as an adverse reaction | "ADAMTS13": 4 hits (2 in Section 14, 2 in dosage text), none in Adverse Reactions |
| CARIPRAZINE | Vraylar (Allergan) | 2025-12-18 | not found | "epiblepharon": not found; "eyelid": not found |
| PARAGARD T 380A | CooperSurgical, NDC 59365-5129-1 | 2024-06-09 | W&P 5.5 Embedment, 5.6 Perforation (foreign body); 5.4 PID and Endometritis, 5.6 (reproductive complication, partial match) | not run |
| XIAFLEX | per DailyMed | 2026-04-22 | Boxed Warning: corporal rupture (penile fracture), 5 of 1044 (0.5%); W&P 5.2. Boxed warning is for Peyronie's disease | not run |
| ZOLGENSMA | Novartis | 2026-06-16 | W&P 5.4 Thrombotic Microangiopathy (also 5.3); matched by meaning | "schistocyte": not found |
| SIMVASTATIN | Zocor (Organon) | 2025-03-28 | not found | "glycogen": not found |
| DUPIXENT (read 2026-10-21) | Sanofi-Aventis U.S. LLC, NDC 0024-5914-00 | 2026-04-22 | not found. Nearest text is 6.2 facial skin reactions (on treatment, a different concept) | "rebound": 0 hits; "post-discontinuation": 0 hits |
