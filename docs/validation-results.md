# Validation Results — Signal-Correctness Fix

> **Template.** Fill in after the Databricks run (order in [SETUP.md](../SETUP.md) §10).
> Every `[NUMBER?]` / `[DATE?]` is a placeholder. Only enter values copied from
> notebook or SQL output — do not estimate.
> Branch: `fix/signal-correctness` · Decisions: [DECISIONS.md](../DECISIONS.md) D1–D7

---

## 1. Summary

### What was wrong

1. **Deduplication was a no-op.** `Window.partitionBy("primaryid")` cannot remove anything because `primaryid` already encodes `caseversion`; superseded case versions could flow into Silver and Gold (DEVELOPER.md Issue 9).
2. **Zero cells produced null PRR/ROR.** When a 2×2 cell was 0 (e.g. every report of an event involves the drug), PRR/ROR were null and the pair fell silently into `NO_SIGNAL` — the most exclusive drug–event pairs were the ones lost (D2).
3. **Evans criteria were incomplete.** Classification used case count and PRR only (no chi-squared) and carried a `WEAK_SIGNAL` tier with no literature basis (D1, D3).

Found during validation: `silver_cases` fanned out on OUTC/RPSR despite a comment claiming otherwise (D7, Issue 14).

### What changed

- Dedup on `caseid`, keep max `caseversion`; child tables semi-joined to survivors (D4).
- Haldane correction (+0.5 to all four cells when any is 0) for PRR/ROR only (D2).
- Full Evans: `case_count ≥ 3 AND PRR ≥ 2.0 AND chi_sq (Yates) ≥ 4.0`; binary `SIGNAL` / `NO_SIGNAL` (D1, D3).
- ANSI mode left on; division safety comes from the math (D5).
- OUTC/RPSR aggregated per `primaryid` so `silver_cases` is one row per case (D7).
- New columns on `gold_signals`: `chi_sq`, `haldane_applied`.

### RESUME NUMBERS

Copy from the `RESUME NUMBERS` block printed by `notebooks/06_validate_fix.py`.

| Metric | Value |
|--------|-------|
| Superseded DEMO versions removed | [NUMBER?] |
| Child rows removed (superseded versions) | [NUMBER?] |
| Signals before (SIGNAL + WEAK_SIGNAL) | [NUMBER?] |
| Signals after (SIGNAL, binary) | [NUMBER?] |
| Pairs rescued by Haldane → SIGNAL | [NUMBER?] |
| Pairs dropped by χ² gate | [NUMBER?] |

---

## 2. Run Metadata

| Item | Value |
|------|-------|
| Run date | [DATE?] |
| Quarter | [NUMBER?] (`QUARTER` in `00_config.py`) |
| `gold_signals` Delta version — before fix (`<V>`) | [NUMBER?] |
| `gold_signals` Delta version — after fix | [NUMBER?] |
| Compute | [Serverless / cluster name?] |
| Commit validated | [SHA?] |

---

## 3. Baseline vs After — Profile Metrics

From the `baseline-vs-after` table in `06_validate_fix.py` (source: `_profile_results`, written by `05_profile.py`).
Add or remove rows to match the metrics the notebook prints.

| Layer | Metric | Baseline | After | Delta |
|-------|--------|----------|-------|-------|
| bronze | `bronze_demo_rows` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| bronze | `bronze_demo_distinct_primaryid` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| bronze | `bronze_demo_distinct_caseid` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| bronze | `bronze_demo_caseids_multi_version` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| bronze | `bronze_demo_superseded_rows` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| bronze | `bronze_{drug,reac,indi,outc,rpsr,ther}_rows_on_superseded_versions` (one row each) | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| silver | `silver_cases_rows` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| silver | `silver_cases_distinct_primaryid` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| silver | `silver_cases_distinct_caseid` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| silver | `silver_drug_reactions_rows` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| silver | `silver_drug_reactions_orphan_primaryid` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| silver | `silver_therapy_rows` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| silver | `silver_therapy_orphan_primaryid` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| gold | `gold_signals_pairs` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| gold | `gold_signal_flag_SIGNAL` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| gold | `gold_signal_flag_WEAK_SIGNAL` | [NUMBER?] | absent (D1) | [NUMBER?] |
| gold | `gold_signal_flag_NO_SIGNAL` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| gold | `gold_null_prr` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| gold | `gold_null_ror` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| gold | `gold_null_prr_case_count_ge_min` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| gold | `gold_exclusive_pairs_case_count_ge_min` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| gold | `gold_exclusive_pairs_case_count_lt_min` | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| gold | `gold_pairs_with_zero_cell` | [NUMBER?] | [NUMBER?] | [NUMBER?] |

Extra rows worth recording from the `after` run: pairs with `haldane_applied = true` [NUMBER?]; signal rate (SIGNAL ÷ pairs) [NUMBER?]%.

---

## 4. Checks

### 4.1 `06_validate_fix.py` (V1–V10)

| Check | What it proves | Observed | Result |
|-------|----------------|----------|--------|
| V1 | `silver_cases`: rows == distinct `caseid` == distinct `primaryid` | [NUMBER?] / [NUMBER?] / [NUMBER?] | [PASS/FAIL?] |
| V2 | No orphan `primaryid` in `silver_drug_reactions` / `silver_therapy` | [NUMBER?] | [PASS/FAIL?] |
| V3 | `signal_flag` ∈ {SIGNAL, NO_SIGNAL}, no nulls | [NUMBER?] | [PASS/FAIL?] |
| V4 | Every SIGNAL meets all three Evans criteria | [NUMBER?] violations | [PASS/FAIL?] |
| V5 | No NO_SIGNAL row meets all three criteria | [NUMBER?] violations | [PASS/FAIL?] |
| V6 | No null PRR where `case_count ≥ 3` | [NUMBER?] | [PASS/FAIL?] |
| V7 | `haldane_applied` true exactly when a raw cell is 0 | [NUMBER?] mismatches | [PASS/FAIL?] |
| V8 | `total_reports` == distinct `primaryid` in `silver_drug_reactions` | [NUMBER?] vs [NUMBER?] | [PASS/FAIL?] |
| V9 | Independent plain-SQL recompute matches (rel. tol. 0.001) | [NUMBER?] of [NUMBER?] pairs differ | [PASS/FAIL?] |
| V10 | No `case_count` > `drug_total` or `event_total` | [NUMBER?] | [PASS/FAIL?] |

### 4.2 `sql/verify_signal_fix.sql` (written by Will, read-only for the agent — D6)

Queries A–E run BEFORE the fix; A–I run AFTER, with `<V>` = the version recorded in §2.

| Query | Result BEFORE | Result AFTER | Pass? |
|-------|---------------|--------------|-------|
| A | [NUMBER?] | [NUMBER?] | [PASS/FAIL?] |
| B | [NUMBER?] | [NUMBER?] | [PASS/FAIL?] |
| C | [NUMBER?] | [NUMBER?] | [PASS/FAIL?] |
| D | [NUMBER?] | [NUMBER?] | [PASS/FAIL?] |
| E | [NUMBER?] | [NUMBER?] | [PASS/FAIL?] |
| F | n/a | [NUMBER?] | [PASS/FAIL?] |
| G | n/a | [NUMBER?] | [PASS/FAIL?] |
| H | n/a | see §5 | n/a |
| I | n/a | see §6 | n/a |

### 4.3 Unit tests

`tests/run_unit_tests.py` in Databricks: [NUMBER?] passed / [NUMBER?] failed.

---

## 5. Transition Matrix (query H)

Rows = old `signal_flag` (baseline); columns = new. `ABSENT` = pair missing on that side (e.g. disappeared after dedup).

| Old ↓ / New → | SIGNAL | NO_SIGNAL | ABSENT |
|---------------|--------|-----------|--------|
| SIGNAL | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| WEAK_SIGNAL | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| NO_SIGNAL | [NUMBER?] | [NUMBER?] | [NUMBER?] |
| ABSENT | [NUMBER?] | [NUMBER?] | — |

Lost-SIGNAL reasons (from `06_validate_fix.py`): failed χ² [NUMBER?] · case count [NUMBER?] · PRR [NUMBER?] · pair disappeared after dedup [NUMBER?].

---

## 6. Top Rescued Signals (query I)

Pairs that became SIGNAL, top 20.

| Rank | Drug | Reaction (PT) | case_count | PRR | ROR | chi_sq | haldane_applied |
|------|------|---------------|-----------|-----|-----|--------|-----------------|
| 1 | [TEXT?] | [TEXT?] | [NUMBER?] | [NUMBER?] | [NUMBER?] | [NUMBER?] | [true/false?] |
| … | | | | | | | |

**Clinical plausibility — my assessment:**

[Write here. Are the rescued pairs consistent with labeled or published adverse reactions? Note any that look like indication confounding or reporting artifacts.]

---

## 7. Limitations

- **Drug name normalization is text-only** (uppercase + trim). The same product appears under several names, splitting counts and weakening PRR. Example seen in Query 5.3 output: `DURVALUMAB` and `IMFINZI` are listed separately. No RxNorm/WHO Drug mapping.
- **Single quarter.** Verified on one FAERS quarter ([NUMBER?] = `QUARTER`); results may not generalize, and dedup across quarters is untested.
- **Haldane is a convention, not the only option.** +0.5 to every cell is the common continuity correction, but other approaches (exact methods, Bayesian shrinkage such as EBGM) would give different values for sparse pairs. It is applied to PRR/ROR only; `chi_sq` uses raw cells.
- **No confidence intervals.** PRR/ROR are point estimates; the χ² gate is the only significance filter.
- **Disproportionality is not causality.** Signals are hypotheses for clinical review, not confirmed adverse reactions.
