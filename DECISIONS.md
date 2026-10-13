# DECISIONS.md — PharmaSignalLake

Architecture and correctness decisions, recorded as they were made. Each entry: problem, decision, effect, what was rejected, and the evidence from the 2026Q1 run. Full numbers live in `docs/validation-results.md`.

## At a glance

| ID | Decision | Date | Status |
|---|---|---|---|
| D1 | Binary classification: `SIGNAL` / `NO_SIGNAL` (drop `WEAK_SIGNAL`) | 2026-09-28 | Accepted |
| D2 | Haldane correction for zero cells (PRR/ROR only) | 2026-09-28 | Accepted |
| D3 | Full Evans Criteria: n ≥ 3, PRR ≥ 2, χ² (Yates) ≥ 4 | 2026-09-28 | Accepted |
| D4 | Deduplicate on `caseid`, not `primaryid` | 2026-09-28 | Accepted |
| D5 | ANSI mode stays ON in Gold | 2026-09-28 | Accepted |
| D6 | `sql/verify_signal_fix.sql` is read-only for the assistant | 2026-09-28 | Accepted |
| D7 | Aggregate OUTC/RPSR per case (no fan-out) | 2026-09-29 | Accepted (reverses an earlier call) |
| D8 | Exclusive pairs in a separate dashboard panel, filtered on `is_exclusive` | 2026-09-29, amended 09-30 | Accepted |
| D9 | Defer patient-level (report) deduplication | 2026-09-30 | Accepted |
| D10 | Per-stage timing log: UTC, `perf_counter`, in-memory | 2026-10-06 | Accepted (verified on Databricks 2026-10-06, OK path only) |
| D11 | Exclude `EU` and `COUNTRY NOT SPECIFIED` from the Top 10 reporter countries chart (report layer only) | 2026-10-14 | Accepted |

---

## D1 — Binary Signal Classification

**Problem:** `WEAK_SIGNAL` (PRR ≥ 1.5) has no literature basis and invites treating an under-powered PRR as a signal.
**Decision:** Remove `WEAK_SIGNAL` from `03_gold_signals.py`, `00_config.py` and all docs. Classification is `SIGNAL` or `NO_SIGNAL`, driven by full Evans Criteria (D3).
**Effect:** Simpler validation queries and Power BI filters. A pair that was `WEAK_SIGNAL` is now `NO_SIGNAL` unless it clears Evans on its own.
**Rejected:** Keeping a 1.5 "early-warning" tier: arbitrary threshold, over-signaling risk.
**Evidence:** `gold_signals` has only `NO_SIGNAL` 323,028 and `SIGNAL` 49,984. BRD v2.1 updated (Issue 13).

## D2 — Haldane Correction for Zero Cells

**Problem:** When any 2×2 cell is 0 (most often `c = 0`: the drug is the only reporter of the event), PRR/ROR were `NULL`, so the most exclusive pairs could never be flagged (Issue 17).
**Decision:** Add 0.5 to all four cells whenever any is 0, for PRR/ROR only. Record it in a `haldane_applied` column. `chi_sq` uses the raw cells (D3).
**Effect:** Every pair gets a finite, conservative PRR/ROR and is evaluated against Evans.
**Rejected:** An `EXCLUSIVE_REPORT` label: gives no signal strength for exactly the pairs that need it.
**Evidence:** Null PRR 2,978 → 0; null ROR 7,197 → 0. `haldane_applied` = 7,197 pairs. 106 pairs with `case_count ≥ 3` moved `NO_SIGNAL` → `SIGNAL`. BRD v2.1 FR-14/15 updated.

## D3 — Full Evans Criteria

**Problem:** Classification used only `case_count` and PRR; PRR alone flags noise from small samples.
**Decision:** `SIGNAL` only if `case_count ≥ 3` and `prr ≥ 2.0` and `chi_sq ≥ 4.0` (Yates), else `NO_SIGNAL`. Thresholds are `EVANS_MIN_CASES`, `EVANS_MIN_PRR`, `EVANS_MIN_CHI2` in `00_config.py`. `case_count` is the raw A count.
**Effect:** High-PRR pairs with weak χ² (small samples) no longer classify as `SIGNAL`. This is the intended stricter behavior.
**Rejected:** PRR-only classification: statistically incomplete.
**Evidence:** `SIGNAL` 49,984, down from 62,053 (56,491 `SIGNAL` + 5,562 `WEAK_SIGNAL`) even after the 106 rescues. 6,613 `SIGNAL` → `NO_SIGNAL`, all "failed χ² only", none Haldane-affected. BRD v2.1 FR-16 updated.

## D4 — Deduplicate on `caseid`, Not `primaryid`

**Problem:** `primaryid = caseid + caseversion` is already unique per row, so a window partitioned by it removes nothing (Issue 9).
**Decision:** Partition DEMO by `caseid`, order by `caseversion` (`try_cast(... AS INT)`, `desc_nulls_last`), keep the top row. The logic is `dedup_cases()` in `src/signal_math.py`, called from `02_silver_clean.py`. `left_semi` join every child table that Silver reads (`DRUG`, `REAC`, `OUTC`, `RPSR`, `THER`) to the surviving `primaryid`s. `INDI` is not read in Silver. This matches BRD FR-06.
**Effect:** Zero numeric impact on 2026Q1 (every `caseid` has one version). It matters for quarters with case updates.
**Rejected:** None. The old key was a latent bug, not a tradeoff.
**Evidence:** 397,224 = 397,224 distinct `primaryid` = `caseid`, 0 superseded. Latent fix, validated structurally, not yet exercised by a real update. BRD v2.1 FR-06 updated.

## D5 — ANSI Mode Stays ON in Gold

**Problem:** Issue 7 disabled `spark.sql.ansi.enabled` to silence `DIVIDE_BY_ZERO`, which masked a real guard bug (the ROR guard let `C − A = 0` through).
**Decision:** Remove the override in `03_gold_signals.py`. Division safety comes from the math: Haldane (D2) makes all four cells > 0 before any division.
**Effect:** If ANSI raises again, it is a real guard bug to fix, not a reason to turn ANSI off.
**Rejected:** Leaving ANSI off permanently: it hides real bugs.

## D6 — `sql/verify_signal_fix.sql` Is Read-Only for the Assistant

**Problem:** A verifier is only independent if the agent that wrote the pipeline cannot edit it.
**Decision:** Will writes and maintains `sql/verify_signal_fix.sql` outside the session. Claude may read it but never edit, move or rename it. If a check looks wrong, stop and explain.
**Rejected:** One session maintaining both pipeline and verifier: collapses the independence.
**Evidence:** Committed as `d0f5097` (blob `9ce44bb`) before the fix was built; unmodified when results were recorded.

## D7 — Aggregate OUTC/RPSR Per Case

**Supersedes** the 2026-09-28 "option A: keep the grain" call.
**Problem:** `build_silver_cases()` joined DEMO to OUTC and RPSR without aggregating, so a case with 2 outcome and 3 report-source codes became 6 rows. `silver_cases` had 470,891 rows for 397,224 cases. On 09-28 this was accepted as a tradeoff; on 09-29 `V1` failed and the function's own comment ("We aggregate... to avoid fan-out") showed the aggregation was simply missing (Issue 14).
**Decision:** Aggregate `outc_cod` and `rpsr_cod` per `primaryid` (`collect_set` + `concat_ws`) before the join. `build_gold_outcomes()` explodes the lists back out, so `gold_outcomes` and the Case Outcomes page are unchanged.
**Effect:** `silver_cases` is one row per case. `outc_cod`/`rpsr_cod` are now comma-separated; any direct consumer of `silver_cases` must handle that.
**Rejected:** Keeping the fan-out (it was the bug, not a tradeoff); a data-quality flag column (no simpler than the fix).
**Evidence:** 470,891 → 397,224 (73,667 rows, 15.6%). `06_validate_fix.py` 10/10 pass, `V1` unmodified. `gold_signals` identical to pre-fix (373,012 pairs); `gold_demographics` (34) and `gold_outcomes` (11) unchanged.

## D8 — Exclusive Pairs in a Separate Dashboard Panel

**Problem:** After D2, exclusive pairs carry Haldane-corrected PRRs that are not interpretable magnitudes (e.g. 210,279) and would fill every top slot of the Signal Leaderboard (FR-21).
**Decision:** Show exclusive pairs in a separate "Exclusive reports" panel, never in the main leaderboard. No PRR shown for them.
**Amended 2026-09-30:** The first version filtered the panel on `haldane_applied`, which would have shown 159 `SIGNAL` pairs, not 106 (Issue 22). The panel filter is now `is_exclusive` (`case_count = event_total`).

| signal_flag | haldane_applied | is_exclusive | pairs |
|---|---|---|---|
| NO_SIGNAL | false | false | 315,990 |
| NO_SIGNAL | true | false | 4,166 |
| NO_SIGNAL | true | true | 2,872 |
| SIGNAL | false | false | 49,825 |
| SIGNAL | true | false | 53 |
| SIGNAL | true | true | 106 |

Check: exclusive 2,872 + 106 = 2,978 (the pre-fix null-PRR count); all `haldane_applied` = 7,197. Saved as P4 in `sql/case_review_rescued_signals.sql`; re-run 2026-10-06 gave the same six rows.

- **Exclusive panel:** `is_exclusive`, 106 `SIGNAL` pairs. The uninterpretable PRR comes from `c = 0`.
- **Main leaderboard:** `SIGNAL` and not `is_exclusive`: 49,878 (49,825 + 53). The 53 non-exclusive Haldane pairs (`b = 0`) get only a minor correction; `haldane_applied` shows as a tooltip flag.
- **Implementation:** Power BI derives `is_exclusive` from `case_count` and `event_total`. No pipeline change. BRD v2.3 FR-21.

**Rejected:** One leaderboard sorted by PRR or χ² (exclusive pairs dominate); filtering on `haldane_applied` under the name "Exclusive" (mislabels 53 pairs).

## D9 — Defer Patient-Level (Report) Deduplication

**Problem:** The case review of the top 10 rescued signals showed `case_count` counts reports, not patients: literature cases submitted by several manufacturers, related reports filed together, follow-ups under new case IDs. D4 cannot detect these (Issue 21).
**Decision:** Keep `caseid` dedup (D4), document the limitation, no fingerprint-based dedup in this release.
**Effect:** `case_count` may overstate patients. The case review (`sql/case_review_rescued_signals.sql`, P1–P3) is a manual check for top signals, not a pipeline stage.
**Rejected:** Fingerprint dedup now: needs gold-standard validation and would block the dashboard.

## D10 — Per-Stage Timing Log: UTC, `perf_counter`, In-Memory Only

**Problem:** The `99_run_pipeline.py` summary mixed naive local times across cells and used `.seconds`, so a partial rerun could print a finish before the start, with no per-stage breakdown (Issues 24, 25).
**Decision:** One in-memory `stage_log` entry per stage (`stage`, `start_utc`, `end_utc`, `duration_s`, `status`). UTC ISO 8601 timestamps, `time.perf_counter()` durations, summary built only from that run's log. A failed stage is logged `FAILED`. Nothing is persisted.
**Effect:** Timing shows in notebook output only; copy runtimes into PROJECT-LOG by hand. `%run` cannot be wrapped in `try/except`, so a `FAILED` entry is written on the next `stage_begin` or summary, with that later time as its end.
**Deferred:** Writing timing rows to a Delta table or Volume file, until run history across quarters is needed.
**Evidence:** Databricks run 2026-10-06: five stages OK, total 2m 39.2s. Only the OK path was exercised.

## D11 — Top 10 Reporter Countries: Exclude `EU` and `COUNTRY NOT SPECIFIED` in the Report Layer

**Problem:** `gold_demographics` keeps the top 20 `reporter_country` values as FAERS reports them. In 2026Q1 rank 2 is `EU` (40,633 reports), a regional code and not a country, and rank 7 is `COUNTRY NOT SPECIFIED` (4,012), a missing-data bucket. An unfiltered "Top 10 reporter countries" chart would show both as countries. Neither BRD v2.5 (FR-18, FR-20) nor ARCHITECTURE.md says how to treat them.
**Decision:** Exclude both values from the Top 10 chart in Power BI only. Derive `is_country` on `gold_demographics` (`NOT IN { "EU", "COUNTRY NOT SPECIFIED" }`), the same pattern as `is_exclusive` (D8). Show the excluded counts in a footnote under the chart. No pipeline change.
**Effect:** The chart lists the real top 10 countries (US, CA, GB, JP, CN, AU, IN, CO, BR, KR on 2026Q1). `gold_demographics.csv` and all verified counts are unchanged. The pipeline already keeps 20 country rows, so the chart still has at least 10 countries after the exclusion.
**Rejected:** Leaving the chart unfiltered (shows non-countries as countries); filtering in the pipeline (changes verified Gold outputs and forces a re-run and re-validation for a display issue); dropping the rows without a footnote (hides data without saying so).
**Check:** The chart shows 10 bars, US first at 268,928, with no `EU` or `COUNTRY NOT SPECIFIED`. The footnote shows 40,633 and 4,012. Update the footnote when a new quarter is loaded.
