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
| D12 | No drug filter on Patient Demographics; optional Drug Name slicer on the leaderboard only; no cross-filtering between same-table visuals | 2026-10-14 | Accepted |
| D13 | Leaderboard ranked by report count (PRR breaks ties); top 10 by a `Rank` measure filter, no helper column | 2026-10-21 | Accepted |
| D14 | Power BI measures that feed a card, measure, MIN or filter stay numeric; card Display units set per field; `FORMAT` only in text label measures | 2026-10-21 | Accepted |
| D15 | Leaderboard shows the Haldane flag as an `H` column, not a tooltip (deviation from FR-21) | 2026-10-21 | Accepted |
| D16 | Keep the `UNKNOWN` age and sex categories visible on Patient Demographics, sorted last; state the denominator in the caption | 2026-10-22 | Accepted |

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

## D12 — No Drug Filter on Patient Demographics; Slicer on the Leaderboard Only

**Problem:** ARCHITECTURE.md listed "Filter: Drug Name" on Patient Demographics ("demographics for a specific drug's cases") and "Filters: Signal Flag, Drug Name" on the leaderboard. BRD v2.5 (FR-20, FR-21) requires neither. `gold_demographics` and `gold_outcomes` have no drug dimension and the model has no relationships, so a drug filter cannot work on those pages. A Signal Flag filter on the leaderboard has no effect, because the leaderboard is fixed to `SIGNAL` (FR-21). Separately, visuals that read different rows of the same table blank each other when one is clicked (for example, clicking ADULT filters the sex donut to rows where `dimension_value = ADULT`).
**Decision:** No drug filter on Patient Demographics or Case Outcomes; drug-level demographics is out of scope. An optional Drug Name slicer goes on the Signal Leaderboard only, where `gold_signals` is at drug × reaction grain. No Signal Flag filter. Visuals on Demographics and Outcomes do not cross-filter each other (Edit interactions: None).
**Effect:** ARCHITECTURE.md matches the BRD and the data model. No pipeline or Gold change.
**Rejected:** A new Gold table at drug × dimension grain (pipeline change, re-run and re-validation of verified outputs for a feature nobody required); leaving cross-filtering on (clicking a bar blanks the other charts).
**Check:** On Demographics and Outcomes, clicking any bar leaves every other visual populated. On the leaderboard, typing a drug name filters both the leaderboard and the exclusive panel.

## D13 — Leaderboard Ranked by Report Count; Top 10 by a `Rank` Measure Filter

**Problem:** The main leaderboard needs a rank column and a top-10 view. Ranked by PRR, the first Power BI render put pairs with 3 to 7 reports on top: 7 of the 10 rows had 7 reports or fewer, four had exactly 3 (the D3 minimum), and three were Haldane-corrected (D2), with PRRs of 53,000 to 77,000 (the top 10 ran from 53,000 to 149,000). FR-21 already says Haldane-corrected PRR is not an interpretable magnitude, and D3 says PRR alone is not enough. The first top-10 attempt also needed a `Pair` helper column (drug and reaction joined), because a Top N filter ranks one field and `drugname` or `pt` alone returns the top 10 drugs or reactions with all their rows (Issue 31).
**Decision:** Rank by `case_count` descending, with `prr` breaking ties, using a `Rank` measure (`RANKX` over `ALLSELECTED ( gold_signals )`). Show it as the first column (`#`) and keep the top 10 with a visual-level filter `Rank <= 10`. PRR and ROR stay as columns beside it. The subtitle reads "Ranked by report count." No `Pair` column. The measure is in the build steps, section 4.
**Effect:** The 2026Q1 top 10 is Dupixent, Tymlos and Paragard T 380A pairs with 2,270 to 5,080 reports, where PRR and ROR are stable. No pipeline, Gold or verified-count change. With the Drug Name slicer (D12) the rank restarts at 1 within the chosen drug. The KPI cards are not affected (interactions set to None, Issue 30). `case_count` counts reports, not patients (D9), and some top pairs reflect a drug's indication, not an adverse reaction (for example Dupixent and dermatitis atopic); both are footnoted in the report.
**Rejected:** Ranking by PRR (puts unstable 3-report pairs on top and gives the Haldane-corrected PRR a prominence FR-21 says it should not have); a higher minimum such as `case_count >= 10` on the visual (a second threshold beyond Evans that needs its own justification, and still ranks by PRR); a Top N filter on `drugname` or `pt` (keeps top 10 drugs or reactions, not 10 rows); a `Pair` calculated column (a text column over 373,012 rows for one filter; superseded by Issue 32).
**Check:** The table shows ranks 1 to 10 with no gaps or repeats, sorted by `#` ascending, `Reports` descending. Clicking a row leaves the six KPI cards at 397,209 / 373,012 / 49,984 / 49,878 / 106 / 7,197. Applying the `Rank <= 10` filter before anything else matters: without it `RANKX` runs across about 49,878 rows and is slow.

## D14 — Measures Stay Numeric; Card Display Units Set Per Field; FORMAT Only in Text Label Measures

**Problem:** `Cases` was a `FORMAT` text measure (Issue 26) while the other KPI cards used numeric measures with Display units set per field (Issue 34). That left two patterns. A text measure cannot feed `MIN`, `DIVIDE` or a filter: `Total Pairs` and `Haldane Pairs` were text, so `Haldane %` divided two text values, and the `FORMAT` wrapper on `Haldane %` caused the "Cannot convert value '' of type Text to type Number" error (Issue 29).
**Decision:** Every measure that feeds a card, another measure, `MIN` or a filter stays numeric, `Cases` included. Set the number format in Measure tools (Whole number, or Percentage with 2 decimals for `Haldane %`) and set each card's Display units per field (Custom, Format code `#,0`) so counts are not abbreviated. `FORMAT` is allowed only in text label measures that nothing else consumes (`Preview Note Measure`, `Subtitle Haldane Pairs`); label measures live in table `_TEXT`.
**Effect:** No pipeline, Gold or verified-count change. Supersedes the `FORMAT` fix for `Cases` in Issue 26 (addendum added there). The six KPI cards read 397,209 / 373,012 / 49,984 / 49,878 / 106 / 7,197 in full, and the Haldane label reads "1.93% of pairs".
**Rejected:** `FORMAT` on `Cases` only (a second pattern, and a text measure that cannot be reused as N in a calculation); `FORMAT` on every card measure (text breaks `MIN`, `DIVIDE` and filters); leaving Display units on Auto (shows "50K" and "373K").
**Check:** Each card shows its full count. The `Haldane %` card shows 1.93% with no error. Searching the `.tmdl` files for `FORMAT` finds only label measures.

## D15 — Haldane Flag Shown as an `H` Column on the Leaderboard, Not a Tooltip

**Problem:** BRD v2.5 FR-21 says `haldane_applied` appears on the main leaderboard as a tooltip flag. The build steps (section 6) give a fallback if the tooltip does not work on the table visual.
**Decision:** Add a calculated column `Haldane flag = IF ( gold_signals[haldane_applied], "H", "" )` to `gold_signals`, shown in a narrow column with a blank header between Drug and Reaction. The visual subtitle says H marks a pair where the Haldane correction was applied.
**Effect:** No pipeline or Gold change. Verified counts are unchanged. On 2026Q1 no top-10 row is Haldane-corrected under D13 (the corrected pairs are the exclusive ones, which the leaderboard filters out), so the column is blank there. The flag still shows for any corrected pair a drug slicer or a new quarter brings in. This deviates from FR-21 (tooltip); BRD wording is unchanged.
**Rejected:** Leaving the flag out (hides which PRRs are Haldane-corrected, FR-21); a report-page tooltip (a second page to maintain for one flag).
**Check:** With a temporary visual filter `haldane_applied = TRUE`, the top 10 rows each show H. Remove the filter and the table returns to the D13 top 10 with a blank column.

## D16 — Keep "Unknown" Age and Sex Visible on Patient Demographics, Sorted Last

**Problem:** `UNKNOWN` is the largest age bar (151,609 reports, 38.2% of 397,224) and about a fifth of the sex donut (75,433, 19.0%). Dropping it would make the real groups stand out, but it would also overstate them: Adult is 33.1% of all reports and 53.5% of reports with a known age. The bars would no longer sum to the Silver case count.
**Decision:** Keep `UNKNOWN` on both charts. It sorts last (`label_order` 90 in the age chart and the sex donut, so `dimension_label` "Unknown" has one order value) and is drawn in grey so it reads as missing data, not as a group. Each chart carries a caption in its own subtitle that gives the missing share and the share among known values (age: 38.2% missing, 53.5% adult and 39.6% elderly among known ages; sex: 19.0% missing, 61.6% female among known sexes). The tooltip on the age chart shows each bar's share of all reports.
**Effect:** No pipeline or Gold change. Age bars sum to 397,224 (397,209 analysed plus 15 reports with no primary-suspect drug), and so do the sex slices (check 13 in the build steps). `UNKNOWN` means the age or sex is missing or unusable, not that the patient is unknown. Different from D11 on purpose: `EU` and `COUNTRY NOT SPECIFIED` are left out of a ranked Top 10 countries chart because a ranking of places cannot include a missing-data bucket, and the report footnote states the excluded counts. An age or sex chart is a distribution of all reports, so its missing bucket belongs in it.
**Rejected:** Dropping `UNKNOWN` (bars no longer reconcile; every share is inflated); showing only shares of known values (hides how much is missing); folding it into another group.
**Check:** Age bars sum to 397,224 and so do the sex slices. The captions read 38.2% and 19.0% missing. `Unmapped Labels` is 0. See DEVELOPER.md Issue 39.
