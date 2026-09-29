# DECISIONS.md — PharmaSignalLake

Architecture and correctness decisions for the project, recorded as they are made.
Each entry is Context (the problem), Decision (what was chosen), Consequences (what
that implies downstream), and Alternatives rejected (what else was considered and why
it lost).

---

## D1 — Binary Signal Classification

**Date:** 2026-09-28
**Status:** Accepted

**Context:** The original `gold_signals` classification used three tiers —
`SIGNAL`, `WEAK_SIGNAL`, `NO_SIGNAL` — with `WEAK_SIGNAL` triggered by
`PRR >= 1.5` (`WEAK_SIGNAL_PRR`). There is no literature basis for 1.5 as a
meaningful intermediate pharmacovigilance threshold; it invites treating an
under-powered PRR as if it were a real signal.

**Decision:** Remove `WEAK_SIGNAL` entirely — from `03_gold_signals.py`,
`00_config.py` (`WEAK_SIGNAL_PRR` deleted), and all documentation.
Classification is binary: `SIGNAL` or `NO_SIGNAL`, driven by full Evans
Criteria (see D3).

**Consequences:** Simplifies every downstream consumer (validation queries,
Power BI signal-flag filters). Any pair that previously reported
`WEAK_SIGNAL` now reports `NO_SIGNAL` unless it independently clears the
full Evans thresholds.

**Alternatives rejected:** Keeping the 1.5 PRR "weak" tier as an early-warning
signal — rejected for having no literature basis and for the risk of
over-signaling from an arbitrary threshold.

**Confirmed on 2026Q1 (2026-09-28):** `05_profile.py` (`run_label="after"`)
shows only `gold_signal_flag_NO_SIGNAL` (323,028) and
`gold_signal_flag_SIGNAL` (49,984) — no `WEAK_SIGNAL` key at all.

**BRD sync (2026-09-29):** `BRD.md` v2.1 updated — Section 5 Scope and the
Evans Criteria glossary entry now describe binary `SIGNAL`/`NO_SIGNAL`
classification, replacing the stale "signal, weak signal, or no signal"
wording. See `DEVELOPER.md` Issue 13.

---

## D2 — Haldane Correction for Zero Cells

**Date:** 2026-09-28
**Status:** Accepted

**Context:** When any of the four 2x2 contingency-table cells (`a`, `b`, `c`,
`d`) is zero — most commonly `c = 0`, an exclusive drug-event pair where
`case_count == event_total` — PRR/ROR are undefined. The original code left
these as `NULL`, which meant the most clinically interesting, most exclusive
pairs were silently excluded from signal detection. Logged in `DEVELOPER.md`
in Step 7's documentation sweep (the exact issue number depends on what else
is logged before then — see the numbering note in project memory).

**Decision:** Apply the Haldane-Anscombe correction — add 0.5 to all four
cells — whenever any cell is 0, before computing PRR/ROR only. Record this
with a `haldane_applied` boolean column on `gold_signals`. `chi_sq` is
computed on the raw, uncorrected cells (see D3), not the Haldane-adjusted
ones.

**Consequences:** Every pair gets a finite PRR/ROR. Corrected ratios are
conservative (pulled toward 1.0) rather than infinite or null, so exclusive
pairs can now actually be evaluated against the Evans thresholds instead of
falling out of the classification entirely.

**Alternatives rejected:** Flagging zero-cell pairs with a categorical label
like `EXCLUSIVE_REPORT` instead of computing a corrected ratio — rejected
because it still leaves no quantitative signal strength for exactly the
pairs Haldane exists to handle.

**Confirmed on 2026Q1 (2026-09-28):** `gold_null_prr` and `gold_null_ror`
both dropped from 2,978 / 7,197 (baseline) to 0 (after). `haldane_applied`
is `true` for exactly 7,197 pairs, matching the baseline's null-ROR count
and the profile's `gold_pairs_with_zero_cell` count exactly. Of those, 106
pairs have `case_count >= 3` (`gold_null_prr_case_count_ge_min` was >0
before the fix, now 0) — these are pairs that were previously structurally
incapable of ever being flagged `SIGNAL`, since a null PRR always fails a
`>=` comparison, and can now be evaluated properly.

**BRD sync (2026-09-29):** `BRD.md` v2.1 FR-14/FR-15 updated to document
this correction — they previously omitted it entirely even though it
changes both the PRR and ROR formulas whenever a cell is zero. See
`DEVELOPER.md` Issue 13.

---

## D3 — Full Evans Criteria (n ≥ 3 AND PRR ≥ 2 AND χ² (Yates) ≥ 4)

**Date:** 2026-09-28
**Status:** Accepted

**Context:** The original classification used only `case_count` and `PRR`
thresholds, omitting the chi-squared disproportionality test that full Evans
Criteria requires. PRR alone can flag noise from very small samples.

**Decision:** `signal_flag = SIGNAL` only when `case_count >= EVANS_MIN_CASES`
(3) **and** `prr >= EVANS_MIN_PRR` (2.0) **and** `chi_sq >= EVANS_MIN_CHI2`
(4.0, Yates-corrected); otherwise `NO_SIGNAL`. `chi_sq` is computed on the
raw, uncorrected 2x2 cells so the statistical test isn't distorted by the
Haldane correction (D2). `case_count` stays the raw `A` count. All three
thresholds live in `00_config.py`.

**Consequences:** Some pairs with a high PRR but an insufficient chi-squared
value (typically very small samples) no longer classify as `SIGNAL`. This is
the intended, stricter behavior of full Evans Criteria versus PRR-only
screening.

**Alternatives rejected:** PRR-only classification (the prior behavior) —
rejected as statistically incomplete.

**Confirmed on 2026Q1 (2026-09-28):** `SIGNAL` count is 49,984 after the
fix, down from the old combined `SIGNAL` (56,491) + `WEAK_SIGNAL` (5,562) =
62,053 — even after also rescuing the 106 previously-null-PRR exclusive
pairs (D2). Net effect: the added `chi_sq >= 4.0` gate removes more
low-confidence pairs than the Haldane correction adds back.

**BRD sync (2026-09-29):** `BRD.md` v2.1 FR-16 and the Evans Criteria
glossary entry updated to require chi-squared (Yates) ≥ 4 in addition to
case count and PRR, replacing the stale PRR-only wording. See
`DEVELOPER.md` Issue 13.

---

## D4 — Deduplicate on `caseid`, Not `primaryid`

**Date:** 2026-09-28
**Status:** Accepted

**Context:** `primaryid = caseid + caseversion`, so it is already unique per
row in Bronze `DEMO` — a dedup window partitioned by `primaryid` is a no-op
(see Issue 9 in `DEVELOPER.md`). The correct entity key for "one row per
case" is `caseid`, keeping the highest `caseversion`.

**Decision:** Deduplicate DEMO by partitioning on `caseid`, ordering by
`caseversion` (parsed with `try_cast(... AS INT)` for ANSI safety,
`desc_nulls_last()`), keeping the top row per `caseid`. Every downstream
child table (`DRUG`, `REAC`, `OUTC`, `RPSR`, `INDI`, `THER`) is `left_semi`
joined against the surviving `primaryid`s so rows from superseded case
versions never reach Silver.

**Consequences:** For a quarter where every `caseid` has exactly one
`caseversion` — as observed in the 2026Q1 baseline (distinct `primaryid` ==
distinct `caseid` == 397,224, 0 superseded) — this fix has zero numeric
impact. It matters for quarters where reporters actually submitted case
updates.

**Alternatives rejected:** None — the original dedup key was a latent bug,
not an intentional tradeoff between options.

**Confirmed on 2026Q1 (2026-09-28):** re-ran after the fix — still
397,224 → 397,224, 0 superseded, identical to baseline. Zero numeric impact
this quarter as predicted; the fix is validated structurally (Issue 9,
Issue 12) but not yet exercised by a quarter with an actual case update.

**BRD sync (2026-09-29):** `BRD.md` v2.1 FR-06 updated to specify dedup on
`caseid` with child-table `primaryid` filtering, replacing the stale
`primaryid`-based wording. See `DEVELOPER.md` Issue 13.

---

## D5 — ANSI Mode Stays ON in Gold

**Date:** 2026-09-28
**Status:** Accepted

**Context:** Issue 7 (`DEVELOPER.md`) disabled `spark.sql.ansi.enabled` to
stop `DIVIDE_BY_ZERO` errors from the PRR/ROR division guards. That masked
the actual bug: at least one guard (the ROR guard) allowed a zero
denominator (`C − A = 0`) through instead of catching it.

**Decision:** Remove the `spark.conf.set("spark.sql.ansi.enabled", "false")`
override in `03_gold_signals.py`. Division safety now comes from the math
itself — the Haldane correction (D2) guarantees all four cells are `> 0`
before any division happens, so no guard can let a zero denominator through.

**Consequences:** If ANSI still raises a divide-by-zero error after this
fix, that indicates a real guard bug that must be found and corrected — not
a reason to re-disable ANSI.

**Alternatives rejected:** Keeping ANSI disabled permanently — rejected
because it hides real guard bugs (as it did for Issue 7) instead of fixing
them.

---

## D6 — `sql/verify_signal_fix.sql` Is Read-Only for the Assistant

**Date:** 2026-09-28
**Status:** Accepted

**Context:** An independent verification script is only trustworthy as a
check on the pipeline's logic if it stays independent of the agent that
wrote that logic. If the same session edits both the pipeline and its
verifier, a bug in one and a matching bug in the other could both pass
silently, and the check would prove nothing.

**Decision:** `sql/verify_signal_fix.sql` is written and maintained by Will
alone, outside this session. Claude may read it to understand what will be
verified, but must never edit, move, or rename it. If a check in it appears
wrong, stop and explain why rather than changing it.

**Consequences:** Verification results stay independent of the agent under
test. A suspected problem in the verifier surfaces as a conversation with
Will, never as a silent edit.

**Alternatives rejected:** Letting the same session maintain both the
pipeline and its verifier — rejected because it collapses the independence
the verification step exists to provide.

---

## D7 — Aggregate OUTC/RPSR Per Case Instead of Keeping the Fan-Out

**Date:** 2026-09-29
**Status:** Accepted (supersedes the 2026-09-28 "option A: keep the grain" call)

**Context:** `build_silver_cases()` (`02_silver_clean.py`) joins DEMO with OUTC
and RPSR, both of which can have multiple rows per `primaryid` (multiple
outcomes, multiple report sources). The join was never aggregated, so a case
with 2 outcome codes and 3 report-source codes produced 2x2=6 rows instead
of 1 — `silver_cases` had 470,891 rows for only 397,224 distinct cases on
2026Q1. On 2026-09-28, this was already known (surfaced via profiling) and
the team explicitly decided to accept it ("option A: keep the grain") rather
than fix it, scoping `06_validate_fix.py`'s `V1` check to dedup-level
uniqueness instead of `silver_cases`' row count. Re-examining it on
2026-09-29 (triggered by `V1` failing when it was written to check the
originally-specified "rows == distinct caseid == distinct primaryid") found
that `build_silver_cases()`'s own comment already claimed *"We aggregate to
a comma-separated list to avoid fan-out on the join"* — the aggregation
code just didn't exist. This isn't an accepted design tradeoff; it's the
implementation not doing what it already claimed to do.

**Decision:** Aggregate `outc_cod` and `rpsr_cod` per `primaryid` into a
comma-separated list (`collect_set` + `concat_ws`) before joining to DEMO,
so `silver_cases` becomes one row per case. `build_gold_outcomes()`
(`03_gold_signals.py`) explodes each list back into one row per (case, code)
before its existing per-code `groupBy`/`countDistinct`, so its output
(`gold_outcomes.csv`, and the Power BI Case Outcomes page it feeds) is
numerically unchanged.

**Consequences:** `silver_cases` now satisfies `rows == distinct caseid ==
distinct primaryid`, so `06_validate_fix.py`'s `V1` needs no special-casing
— it passes as originally specified. `silver_cases.outc_cod`/`rpsr_cod` are
now multi-valued (comma-separated) columns rather than single codes; any
future direct consumer of `silver_cases` (not routed through
`gold_outcomes`) must account for this.

**Alternatives rejected:** Keeping the fan-out (the original 2026-09-28
decision) — reversed because it wasn't actually an intentional tradeoff, it
was this bug going undiagnosed. Flagging the fan-out with a data-quality
column instead of fixing it — rejected because the fix is no more complex
than the flag would have been, and `build_gold_outcomes()` already needed
to keep working, not just be told about the problem.
