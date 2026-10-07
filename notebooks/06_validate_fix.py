# Databricks notebook source
# MAGIC %md
# MAGIC # 06_validate_fix — Signal Correctness Fix Validation
# MAGIC **Project:** PharmaSignalLake
# MAGIC **Purpose:** Independently verify the signal-correctness fix (Steps 1-3: dedup on
# MAGIC `caseid`, full Evans Criteria, Haldane correction, binary `signal_flag`) against the
# MAGIC current `gold_signals` table, then report before/after impact against the baseline
# MAGIC snapshotted by `05_profile` (`run_label="baseline"`).
# MAGIC
# MAGIC **Read-only tooling.** This notebook only READS pipeline tables and the `_`-prefixed
# MAGIC validation tables. It writes nothing and is NOT part of `99_run_pipeline`.
# MAGIC
# MAGIC **Checks V1-V10** each print PASS/FAIL with the observed value. If any check fails,
# MAGIC the notebook raises after the full report below has printed, so a failure is always
# MAGIC diagnosed with the complete before/after picture in front of it, not just the bare
# MAGIC failing check.
# MAGIC
# MAGIC | Check | What it verifies |
# MAGIC |-------|-------------------|
# MAGIC | V1 | `silver_cases` rows == distinct `caseid` == distinct `primaryid` |
# MAGIC | V2 | Every `primaryid` in `silver_drug_reactions` / `silver_therapy` exists in `silver_cases` |
# MAGIC | V3 | `signal_flag` is always `SIGNAL` or `NO_SIGNAL` — never null, never a third value |
# MAGIC | V4 | Every `SIGNAL` row satisfies `case_count>=3 AND prr>=2 AND chi_sq>=4` |
# MAGIC | V5 | No `NO_SIGNAL` row secretly satisfies all three `SIGNAL` criteria |
# MAGIC | V6 | No null `prr` where `case_count >= EVANS_MIN_CASES` |
# MAGIC | V7 | `haldane_applied` is true exactly when some raw 2x2 cell is 0 |
# MAGIC | V8 | `total_reports` (N) matches distinct `primaryid` in `silver_drug_reactions` |
# MAGIC | V9 | Independent recompute of PRR/ROR/chi_sq in plain Spark SQL (not `compute_signals()`) — no pair differs by more than 0.1% relative |
# MAGIC | V10 | No row has `case_count > drug_total` or `case_count > event_total` |

# COMMAND ----------

# Load configuration
# MAGIC %run ./00_config

# COMMAND ----------
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    count,
    countDistinct,
    lit,
    when,
)
from pyspark.sql.types import LongType

# Tolerance for V9's independent recompute vs. the pipeline's compute_signals()
# output. Both round to 4 decimals, so this only needs to absorb floating-point
# ordering differences between the two independent implementations — not a
# pipeline threshold, so it stays local to this notebook rather than living in
# 00_config.py alongside EVANS_MIN_*.
V9_RELATIVE_TOLERANCE = 0.001

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helpers: Read Tables

# COMMAND ----------

def read_quarter(path: str) -> DataFrame:
    """Read a Delta table and filter it to the configured quarter."""
    return (
        spark.read.format("delta").load(path)
        .filter(col("source_quarter") == QUARTER)
    )


def assert_nonzero(df: DataFrame, name: str) -> DataFrame:
    """Raise a descriptive error if `df` has zero rows for the configured quarter."""
    n = df.count()
    if n == 0:
        raise RuntimeError(f"{name} has zero rows for quarter {QUARTER} — cannot validate.")
    return df

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Tables

# COMMAND ----------

print(f"  Loading tables for quarter {QUARTER}...")

cases    = assert_nonzero(read_quarter(SILVER["cases"]), "silver_cases")
dr       = assert_nonzero(read_quarter(SILVER["drug_reactions"]), "silver_drug_reactions")
therapy  = assert_nonzero(read_quarter(SILVER["therapy"]), "silver_therapy")
gold     = assert_nonzero(read_quarter(GOLD["signals"]), "gold_signals")
baseline = assert_nonzero(read_quarter(VALIDATION["gold_signals_baseline"]), "_gold_signals_baseline")

print(f"  silver_cases           : {cases.count():,} rows")
print(f"  silver_drug_reactions  : {dr.count():,} rows")
print(f"  silver_therapy         : {therapy.count():,} rows")
print(f"  gold_signals           : {gold.count():,} rows")
print(f"  _gold_signals_baseline : {baseline.count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## V1-V10 Check Functions
# MAGIC Each returns `{"id", "description", "passed", "observed"}`.

# COMMAND ----------

def check_v1_silver_cases_dedup(cases: DataFrame) -> dict:
    """silver_cases rows == distinct caseid == distinct primaryid."""
    rows               = cases.count()
    distinct_caseid    = cases.select(countDistinct("caseid")).first()[0]
    distinct_primaryid = cases.select(countDistinct("primaryid")).first()[0]
    passed = rows == distinct_caseid == distinct_primaryid
    return {
        "id": "V1",
        "description": "silver_cases rows == distinct caseid == distinct primaryid",
        "passed": passed,
        "observed": f"rows={rows:,} distinct_caseid={distinct_caseid:,} distinct_primaryid={distinct_primaryid:,}",
    }


def count_orphan_primaryid(child: DataFrame, parent: DataFrame) -> int:
    """Rows of `child` whose primaryid does not exist in `parent`."""
    return child.join(parent.select("primaryid").distinct(), on="primaryid", how="left_anti").count()


def check_v2_no_orphan_primaryid(cases: DataFrame, dr: DataFrame, therapy: DataFrame) -> dict:
    """Every primaryid in silver_drug_reactions/silver_therapy exists in silver_cases."""
    orphan_dr      = count_orphan_primaryid(dr, cases)
    orphan_therapy = count_orphan_primaryid(therapy, cases)
    passed = (orphan_dr == 0) and (orphan_therapy == 0)
    return {
        "id": "V2",
        "description": "every primaryid in silver_drug_reactions/silver_therapy exists in silver_cases",
        "passed": passed,
        "observed": f"orphan_drug_reactions={orphan_dr:,} orphan_therapy={orphan_therapy:,}",
    }


def check_v3_signal_flag_domain(gold: DataFrame) -> dict:
    """signal_flag is always SIGNAL or NO_SIGNAL, never null or a third value."""
    bad = gold.filter(
        col("signal_flag").isNull() | ~col("signal_flag").isin("SIGNAL", "NO_SIGNAL")
    ).count()
    passed = bad == 0
    return {
        "id": "V3",
        "description": "signal_flag in {SIGNAL, NO_SIGNAL}, no nulls",
        "passed": passed,
        "observed": f"violating_rows={bad:,}",
    }


def check_v4_signal_rows_meet_criteria(gold: DataFrame) -> dict:
    """Every SIGNAL row satisfies case_count>=min AND prr>=min AND chi_sq>=min."""
    violating = gold.filter(
        (col("signal_flag") == "SIGNAL") &
        ~(
            (col("case_count") >= EVANS_MIN_CASES) &
            (col("prr") >= EVANS_MIN_PRR) &
            (col("chi_sq") >= EVANS_MIN_CHI2)
        )
    ).count()
    passed = violating == 0
    return {
        "id": "V4",
        "description": f"every SIGNAL row: case_count>={EVANS_MIN_CASES} AND prr>={EVANS_MIN_PRR} AND chi_sq>={EVANS_MIN_CHI2}",
        "passed": passed,
        "observed": f"violating_rows={violating:,}",
    }


def check_v5_no_signal_rows_dont_qualify(gold: DataFrame) -> dict:
    """No NO_SIGNAL row secretly satisfies all three SIGNAL criteria."""
    violating = gold.filter(
        (col("signal_flag") == "NO_SIGNAL") &
        (col("case_count") >= EVANS_MIN_CASES) &
        (col("prr") >= EVANS_MIN_PRR) &
        (col("chi_sq") >= EVANS_MIN_CHI2)
    ).count()
    passed = violating == 0
    return {
        "id": "V5",
        "description": "no NO_SIGNAL row meets all three SIGNAL criteria",
        "passed": passed,
        "observed": f"violating_rows={violating:,}",
    }


def check_v6_no_null_prr_above_min_cases(gold: DataFrame) -> dict:
    """No null prr where case_count >= EVANS_MIN_CASES."""
    bad = gold.filter((col("case_count") >= EVANS_MIN_CASES) & col("prr").isNull()).count()
    passed = bad == 0
    return {
        "id": "V6",
        "description": f"no null prr where case_count >= {EVANS_MIN_CASES}",
        "passed": passed,
        "observed": f"violating_rows={bad:,}",
    }


def any_zero_cell() -> "Column":
    """Boolean column: true when any raw 2x2 cell (a, b, c, d) is 0."""
    a = col("case_count").cast(LongType())
    b = col("drug_total").cast(LongType()) - a
    c = col("event_total").cast(LongType()) - a
    d = col("total_reports").cast(LongType()) - col("drug_total").cast(LongType()) - col("event_total").cast(LongType()) + a
    return (a == 0) | (b == 0) | (c == 0) | (d == 0)


def check_v7_haldane_matches_zero_cell(gold: DataFrame) -> dict:
    """haldane_applied is true exactly when some raw 2x2 cell is 0."""
    mismatched = gold.filter(col("haldane_applied") != any_zero_cell()).count()
    passed = mismatched == 0
    return {
        "id": "V7",
        "description": "haldane_applied is true exactly when some raw cell is 0",
        "passed": passed,
        "observed": f"mismatched_rows={mismatched:,}",
    }


def check_v8_total_reports_matches_n(gold: DataFrame, dr: DataFrame) -> dict:
    """total_reports == distinct primaryid in silver_drug_reactions."""
    distinct_values = [r["total_reports"] for r in gold.select("total_reports").distinct().collect()]
    n_dr = dr.select(countDistinct("primaryid")).first()[0]
    passed = (len(distinct_values) == 1) and (distinct_values[0] == n_dr)
    observed_totals = ", ".join(f"{v:,}" for v in distinct_values)
    return {
        "id": "V8",
        "description": "total_reports == distinct primaryid in silver_drug_reactions",
        "passed": passed,
        "observed": f"distinct_total_reports_values=[{observed_totals}] silver_dr_distinct_primaryid={n_dr:,}",
    }


def values_differ(a: str, b: str, tol: float = V9_RELATIVE_TOLERANCE) -> "Column":
    """True when two nullable double columns differ by more than `tol` relative."""
    ac, bc = col(a), col(b)
    both_null = ac.isNull() & bc.isNull()
    one_null  = ac.isNull() != bc.isNull()
    denom     = when(spark_abs(ac) > 0, spark_abs(ac)).otherwise(lit(1.0))
    rel_diff  = spark_abs(ac - bc) / denom
    return when(both_null, lit(False)).when(one_null, lit(True)).otherwise(rel_diff > tol)


def recompute_signals_plain_sql(dr: DataFrame, n_total: int) -> DataFrame:
    """
    Recompute PRR/ROR/chi_sq/haldane_applied from silver_drug_reactions using a
    hand-written Spark SQL query — deliberately NOT calling
    src/signal_math.py's compute_signals(). This is the independent check: if a
    bug exists in compute_signals(), a from-scratch reimplementation of the same
    published formulas should not reproduce it.
    """
    dr.select("primaryid", "drugname", "pt").createOrReplaceTempView("v9_silver_dr")

    return spark.sql(f"""
        WITH pair_counts AS (
            SELECT drugname, pt, COUNT(DISTINCT primaryid) AS pair_n
            FROM v9_silver_dr
            GROUP BY drugname, pt
        ),
        drug_totals AS (
            SELECT drugname, COUNT(DISTINCT primaryid) AS drug_n
            FROM v9_silver_dr
            GROUP BY drugname
        ),
        reaction_totals AS (
            SELECT pt, COUNT(DISTINCT primaryid) AS reaction_n
            FROM v9_silver_dr
            GROUP BY pt
        ),
        cells AS (
            -- Cast every raw cell to DOUBLE here, before any arithmetic on them.
            -- chi_sq's denominator multiplies four marginals together
            -- ((a+b)*(c+d)*(a+c)*(b+d)), which overflows BIGINT for an N this
            -- large (~4x10^5^4 far exceeds BIGINT's ~9.2x10^18 max) and trips
            -- ANSI's ARITHMETIC_OVERFLOW. DOUBLE arithmetic doesn't hit ANSI's
            -- overflow check (it saturates to Infinity instead of raising),
            -- which is exactly why compute_signals() (src/signal_math.py)
            -- casts pair_n/drug_n/reaction_n to DoubleType() before this same
            -- computation — this recompute now mirrors that (D5).
            SELECT
                p.drugname AS drugname,
                p.pt       AS pt,
                CAST(p.pair_n AS DOUBLE) AS raw_a,
                CAST(dt.drug_n - p.pair_n AS DOUBLE)     AS raw_b,
                CAST(rt.reaction_n - p.pair_n AS DOUBLE) AS raw_c,
                CAST({n_total} - dt.drug_n - rt.reaction_n + p.pair_n AS DOUBLE) AS raw_d
            FROM pair_counts p
            JOIN drug_totals dt     ON p.drugname = dt.drugname
            JOIN reaction_totals rt ON p.pt = rt.pt
        ),
        haldane AS (
            SELECT *,
                (raw_a = 0 OR raw_b = 0 OR raw_c = 0 OR raw_d = 0) AS recomputed_haldane_applied
            FROM cells
        ),
        adjusted AS (
            SELECT
                drugname, pt, raw_a, raw_b, raw_c, raw_d, recomputed_haldane_applied,
                CASE WHEN recomputed_haldane_applied THEN raw_a + 0.5 ELSE raw_a END AS adj_a,
                CASE WHEN recomputed_haldane_applied THEN raw_b + 0.5 ELSE raw_b END AS adj_b,
                CASE WHEN recomputed_haldane_applied THEN raw_c + 0.5 ELSE raw_c END AS adj_c,
                CASE WHEN recomputed_haldane_applied THEN raw_d + 0.5 ELSE raw_d END AS adj_d
            FROM haldane
        )
        SELECT
            drugname,
            pt,
            CAST(raw_a AS INT) AS case_count,
            recomputed_haldane_applied,
            ROUND((adj_a / (adj_a + adj_b)) / (adj_c / (adj_c + adj_d)), 4) AS recomputed_prr,
            ROUND((adj_a * adj_d) / (adj_b * adj_c), 4) AS recomputed_ror,
            CASE
                WHEN (raw_a + raw_b) > 0 AND (raw_c + raw_d) > 0 AND (raw_a + raw_c) > 0 AND (raw_b + raw_d) > 0
                THEN ROUND(
                    ({n_total} * POWER(ABS(raw_a * raw_d - raw_b * raw_c) - {n_total} / 2.0, 2))
                    / ((raw_a + raw_b) * (raw_c + raw_d) * (raw_a + raw_c) * (raw_b + raw_d)),
                    4
                )
                ELSE NULL
            END AS recomputed_chi_sq
        FROM adjusted
    """)


def check_v9_independent_recompute(dr: DataFrame, gold: DataFrame) -> dict:
    """Independent plain-SQL recompute of PRR/ROR/chi_sq — no pair differs beyond tolerance."""
    n_total    = dr.select(countDistinct("primaryid")).first()[0]
    recomputed = recompute_signals_plain_sql(dr, n_total)

    n_gold       = gold.count()
    n_recomputed = recomputed.count()

    compared = recomputed.join(
        gold.select("drugname", "pt", "prr", "ror", "chi_sq", "haldane_applied"),
        on=["drugname", "pt"],
        how="inner",
    )
    n_joined = compared.count()

    mismatches = (
        compared
        .withColumn("prr_mismatch",     values_differ("prr", "recomputed_prr"))
        .withColumn("ror_mismatch",     values_differ("ror", "recomputed_ror"))
        .withColumn("chi_sq_mismatch",  values_differ("chi_sq", "recomputed_chi_sq"))
        .withColumn("haldane_mismatch", col("haldane_applied") != col("recomputed_haldane_applied"))
        .filter(col("prr_mismatch") | col("ror_mismatch") | col("chi_sq_mismatch") | col("haldane_mismatch"))
    )
    mismatch_count = mismatches.count()

    passed = (n_gold == n_recomputed == n_joined) and (mismatch_count == 0)
    return {
        "id": "V9",
        "description": f"independent plain-SQL recompute matches gold_signals within {V9_RELATIVE_TOLERANCE:.1%} relative",
        "passed": passed,
        "observed": (
            f"gold_pairs={n_gold:,} recomputed_pairs={n_recomputed:,} "
            f"joined_pairs={n_joined:,} mismatched_pairs={mismatch_count:,}"
        ),
    }


def check_v10_case_count_bounds(gold: DataFrame) -> dict:
    """No row has case_count > drug_total or case_count > event_total."""
    violating = gold.filter(
        (col("case_count") > col("drug_total")) | (col("case_count") > col("event_total"))
    ).count()
    passed = violating == 0
    return {
        "id": "V10",
        "description": "no gold row with case_count > drug_total or case_count > event_total",
        "passed": passed,
        "observed": f"violating_rows={violating:,}",
    }

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run V1-V10

# COMMAND ----------

print("=" * 70)
print(f"  SIGNAL FIX VALIDATION — Quarter: {QUARTER}")
print("=" * 70)

checks = [
    check_v1_silver_cases_dedup(cases),
    check_v2_no_orphan_primaryid(cases, dr, therapy),
    check_v3_signal_flag_domain(gold),
    check_v4_signal_rows_meet_criteria(gold),
    check_v5_no_signal_rows_dont_qualify(gold),
    check_v6_no_null_prr_above_min_cases(gold),
    check_v7_haldane_matches_zero_cell(gold),
    check_v8_total_reports_matches_n(gold, dr),
    check_v9_independent_recompute(dr, gold),
    check_v10_case_count_bounds(gold),
]

for c in checks:
    icon = "PASS" if c["passed"] else "FAIL"
    print(f"\n[{c['id']}] {c['description']}")
    print(f"      {icon} — {c['observed']}")

failed_checks = [c["id"] for c in checks if not c["passed"]]

print("\n" + "=" * 70)
print(f"  VALIDATION SUMMARY: {len(checks) - len(failed_checks)}/{len(checks)} passed")
if failed_checks:
    print(f"  FAILED: {', '.join(failed_checks)}")
print("=" * 70)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Before / After — Transition Matrix and Top Movers
# MAGIC Compares `_gold_signals_baseline` (pre-fix, 3-tier `signal_flag`, no `chi_sq`/
# MAGIC `haldane_applied`) against the current `gold_signals` (post-fix, binary
# MAGIC `signal_flag`). Runs regardless of the V1-V10 result above, so a failure is
# MAGIC always diagnosed with full before/after context in front of it.

# COMMAND ----------

def build_comparison(baseline: DataFrame, gold: DataFrame) -> DataFrame:
    """Full outer join of baseline vs. current gold_signals on (drugname, pt)."""
    old = baseline.select(
        col("drugname"), col("pt"),
        col("signal_flag").alias("old_signal_flag"),
        col("case_count").alias("old_case_count"),
        col("prr").alias("old_prr"),
    )
    new = gold.select(
        col("drugname"), col("pt"),
        col("signal_flag").alias("new_signal_flag"),
        col("case_count").alias("new_case_count"),
        col("prr").alias("new_prr"),
        col("chi_sq").alias("new_chi_sq"),
    )
    return (
        old.join(new, on=["drugname", "pt"], how="full_outer")
        .withColumn("old_signal_flag", coalesce(col("old_signal_flag"), lit("ABSENT")))
        .withColumn("new_signal_flag", coalesce(col("new_signal_flag"), lit("ABSENT")))
    )


def build_transition_matrix(comparison: DataFrame) -> DataFrame:
    """Pair counts for every (old_signal_flag, new_signal_flag) combination."""
    return (
        comparison
        .groupBy("old_signal_flag", "new_signal_flag")
        .agg(count("*").alias("pair_count"))
        .orderBy("old_signal_flag", "new_signal_flag")
    )


def reason_became_signal(comparison: DataFrame) -> DataFrame:
    """Top 20 pairs that were not SIGNAL before but are SIGNAL now, with a reason."""
    return (
        comparison
        .filter((col("old_signal_flag") != "SIGNAL") & (col("new_signal_flag") == "SIGNAL"))
        .withColumn(
            "reason",
            when(col("old_signal_flag") == "ABSENT", "new pair (not present in baseline)")
            .when(col("old_prr").isNull(), "rescued by Haldane (null PRR before fix)")
            .when(col("old_signal_flag") == "WEAK_SIGNAL", "was WEAK_SIGNAL pre-fix, now clears full Evans")
            .otherwise("newly meets Evans thresholds after fix")
        )
        .orderBy(col("new_prr").desc_nulls_last())
        .limit(20)
        .select("drugname", "pt", "old_signal_flag", "old_prr", "new_signal_flag", "new_prr", "new_chi_sq", "reason")
    )


def reason_lost_signal(comparison: DataFrame) -> DataFrame:
    """Top 20 pairs that were SIGNAL before but are not SIGNAL now, with a reason."""
    return (
        comparison
        .filter((col("old_signal_flag") == "SIGNAL") & (col("new_signal_flag") != "SIGNAL"))
        .withColumn(
            "reason",
            when(col("new_signal_flag") == "ABSENT", "pair disappeared after fix (dedup/left-semi join change)")
            .when(col("new_case_count") < EVANS_MIN_CASES, "failed case count")
            .when(col("new_prr").isNull() | (col("new_prr") < EVANS_MIN_PRR), "failed PRR")
            .when(col("new_chi_sq").isNull() | (col("new_chi_sq") < EVANS_MIN_CHI2), "failed chi_sq (new full-Evans gate)")
            .otherwise("unknown regression — investigate")
        )
        .orderBy(col("old_prr").desc_nulls_last())
        .limit(20)
        .select("drugname", "pt", "old_signal_flag", "old_prr", "new_signal_flag", "new_case_count", "new_prr", "new_chi_sq", "reason")
    )


comparison = build_comparison(baseline, gold)

print("\n[TRANSITION MATRIX] old_signal_flag -> new_signal_flag")
display(build_transition_matrix(comparison))

print("\n[TOP 20 — became SIGNAL]")
display(reason_became_signal(comparison))

print("\n[TOP 20 — lost SIGNAL]")
display(reason_lost_signal(comparison))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Before / After — Profile Metric Deltas
# MAGIC Pure arithmetic over the numbers already collected by `05_profile`
# MAGIC (`run_label="baseline"` and `run_label="after"`) — no new pipeline query needed.

# COMMAND ----------

def build_profile_delta() -> DataFrame:
    """Join baseline vs. after _profile_results metrics and compute the delta."""
    profile_results = spark.read.format("delta").load(VALIDATION["profile_results"])

    baseline_metrics = (
        profile_results
        .filter((col("run_label") == "baseline") & (col("source_quarter") == QUARTER))
        .select("layer", "metric", col("value").alias("baseline_value"))
    )
    after_metrics = (
        profile_results
        .filter((col("run_label") == "after") & (col("source_quarter") == QUARTER))
        .select("layer", "metric", col("value").alias("after_value"))
    )

    return (
        baseline_metrics.join(after_metrics, on=["layer", "metric"], how="full_outer")
        .withColumn("delta", col("after_value") - col("baseline_value"))
        .orderBy(
            when(col("layer") == "bronze", 1).when(col("layer") == "silver", 2)
            .when(col("layer") == "gold", 3).otherwise(4),
            "metric",
        )
    )


profile_delta = build_profile_delta()

print("\n[PROFILE METRIC DELTAS] baseline -> after")
display(profile_delta)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Resume Numbers

# COMMAND ----------

def count_pairs_rescued_by_haldane_now_signal(comparison: DataFrame) -> int:
    """Pairs with no PRR before (null) that are SIGNAL now."""
    return comparison.filter(
        (col("new_signal_flag") == "SIGNAL") & col("old_prr").isNull()
    ).count()


def count_pairs_dropped_by_chi_sq(comparison: DataFrame) -> int:
    """Pairs that were SIGNAL before, still clear case_count and PRR now, but fail chi_sq."""
    return comparison.filter(
        (col("old_signal_flag") == "SIGNAL") &
        (col("new_signal_flag") != "SIGNAL") &
        (col("new_case_count") >= EVANS_MIN_CASES) &
        (col("new_prr") >= EVANS_MIN_PRR) &
        (col("new_chi_sq").isNull() | (col("new_chi_sq") < EVANS_MIN_CHI2))
    ).count()


def print_resume_numbers(profile_delta: DataFrame, comparison: DataFrame) -> None:
    """Print the final summary block: what changed and why."""
    rows = {r["metric"]: r.asDict() for r in profile_delta.collect()}

    superseded_val = rows.get("bronze_demo_superseded_rows", {}).get("after_value")

    child_removed = sum(
        (rows[m]["after_value"] or 0)
        for m in rows
        if m.startswith("bronze_") and m.endswith("_rows_on_superseded_versions")
    )

    baseline_signal = rows.get("gold_signal_flag_SIGNAL", {}).get("baseline_value") or 0
    baseline_weak   = rows.get("gold_signal_flag_WEAK_SIGNAL", {}).get("baseline_value") or 0
    after_signal    = rows.get("gold_signal_flag_SIGNAL", {}).get("after_value") or 0

    rescued           = count_pairs_rescued_by_haldane_now_signal(comparison)
    dropped_by_chi_sq = count_pairs_dropped_by_chi_sq(comparison)

    print("=" * 70)
    print("  RESUME NUMBERS")
    print("=" * 70)
    if superseded_val is not None:
        print(f"  Superseded DEMO versions present  : {superseded_val:,}")
    else:
        print("  Superseded DEMO versions present  : n/a (no 'after' profile run found)")
    print(f"  Child rows removed (superseded)   : {child_removed:,}")
    print(f"  Signals before (SIGNAL+WEAK_SIGNAL): {baseline_signal + baseline_weak:,}")
    print(f"  Signals after (SIGNAL, binary)    : {after_signal:,}")
    print(f"  Pairs rescued by Haldane -> SIGNAL : {rescued:,}")
    print(f"  Pairs dropped by chi_sq gate       : {dropped_by_chi_sq:,}")
    print("=" * 70)


print_resume_numbers(profile_delta, comparison)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Final Result

# COMMAND ----------

if failed_checks:
    raise RuntimeError(
        f"Signal fix validation FAILED: {len(failed_checks)}/{len(checks)} checks failed "
        f"({', '.join(failed_checks)}). Review the [FAIL] lines above before proceeding."
    )

print("\n  All 10 validation checks passed. Signal fix confirmed correct for", QUARTER)
