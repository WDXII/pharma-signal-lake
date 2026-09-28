"""
signal_math.py — Pure functions for PharmaSignalLake's pipeline logic.

Extracted out of the Databricks notebooks (notebooks/02_silver_clean.py,
notebooks/03_gold_signals.py) so they can be imported both from those
notebooks and from a plain pytest file (tests/test_signals.py). Every
function here takes a DataFrame (and plain scalar arguments) and returns a
DataFrame — no table reads, no table writes, no dependency on notebook
globals like `spark`, `dbutils`, or 00_config.py's constants. Thresholds
that live in 00_config.py (e.g. EVANS_MIN_CASES) are passed in explicitly
by the caller, so 00_config.py stays the single source of truth (NFR-02)
and this module has no hidden state.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    countDistinct,
    expr,
    lit,
    round as spark_round,
    row_number,
    when,
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window


def dedup_cases(demo: DataFrame) -> DataFrame:
    """
    Deduplicate a DEMO-shaped DataFrame to one row per caseid, keeping the
    row with the highest caseversion. Requires `caseid`, `primaryid`, and
    `caseversion` columns; every other column is preserved unchanged.

    caseversion is parsed with try_cast so a malformed value sorts last
    (NULL) instead of raising under ANSI mode.
    """
    caseversion_int = expr("try_cast(caseversion AS INT)")
    window_latest = Window.partitionBy("caseid").orderBy(
        caseversion_int.desc_nulls_last()
    )

    return (
        demo
        .withColumn("rn", row_number().over(window_latest))
        .filter(col("rn") == 1)
        .drop("rn")
    )


def compute_signals(
    dr: DataFrame,
    n_total: int,
    evans_min_cases: int,
    evans_min_prr: float,
    evans_min_chi2: float,
) -> DataFrame:
    """
    Compute PRR, ROR, Yates chi_sq and signal_flag for every (drugname, pt)
    pair in `dr`. `dr` must have `primaryid`, `drugname`, `pt` columns (one
    row per case-drug-reaction, as produced by silver_drug_reactions).
    `n_total` is the total unique case count (N) — passed in rather than
    derived from `dr`, since the caller may filter/dedupe cases differently
    than this function would.

    2x2 contingency cells (Evans/FAERS math notation a/b/c/d; internal
    columns are named cell_a/cell_b/cell_c/cell_d — see the note below):
        a = A = cases with drug D AND reaction R
        b = B - A = cases with drug D, any other reaction
        c = C - A = cases with reaction R, any other drug
        d = N - B - C + A = cases with neither drug D nor reaction R

    Haldane-Anscombe correction (+0.5 to all four cells) is applied to
    a/b/c/d for PRR/ROR only, whenever any cell is 0 (haldane_applied).
    This guarantees every PRR/ROR denominator is > 0, so no when() guard
    is needed for them under ANSI mode. chi_sq always uses the RAW,
    uncorrected cells and is null when any of its four marginals is 0.

    signal_flag is binary: "SIGNAL" iff
    case_count >= evans_min_cases AND prr >= evans_min_prr
    AND chi_sq >= evans_min_chi2; otherwise "NO_SIGNAL". case_count is
    always the raw `a` (= A), never the Haldane-adjusted value.

    NOTE: internal DataFrame columns are never named with only a case
    difference from one another (e.g. never both "A" and "a"). Spark's
    default spark.sql.caseSensitive=false makes such names ambiguous, and
    Spark Connect (Databricks Serverless) was observed to silently resolve
    col() to the wrong one instead of raising an error — see DEVELOPER.md
    Issue 11.
    """

    N = float(n_total)

    # ── Step 1: Compute pair_n — cases per (drug, reaction) pair (A) ──────────
    pair_counts = (
        dr
        .select("primaryid", "drugname", "pt")
        .distinct()                             # one row per case-drug-reaction
        .groupBy("drugname", "pt")
        .agg(countDistinct("primaryid").alias("pair_n"))
    )

    # ── Step 2: Compute drug_n — total cases per drug (across reactions, B) ───
    drug_totals = (
        dr
        .select("primaryid", "drugname")
        .distinct()
        .groupBy("drugname")
        .agg(countDistinct("primaryid").alias("drug_n"))
    )

    # ── Step 3: Compute reaction_n — total cases per reaction (across drugs, C)
    reaction_totals = (
        dr
        .select("primaryid", "pt")
        .distinct()
        .groupBy("pt")
        .agg(countDistinct("primaryid").alias("reaction_n"))
    )

    # ── Step 4: Assemble the 2x2 cells ──────────────────────────────────────────
    # NOTE: column names below are deliberately all-lowercase and mutually
    # distinct (never differing only by case, e.g. never both "A" and "a").
    # Spark's default spark.sql.caseSensitive=false makes case-only-distinct
    # column names ambiguous, and Spark Connect (Databricks Serverless)
    # silently resolves col() to the wrong one instead of raising — see
    # DEVELOPER.md Issue 11.
    contingency = (
        pair_counts
        .join(drug_totals,     on="drugname", how="left")
        .join(reaction_totals, on="pt",       how="left")
        .withColumn("pair_n",     col("pair_n").cast(DoubleType()))
        .withColumn("drug_n",     col("drug_n").cast(DoubleType()))
        .withColumn("reaction_n", col("reaction_n").cast(DoubleType()))
        .withColumn("total_n",    lit(N))
        .withColumn("cell_a", col("pair_n"))
        .withColumn("cell_b", col("drug_n") - col("pair_n"))
        .withColumn("cell_c", col("reaction_n") - col("pair_n"))
        .withColumn("cell_d", col("total_n") - col("drug_n") - col("reaction_n") + col("pair_n"))
    )

    # ── Step 5: Haldane-Anscombe correction, PRR/ROR cells only ────────────────
    contingency = contingency.withColumn(
        "haldane_applied",
        (col("cell_a") == 0) | (col("cell_b") == 0) | (col("cell_c") == 0) | (col("cell_d") == 0)
    )

    contingency = (
        contingency
        .withColumn("cell_a_h", when(col("haldane_applied"), col("cell_a") + 0.5).otherwise(col("cell_a")))
        .withColumn("cell_b_h", when(col("haldane_applied"), col("cell_b") + 0.5).otherwise(col("cell_b")))
        .withColumn("cell_c_h", when(col("haldane_applied"), col("cell_c") + 0.5).otherwise(col("cell_c")))
        .withColumn("cell_d_h", when(col("haldane_applied"), col("cell_d") + 0.5).otherwise(col("cell_d")))
    )

    # ── Step 6: PRR and ROR on Haldane-adjusted cells ──────────────────────────
    # Haldane guarantees cell_a_h, cell_b_h, cell_c_h, cell_d_h are all > 0, so
    # (cell_a_h+cell_b_h) > 0, (cell_c_h+cell_d_h) > 0, and (cell_b_h*cell_c_h)
    # > 0 always — no null guard needed.
    contingency = (
        contingency
        .withColumn(
            "prr",
            spark_round(
                (col("cell_a_h") / (col("cell_a_h") + col("cell_b_h"))) /
                (col("cell_c_h") / (col("cell_c_h") + col("cell_d_h"))),
                4
            )
        )
        .withColumn(
            "ror",
            spark_round(
                (col("cell_a_h") * col("cell_d_h")) / (col("cell_b_h") * col("cell_c_h")),
                4
            )
        )
    )

    # ── Step 7: chi_sq (Yates), on RAW cells, null if any marginal is 0 ───────
    marginal_ab = col("cell_a") + col("cell_b")   # = drug_n
    marginal_cd = col("cell_c") + col("cell_d")   # = total_n - drug_n
    marginal_ac = col("cell_a") + col("cell_c")   # = reaction_n
    marginal_bd = col("cell_b") + col("cell_d")   # = total_n - reaction_n

    contingency = contingency.withColumn(
        "chi_sq",
        when(
            (marginal_ab > 0) & (marginal_cd > 0) & (marginal_ac > 0) & (marginal_bd > 0),
            spark_round(
                (col("total_n") *
                    (spark_abs(col("cell_a") * col("cell_d") - col("cell_b") * col("cell_c"))
                        - col("total_n") / 2.0) ** 2
                ) / (marginal_ab * marginal_cd * marginal_ac * marginal_bd),
                4
            )
        ).otherwise(None)
    )

    # ── Step 8: Classify signals using full Evans Criteria ────────────────────
    signals = (
        contingency
        .withColumn(
            "signal_flag",
            when(
                (col("cell_a") >= evans_min_cases) &
                (col("prr") >= evans_min_prr) &
                (col("chi_sq") >= evans_min_chi2),
                "SIGNAL"
            )
            .otherwise("NO_SIGNAL")
        )
        .select(
            "drugname",
            "pt",
            col("cell_a").cast("int").alias("case_count"),
            col("drug_n").cast("int").alias("drug_total"),
            col("reaction_n").cast("int").alias("event_total"),
            col("total_n").cast("int").alias("total_reports"),
            "prr",
            "ror",
            "chi_sq",
            "haldane_applied",
            "signal_flag",
        )
    )

    return signals
