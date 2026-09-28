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

    2x2 contingency cells (Evans/FAERS notation):
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
    """

    N = float(n_total)

    # ── Step 1: Compute A — cases per (drug, reaction) pair ───────────────────
    pair_counts = (
        dr
        .select("primaryid", "drugname", "pt")
        .distinct()                             # one row per case-drug-reaction
        .groupBy("drugname", "pt")
        .agg(countDistinct("primaryid").alias("A"))
    )

    # ── Step 2: Compute B — total cases per drug (across all reactions) ────────
    drug_totals = (
        dr
        .select("primaryid", "drugname")
        .distinct()
        .groupBy("drugname")
        .agg(countDistinct("primaryid").alias("B"))
    )

    # ── Step 3: Compute C — total cases per reaction (across all drugs) ────────
    reaction_totals = (
        dr
        .select("primaryid", "pt")
        .distinct()
        .groupBy("pt")
        .agg(countDistinct("primaryid").alias("C"))
    )

    # ── Step 4: Assemble the 2x2 cells ──────────────────────────────────────────
    contingency = (
        pair_counts
        .join(drug_totals,     on="drugname", how="left")
        .join(reaction_totals, on="pt",       how="left")
        .withColumn("A", col("A").cast(DoubleType()))
        .withColumn("B", col("B").cast(DoubleType()))
        .withColumn("C", col("C").cast(DoubleType()))
        .withColumn("N", lit(N))
        .withColumn("a", col("A"))
        .withColumn("b", col("B") - col("A"))
        .withColumn("c", col("C") - col("A"))
        .withColumn("d", col("N") - col("B") - col("C") + col("A"))
    )

    # ── Step 5: Haldane-Anscombe correction, PRR/ROR cells only ────────────────
    contingency = contingency.withColumn(
        "haldane_applied",
        (col("a") == 0) | (col("b") == 0) | (col("c") == 0) | (col("d") == 0)
    )

    contingency = (
        contingency
        .withColumn("a_h", when(col("haldane_applied"), col("a") + 0.5).otherwise(col("a")))
        .withColumn("b_h", when(col("haldane_applied"), col("b") + 0.5).otherwise(col("b")))
        .withColumn("c_h", when(col("haldane_applied"), col("c") + 0.5).otherwise(col("c")))
        .withColumn("d_h", when(col("haldane_applied"), col("d") + 0.5).otherwise(col("d")))
    )

    # ── Step 6: PRR and ROR on Haldane-adjusted cells ──────────────────────────
    # Haldane guarantees a_h, b_h, c_h, d_h are all > 0, so (a_h+b_h) > 0,
    # (c_h+d_h) > 0, and (b_h*c_h) > 0 always — no null guard needed.
    contingency = (
        contingency
        .withColumn(
            "prr",
            spark_round(
                (col("a_h") / (col("a_h") + col("b_h"))) /
                (col("c_h") / (col("c_h") + col("d_h"))),
                4
            )
        )
        .withColumn(
            "ror",
            spark_round(
                (col("a_h") * col("d_h")) / (col("b_h") * col("c_h")),
                4
            )
        )
    )

    # ── Step 7: chi_sq (Yates), on RAW cells, null if any marginal is 0 ───────
    marginal_ab = col("a") + col("b")   # = B
    marginal_cd = col("c") + col("d")   # = N - B
    marginal_ac = col("a") + col("c")   # = C
    marginal_bd = col("b") + col("d")   # = N - C

    contingency = contingency.withColumn(
        "chi_sq",
        when(
            (marginal_ab > 0) & (marginal_cd > 0) & (marginal_ac > 0) & (marginal_bd > 0),
            spark_round(
                (col("N") *
                    (spark_abs(col("a") * col("d") - col("b") * col("c")) - col("N") / 2.0) ** 2
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
                (col("a") >= evans_min_cases) &
                (col("prr") >= evans_min_prr) &
                (col("chi_sq") >= evans_min_chi2),
                "SIGNAL"
            )
            .otherwise("NO_SIGNAL")
        )
        .select(
            "drugname",
            "pt",
            col("a").cast("int").alias("case_count"),
            col("B").cast("int").alias("drug_total"),
            col("C").cast("int").alias("event_total"),
            col("N").cast("int").alias("total_reports"),
            "prr",
            "ror",
            "chi_sq",
            "haldane_applied",
            "signal_flag",
        )
    )

    return signals
