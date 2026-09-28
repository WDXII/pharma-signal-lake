"""
test_signals.py — Unit tests for src/signal_math.py.

Run inside Databricks via tests/run_unit_tests.py (see conftest.py — the
`spark` fixture attaches to the cluster's already-running SparkSession, no
local Java/Spark install needed).

Numeric assertions use pytest.approx with a 1e-4 tolerance: compute_signals
already rounds prr/ror/chi_sq to 4 decimal places internally (spark_round),
so this asserts "correct to 4 decimals" without depending on Spark's and
Python's floating-point rounding producing bit-identical doubles.
"""

import pytest

from signal_math import compute_signals, dedup_cases

# Mirrors 00_config.py's Evans thresholds. Not imported from there directly —
# signal_math.py takes thresholds as explicit arguments (no notebook
# globals), so tests declare them locally too. See DECISIONS.md D3.
EVANS_MIN_CASES = 3
EVANS_MIN_PRR   = 2.0
EVANS_MIN_CHI2  = 4.0

# Shared by the two tests below that both use the (ASPIRIN, HEADACHE) pair.
_ASPIRIN_HEADACHE_ROWS = [
    ("1",  "ASPIRIN",   "HEADACHE"),
    ("2",  "ASPIRIN",   "HEADACHE"),
    ("3",  "ASPIRIN",   "HEADACHE"),
    ("4",  "ASPIRIN",   "NAUSEA"),
    ("5",  "ASPIRIN",   "NAUSEA"),
    ("6",  "ASPIRIN",   "NAUSEA"),
    ("7",  "ASPIRIN",   "NAUSEA"),
    ("8",  "ASPIRIN",   "NAUSEA"),
    ("9",  "IBUPROFEN", "HEADACHE"),
    ("10", "IBUPROFEN", "HEADACHE"),
    ("11", "IBUPROFEN", "DIZZINESS"),
    ("12", "IBUPROFEN", "DIZZINESS"),
    ("13", "IBUPROFEN", "DIZZINESS"),
    ("14", "IBUPROFEN", "DIZZINESS"),
    ("15", "IBUPROFEN", "DIZZINESS"),
    ("16", "IBUPROFEN", "DIZZINESS"),
    ("17", "IBUPROFEN", "DIZZINESS"),
    ("18", "IBUPROFEN", "DIZZINESS"),
    ("19", "IBUPROFEN", "DIZZINESS"),
    ("20", "IBUPROFEN", "DIZZINESS"),
]


def test_hand_computed_2x2_table(spark):
    """
    20 total cases. ASPIRIN used in 8 (3 with HEADACHE, 5 with NAUSEA).
    IBUPROFEN used in 12 (2 with HEADACHE, 10 with DIZZINESS).

    For (ASPIRIN, HEADACHE):
      a = A = 3, b = B - A = 8 - 3 = 5, c = C - A = 5 - 3 = 2,
      d = N - B - C + A = 20 - 8 - 5 + 3 = 10
      (all four cells > 0, so no Haldane correction)

    PRR = (a/(a+b)) / (c/(c+d)) = (3/8) / (2/12) = 0.375 / 0.166667 = 2.25
    ROR = (a*d) / (b*c) = (3*10) / (5*2) = 30/10 = 3.0
    chi_sq (Yates) = N*(|ad-bc| - N/2)^2 / ((a+b)(c+d)(a+c)(b+d))
                    = 20*(|30-10| - 10)^2 / (8*12*5*15)
                    = 20*(20-10)^2 / 7200 = 20*100/7200 = 2000/7200 = 0.27778
    """
    dr = spark.createDataFrame(_ASPIRIN_HEADACHE_ROWS, ["primaryid", "drugname", "pt"])

    result = compute_signals(dr, 20, EVANS_MIN_CASES, EVANS_MIN_PRR, EVANS_MIN_CHI2)
    row = result.filter((result.drugname == "ASPIRIN") & (result.pt == "HEADACHE")).collect()[0]

    assert row["case_count"] == 3
    assert row["drug_total"] == 8
    assert row["event_total"] == 5
    assert row["total_reports"] == 20
    assert row["prr"] == pytest.approx(2.25, abs=1e-4)
    assert row["ror"] == pytest.approx(3.0, abs=1e-4)
    assert row["chi_sq"] == pytest.approx(0.2778, abs=1e-4)
    assert row["haldane_applied"] is False


def test_c_equals_a_triggers_haldane(spark):
    """
    10 total cases. ASPIRIN used in 6 (2 with HEADACHE, 4 with NAUSEA).
    HEADACHE never occurs with any other drug, so C = A = 2.

    a = A = 2, b = B - A = 6 - 2 = 4, c = C - A = 2 - 2 = 0,
    d = N - B - C + A = 10 - 6 - 2 + 2 = 4
    c = 0 -> haldane_applied = True; PRR/ROR use a_h=2.5, b_h=4.5,
    c_h=0.5, d_h=4.5.

    PRR = (2.5/7) / (0.5/5) = 0.357143 / 0.1 = 3.5714
    ROR = (2.5*4.5) / (4.5*0.5) = 11.25 / 2.25 = 5.0
    """
    rows = [
        ("1",  "ASPIRIN",   "HEADACHE"),
        ("2",  "ASPIRIN",   "HEADACHE"),
        ("3",  "ASPIRIN",   "NAUSEA"),
        ("4",  "ASPIRIN",   "NAUSEA"),
        ("5",  "ASPIRIN",   "NAUSEA"),
        ("6",  "ASPIRIN",   "NAUSEA"),
        ("7",  "IBUPROFEN", "DIZZINESS"),
        ("8",  "IBUPROFEN", "DIZZINESS"),
        ("9",  "IBUPROFEN", "DIZZINESS"),
        ("10", "IBUPROFEN", "DIZZINESS"),
    ]
    dr = spark.createDataFrame(rows, ["primaryid", "drugname", "pt"])

    result = compute_signals(dr, 10, EVANS_MIN_CASES, EVANS_MIN_PRR, EVANS_MIN_CHI2)
    row = result.filter((result.drugname == "ASPIRIN") & (result.pt == "HEADACHE")).collect()[0]

    assert row["haldane_applied"] is True
    assert row["prr"] is not None
    assert row["prr"] == pytest.approx(3.5714, abs=1e-4)
    assert row["ror"] == pytest.approx(5.0, abs=1e-4)


def test_low_case_count_with_huge_prr_is_no_signal(spark):
    """
    A = 2 (below EVANS_MIN_CASES = 3), B = 3, C = 3, N = 1000 passed
    directly — no need to construct 1000 rows, since compute_signals takes
    n_total as a plain argument rather than deriving it from dr.

    a = 2, b = 3-2 = 1, c = 3-2 = 1, d = 1000-3-3+2 = 996 (no zero cells,
    no Haldane correction).
    PRR = (2/3) / (1/997) = 1994/3 = 664.6667 -- huge, but case_count(2)
    is below EVANS_MIN_CASES(3), so this must be NO_SIGNAL regardless.
    """
    rows = [
        ("1", "DRUGX",     "REACTIONY"),
        ("2", "DRUGX",     "REACTIONY"),
        ("3", "DRUGX",     "OTHERREACTION"),
        ("4", "OTHERDRUG", "REACTIONY"),
    ]
    dr = spark.createDataFrame(rows, ["primaryid", "drugname", "pt"])

    result = compute_signals(dr, 1000, EVANS_MIN_CASES, EVANS_MIN_PRR, EVANS_MIN_CHI2)
    row = result.filter((result.drugname == "DRUGX") & (result.pt == "REACTIONY")).collect()[0]

    assert row["case_count"] == 2
    assert row["prr"] > 100  # "huge" PRR (~664.67)
    assert row["signal_flag"] == "NO_SIGNAL"


def test_prr_above_threshold_but_chi_sq_below_is_no_signal(spark):
    """
    Same (ASPIRIN, HEADACHE) pair as test_hand_computed_2x2_table:
    case_count=3 (>= EVANS_MIN_CASES) and prr=2.25 (>= EVANS_MIN_PRR) both
    clear their thresholds, but chi_sq=0.2778 is well below
    EVANS_MIN_CHI2=4.0 — full Evans Criteria (D3) must classify this
    NO_SIGNAL, not SIGNAL, even though the old PRR-only rule would have.
    """
    dr = spark.createDataFrame(_ASPIRIN_HEADACHE_ROWS, ["primaryid", "drugname", "pt"])

    result = compute_signals(dr, 20, EVANS_MIN_CASES, EVANS_MIN_PRR, EVANS_MIN_CHI2)
    row = result.filter((result.drugname == "ASPIRIN") & (result.pt == "HEADACHE")).collect()[0]

    assert row["case_count"] >= EVANS_MIN_CASES
    assert row["prr"] >= EVANS_MIN_PRR
    assert row["chi_sq"] < EVANS_MIN_CHI2
    assert row["signal_flag"] == "NO_SIGNAL"


def test_dedup_keeps_highest_caseversion_and_its_child_rows(spark):
    """
    caseid=100 has two versions; only caseversion=2 (primaryid ...02)
    should survive dedup_cases(). caseid=200 has a single version and is
    untouched. A synthetic child (drug) table is then left_semi-joined
    against the survivors, mirroring exactly what 02_silver_clean.py does
    for DRUG/REAC/OUTC/RPSR/THER — a row tied to the superseded primaryid
    must be dropped.
    """
    demo_rows = [
        ("100", "1000000001", "1", "M"),  # superseded — must be dropped
        ("100", "1000000002", "2", "M"),  # survives — highest caseversion
        ("200", "2000000001", "1", "F"),  # untouched — only version
    ]
    demo = spark.createDataFrame(demo_rows, ["caseid", "primaryid", "caseversion", "sex"])

    deduped = dedup_cases(demo)
    survivor_ids = {r["primaryid"] for r in deduped.select("primaryid").collect()}

    assert deduped.count() == 2
    assert survivor_ids == {"1000000002", "2000000001"}
    assert "1000000001" not in survivor_ids

    drug_rows = [
        ("1000000001", "OLDDRUG"),    # tied to superseded version — must drop
        ("1000000002", "NEWDRUG"),    # tied to surviving version — keeps
        ("2000000001", "OTHERDRUG"),  # untouched case — keeps
    ]
    drug = spark.createDataFrame(drug_rows, ["primaryid", "drugname"])

    survivors = deduped.select("primaryid")
    filtered_drug = drug.join(survivors, on="primaryid", how="left_semi")
    drugnames = {r["drugname"] for r in filtered_drug.collect()}

    assert filtered_drug.count() == 2
    assert drugnames == {"NEWDRUG", "OTHERDRUG"}
    assert "OLDDRUG" not in drugnames
