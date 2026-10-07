-- ============================================================
-- sql/verify_signal_fix.sql
-- PharmaSignalLake — independent verification of the signal-correctness fix
--
-- WHY THIS FILE EXISTS
--   Written before the fix, in a separate session from the code under test,
--   and reviewed by Will.
--   It recomputes dedup and signal math from scratch in plain SQL, so it doesn't
--   share the fix's assumptions. See DECISIONS.md D6.
--
-- READ-ONLY for automated agents. Humans change it only in its own commit,
-- with the reason in the message, decided before looking at pass/fail.
--
-- PATHS mirror 00_config.py (BASE_BRONZE / BASE_SILVER / BASE_GOLD).
--   SQL cells can't read the Python config: if storage moves, update both.
--
-- HOW TO RUN (Databricks SQL cell or %sql)
--   Before the fix re-run : step 0, then A–E  → save output
--   After the fix re-run  : A–I, with <V> = version recorded in step 0
--   Results go in docs/validation-results.md
-- ============================================================

-- ------------------------------------------------------------
-- 0. BEFORE RE-RUNNING THE PIPELINE: record the current gold version.
--    This lets you time-travel back to the pre-fix table later.
-- ------------------------------------------------------------
DESCRIBE HISTORY delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals`;
-- Write down the latest `version` number here: ______


-- ------------------------------------------------------------
-- A. How many superseded case versions exist in raw DEMO?
--    (Bronze doesn't change with the fix, so this number is the ceiling on what the dedup can remove.)
-- ------------------------------------------------------------
SELECT COUNT(*)                            AS demo_rows,
       COUNT(DISTINCT primaryid)           AS distinct_primaryid,
       COUNT(DISTINCT caseid)              AS distinct_caseid,
       COUNT(*) - COUNT(DISTINCT caseid)   AS superseded_versions
FROM delta.`/Volumes/workspace/default/pharma_raw/bronze/bronze_demo`;


-- ------------------------------------------------------------
-- B. Silver dedup invariant.
--    BEFORE the fix: rows == distinct_primaryid, but distinct_caseid may be lower.
--    AFTER the fix:  all three numbers must be equal.
-- ------------------------------------------------------------
SELECT COUNT(*)                  AS rows,
       COUNT(DISTINCT primaryid) AS distinct_primaryid,
       COUNT(DISTINCT caseid)    AS distinct_caseid
FROM delta.`/Volumes/workspace/default/pharma_raw/silver/silver_cases`;


-- ------------------------------------------------------------
-- C. Stale child rows: drug-reaction rows whose case isn't in silver_cases.
--    AFTER the fix: must be 0.
-- ------------------------------------------------------------
SELECT COUNT(*) AS drug_reaction_rows_without_case
FROM delta.`/Volumes/workspace/default/pharma_raw/silver/silver_drug_reactions` dr
LEFT ANTI JOIN delta.`/Volumes/workspace/default/pharma_raw/silver/silver_cases` c
  ON dr.primaryid = c.primaryid;


-- ------------------------------------------------------------
-- D. Classification sanity.
--    AFTER: only SIGNAL / NO_SIGNAL. For SIGNAL rows: min_cases >= 3, min_prr >= 2.
--    (chi_sq is checked in F, since it only exists after the fix.)
-- ------------------------------------------------------------
SELECT signal_flag,
       COUNT(*)          AS pairs,
       MIN(case_count)   AS min_cases,
       MIN(prr)          AS min_prr,
       SUM(CASE WHEN prr IS NULL THEN 1 ELSE 0 END) AS null_prr
FROM delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals`
GROUP BY signal_flag
ORDER BY signal_flag;


-- ------------------------------------------------------------
-- E. The hidden-signal bug: the drug is the ONLY reporter of the event.
--    BEFORE: expect null prr and NO_SIGNAL for these pairs.
--    AFTER:  prr is not null; most should now be SIGNAL.
-- ------------------------------------------------------------
SELECT signal_flag,
       COUNT(*)                                      AS pairs,
       SUM(CASE WHEN prr IS NULL THEN 1 ELSE 0 END)  AS null_prr
FROM delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals`
WHERE case_count >= 3
  AND case_count = event_total
GROUP BY signal_flag;


-- ============================================================
-- AFTER THE FIX ONLY
-- ============================================================

-- ------------------------------------------------------------
-- F. Independent recompute of PRR / ROR / chi-squared from Silver.
--    Rebuilt from scratch in SQL — does not call compute_signals().
--    Expect: mismatches = 0 in every column.
-- ------------------------------------------------------------
WITH dr AS (
  SELECT DISTINCT primaryid, drugname, pt
  FROM delta.`/Volumes/workspace/default/pharma_raw/silver/silver_drug_reactions`
),
n  AS (SELECT CAST(COUNT(DISTINCT primaryid) AS DOUBLE) AS N FROM dr),
pa AS (SELECT drugname, pt, CAST(COUNT(DISTINCT primaryid) AS DOUBLE) AS A FROM dr GROUP BY drugname, pt),
pb AS (SELECT drugname,     CAST(COUNT(DISTINCT primaryid) AS DOUBLE) AS B FROM dr GROUP BY drugname),
pc AS (SELECT pt,           CAST(COUNT(DISTINCT primaryid) AS DOUBLE) AS C FROM dr GROUP BY pt),
cells AS (
  SELECT pa.drugname, pa.pt, N,
         A           AS a,
         B - A       AS b,
         C - A       AS c,
         N - B - C + A AS d
  FROM pa JOIN pb USING (drugname) JOIN pc USING (pt) CROSS JOIN n
),
adj AS (
  SELECT *,
         CASE WHEN a = 0 OR b = 0 OR c = 0 OR d = 0 THEN 0.5 ELSE 0.0 END AS k
  FROM cells
),
mine AS (
  SELECT drugname, pt,
         ((a + k) / (a + b + 2 * k)) / ((c + k) / (c + d + 2 * k))   AS prr_x,
         ((a + k) * (d + k)) / ((b + k) * (c + k))                   AS ror_x,
         CASE WHEN (a + b) = 0 OR (c + d) = 0 OR (a + c) = 0 OR (b + d) = 0 THEN NULL
              ELSE N * POW(ABS(a * d - b * c) - N / 2, 2)
                   / ((a + b) * (c + d) * (a + c) * (b + d))
         END                                                         AS chi_x,
         k > 0                                                       AS haldane_x
  FROM adj
)
SELECT
  COUNT(*)                                                                         AS pairs_compared,
  SUM(CASE WHEN ABS(g.prr    - m.prr_x) > 0.001 * GREATEST(1, m.prr_x) THEN 1 ELSE 0 END) AS prr_mismatch,
  SUM(CASE WHEN ABS(g.ror    - m.ror_x) > 0.001 * GREATEST(1, m.ror_x) THEN 1 ELSE 0 END) AS ror_mismatch,
  SUM(CASE WHEN ABS(g.chi_sq - m.chi_x) > 0.001 * GREATEST(1, m.chi_x) THEN 1 ELSE 0 END) AS chi_mismatch,
  SUM(CASE WHEN g.haldane_applied <> m.haldane_x THEN 1 ELSE 0 END)               AS haldane_mismatch
FROM delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals` g
JOIN mine m USING (drugname, pt);
-- Also check: pairs_compared should equal the total row count of gold_signals.
-- If it's lower, gold has pairs this recompute doesn't — find out why before trusting anything.


-- ------------------------------------------------------------
-- G. Full Evans check on SIGNAL rows. Expect: 0.
-- ------------------------------------------------------------
SELECT COUNT(*) AS bad_signal_rows
FROM delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals`
WHERE signal_flag = 'SIGNAL'
  AND NOT (case_count >= 3 AND prr >= 2 AND chi_sq >= 4);


-- ------------------------------------------------------------
-- H. Before/after transition matrix via Delta time travel.
--    Replace <V> with the version you wrote down in step 0.
--    This is your interview story: how many pairs changed class and why.
-- ------------------------------------------------------------
SELECT COALESCE(o.signal_flag, 'ABSENT') AS old_flag,
       COALESCE(n.signal_flag, 'ABSENT') AS new_flag,
       COUNT(*)                          AS pairs
FROM delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals` VERSION AS OF <V> o
FULL OUTER JOIN delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals` n
  ON o.drugname = n.drugname AND o.pt = n.pt
GROUP BY 1, 2
ORDER BY 1, 2;


-- ------------------------------------------------------------
-- I. Eyeball test: the 10 strongest "rescued" signals.
--    Read the drug names and reactions. Do they make clinical sense, or are they
--    data junk (typos, a single literature report duplicated)? This is a judgment check,
--    and SQL can't do it for you.
-- ------------------------------------------------------------
SELECT n.drugname, n.pt, n.case_count, n.event_total, n.prr, n.chi_sq
FROM delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals` n
JOIN delta.`/Volumes/workspace/default/pharma_raw/gold/gold_signals` VERSION AS OF <V> o
  ON o.drugname = n.drugname AND o.pt = n.pt
WHERE n.signal_flag = 'SIGNAL' AND o.signal_flag <> 'SIGNAL'
ORDER BY n.chi_sq DESC
LIMIT 10;
