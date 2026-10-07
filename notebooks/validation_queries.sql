-- ============================================================
-- PharmaSignalLake — Validation & Exploration Queries
-- ============================================================
-- Purpose : Ad-hoc Spark SQL for data quality checks,
--           pipeline validation, and exploratory analysis.
--
-- Signal-fix verification lives in sql/verify_signal_fix.sql.
--
-- Usage   : Paste any query into a Databricks SQL cell.
--           Replace table paths with your actual Delta paths,
--           or register the tables first using the CREATE VIEW
--           statements in Section 0.
--
-- Sections:
--   0. Register Delta tables as temporary views
--   1. Bronze validation
--   2. Silver validation
--   3. Gold signal exploration
--   4. Demographics exploration
--   5. Outcome analysis
-- ============================================================


-- ============================================================
-- SECTION 0 — Register Delta Tables as Temp Views
-- ============================================================
-- Run these once per session so the SQL below works cleanly.
-- Paths match 00_config.py (Unity Catalog Volume); DBFS root is disabled on Free Edition.

CREATE OR REPLACE TEMP VIEW bronze_demo
USING delta
OPTIONS (path '/Volumes/workspace/default/pharma_raw/bronze/bronze_demo');

CREATE OR REPLACE TEMP VIEW bronze_drug
USING delta
OPTIONS (path '/Volumes/workspace/default/pharma_raw/bronze/bronze_drug');

CREATE OR REPLACE TEMP VIEW bronze_reac
USING delta
OPTIONS (path '/Volumes/workspace/default/pharma_raw/bronze/bronze_reac');

CREATE OR REPLACE TEMP VIEW bronze_indi
USING delta
OPTIONS (path '/Volumes/workspace/default/pharma_raw/bronze/bronze_indi');

CREATE OR REPLACE TEMP VIEW bronze_outc
USING delta
OPTIONS (path '/Volumes/workspace/default/pharma_raw/bronze/bronze_outc');

CREATE OR REPLACE TEMP VIEW bronze_rpsr
USING delta
OPTIONS (path '/Volumes/workspace/default/pharma_raw/bronze/bronze_rpsr');

CREATE OR REPLACE TEMP VIEW bronze_ther
USING delta
OPTIONS (path '/Volumes/workspace/default/pharma_raw/bronze/bronze_ther');

CREATE OR REPLACE TEMP VIEW silver_cases
USING delta
OPTIONS (path '/Volumes/workspace/default/pharma_raw/silver/silver_cases');

CREATE OR REPLACE TEMP VIEW silver_drug_reactions
USING delta
OPTIONS (path '/Volumes/workspace/default/pharma_raw/silver/silver_drug_reactions');

CREATE OR REPLACE TEMP VIEW silver_therapy
USING delta
OPTIONS (path '/Volumes/workspace/default/pharma_raw/silver/silver_therapy');

CREATE OR REPLACE TEMP VIEW gold_signals
USING delta
OPTIONS (path '/Volumes/workspace/default/pharma_raw/gold/gold_signals');

CREATE OR REPLACE TEMP VIEW gold_demographics
USING delta
OPTIONS (path '/Volumes/workspace/default/pharma_raw/gold/gold_demographics');

CREATE OR REPLACE TEMP VIEW gold_outcomes
USING delta
OPTIONS (path '/Volumes/workspace/default/pharma_raw/gold/gold_outcomes');


-- ============================================================
-- SECTION 1 — Bronze Validation
-- ============================================================

-- 1.1 Row counts per Bronze table — compare to expected FAERS file sizes
SELECT 'DEMO'  AS file, source_quarter, COUNT(*) AS row_count FROM bronze_demo GROUP BY source_quarter
UNION ALL
SELECT 'DRUG',           source_quarter, COUNT(*) FROM bronze_drug GROUP BY source_quarter
UNION ALL
SELECT 'REAC',           source_quarter, COUNT(*) FROM bronze_reac GROUP BY source_quarter
UNION ALL
SELECT 'INDI',           source_quarter, COUNT(*) FROM bronze_indi GROUP BY source_quarter
UNION ALL
SELECT 'OUTC',           source_quarter, COUNT(*) FROM bronze_outc GROUP BY source_quarter
UNION ALL
SELECT 'RPSR',           source_quarter, COUNT(*) FROM bronze_rpsr GROUP BY source_quarter
UNION ALL
SELECT 'THER',           source_quarter, COUNT(*) FROM bronze_ther GROUP BY source_quarter
ORDER BY file;


-- 1.2 Sample Bronze DEMO — confirm columns landed correctly
SELECT *
FROM   bronze_demo
LIMIT  10;


-- 1.3 Check all expected DEMO columns are present
-- Expected: primaryid, caseid, caseversion, age, age_cod, sex,
--           reporter_country, event_dt, occp_cod, load_timestamp, source_quarter
DESCRIBE bronze_demo;


-- 1.4 Verify DRUG role_cod distribution — PS should be majority
SELECT
    role_cod,
    COUNT(*)          AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM   bronze_drug
WHERE  source_quarter = '2026Q1'
GROUP BY role_cod
ORDER BY record_count DESC;


-- ============================================================
-- SECTION 2 — Silver Validation
-- ============================================================

-- 2.1 Deduplication check — confirm exactly one row per caseid in silver_cases
-- (primaryid is unique by construction, so checking it can never fail — see DEVELOPER.md Issue 9)
-- Fails (duplicate_rows > 0) if a superseded caseversion survives or a join fans out.
SELECT
    COUNT(*)                   AS total_rows,
    COUNT(DISTINCT caseid)     AS distinct_cases,
    COUNT(*) - COUNT(DISTINCT caseid) AS duplicate_rows
FROM silver_cases
WHERE source_quarter = '2026Q1';


-- 2.2 Age quality distribution — how many cases have valid age data?
SELECT
    age_quality,
    COUNT(*) AS case_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM   silver_cases
WHERE  source_quarter = '2026Q1'
GROUP BY age_quality
ORDER BY case_count DESC;


-- 2.3 Silver drug reactions — confirm PS filter applied
-- All rows should show role_cod = 'PS'
SELECT
    role_cod,
    COUNT(*) AS record_count
FROM   silver_drug_reactions
WHERE  source_quarter = '2026Q1'
GROUP BY role_cod;


-- 2.4 Top 20 drugs in Silver by case count
SELECT
    drugname,
    COUNT(DISTINCT primaryid) AS case_count
FROM   silver_drug_reactions
WHERE  source_quarter = '2026Q1'
GROUP BY drugname
ORDER BY case_count DESC
LIMIT  20;


-- 2.5 Top 20 adverse reactions (PT) in Silver by case count
SELECT
    pt,
    COUNT(DISTINCT primaryid) AS case_count
FROM   silver_drug_reactions
WHERE  source_quarter = '2026Q1'
GROUP BY pt
ORDER BY case_count DESC
LIMIT  20;


-- 2.6 Therapy date quality distribution
SELECT
    date_quality,
    COUNT(*) AS record_count
FROM   silver_therapy
WHERE  source_quarter = '2026Q1'
GROUP BY date_quality
ORDER BY record_count DESC;


-- 2.7 Bronze to Silver row retention check
-- Bronze DEMO rows vs Silver cases — drop should be < 5% after deduplication
SELECT
    b.bronze_rows,
    s.silver_rows,
    ROUND((b.bronze_rows - s.silver_rows) * 100.0 / b.bronze_rows, 2) AS pct_dropped
FROM
    (SELECT COUNT(*) AS bronze_rows FROM bronze_demo WHERE source_quarter = '2026Q1') b,
    (SELECT COUNT(*) AS silver_rows FROM silver_cases WHERE source_quarter = '2026Q1') s;


-- ============================================================
-- SECTION 3 — Gold Signal Exploration
-- ============================================================

-- 3.1 Signal classification summary
SELECT
    signal_flag,
    COUNT(*)                   AS pair_count,
    ROUND(AVG(prr), 2)         AS avg_prr,
    ROUND(AVG(ror), 2)         AS avg_ror,
    MIN(case_count)            AS min_cases,
    MAX(case_count)            AS max_cases
FROM   gold_signals
WHERE  source_quarter = '2026Q1'
GROUP BY signal_flag
ORDER BY pair_count DESC;


-- 3.2 Top 25 strongest signals ranked by PRR
SELECT
    drugname,
    pt,
    case_count,
    drug_total,
    event_total,
    prr,
    ror,
    signal_flag
FROM   gold_signals
WHERE  source_quarter = '2026Q1'
  AND  signal_flag = 'SIGNAL'
ORDER BY prr DESC
LIMIT  25;


-- 3.3 Signals for a specific drug — replace 'WARFARIN' with drug of interest
SELECT
    drugname,
    pt,
    case_count,
    prr,
    ror,
    signal_flag
FROM   gold_signals
WHERE  source_quarter = '2026Q1'
  AND  drugname       = 'WARFARIN'
  AND  signal_flag    = 'SIGNAL'
ORDER BY prr DESC;


-- 3.4 Signals for a specific reaction — replace 'CARDIAC ARREST'
SELECT
    drugname,
    pt,
    case_count,
    prr,
    ror,
    signal_flag
FROM   gold_signals
WHERE  source_quarter = '2026Q1'
  AND  pt             = 'CARDIAC ARREST'
  AND  signal_flag    = 'SIGNAL'
ORDER BY prr DESC;


-- 3.5 Drugs with the most distinct signals detected
SELECT
    drugname,
    COUNT(*) AS signal_count
FROM   gold_signals
WHERE  source_quarter = '2026Q1'
  AND  signal_flag    = 'SIGNAL'
GROUP BY drugname
ORDER BY signal_count DESC
LIMIT  20;


-- 3.6 Reactions appearing most frequently across drug signals
SELECT
    pt,
    COUNT(*) AS signal_count
FROM   gold_signals
WHERE  source_quarter = '2026Q1'
  AND  signal_flag    = 'SIGNAL'
GROUP BY pt
ORDER BY signal_count DESC
LIMIT  20;


-- 3.7 High-volume pairs that do NOT meet Evans criteria
-- These are worth reviewing — large counts but low PRR (expected reactions)
SELECT
    drugname,
    pt,
    case_count,
    prr,
    ror,
    signal_flag
FROM   gold_signals
WHERE  source_quarter = '2026Q1'
  AND  signal_flag    = 'NO_SIGNAL'
  AND  case_count    >= 100
ORDER BY case_count DESC
LIMIT  20;


-- ============================================================
-- SECTION 4 — Demographics Exploration
-- ============================================================

-- 4.1 Age group distribution
SELECT
    dimension_value AS age_group,
    case_count
FROM   gold_demographics
WHERE  source_quarter = '2026Q1'
  AND  dimension      = 'AGE_GROUP'
ORDER BY case_count DESC;


-- 4.2 Sex distribution
SELECT
    dimension_value AS sex,
    case_count
FROM   gold_demographics
WHERE  source_quarter = '2026Q1'
  AND  dimension      = 'SEX'
ORDER BY case_count DESC;


-- 4.3 Top 15 reporter countries
SELECT
    dimension_value AS country,
    case_count
FROM   gold_demographics
WHERE  source_quarter = '2026Q1'
  AND  dimension      = 'REPORTER_COUNTRY'
ORDER BY case_count DESC
LIMIT  15;


-- 4.4 Reporter occupation distribution
SELECT
    dimension_value AS occupation,
    case_count
FROM   gold_demographics
WHERE  source_quarter = '2026Q1'
  AND  dimension      = 'OCCUPATION'
ORDER BY case_count DESC;


-- ============================================================
-- SECTION 5 — Outcome Analysis
-- ============================================================

-- 5.1 Outcome type distribution
SELECT
    label          AS outcome,
    case_count,
    ROUND(case_count * 100.0 / SUM(case_count) OVER (), 2) AS pct
FROM   gold_outcomes
WHERE  source_quarter = '2026Q1'
  AND  dimension      = 'OUTCOME'
ORDER BY case_count DESC;


-- 5.2 Reporter source distribution
SELECT
    label      AS reporter_source,
    case_count,
    ROUND(case_count * 100.0 / SUM(case_count) OVER (), 2) AS pct
FROM   gold_outcomes
WHERE  source_quarter = '2026Q1'
  AND  dimension      = 'REPORTER_SOURCE'
ORDER BY case_count DESC;


-- 5.3 Death cases — which drugs are most associated with fatal outcomes?
-- Joins Gold signals to Silver cases to find drugs where deaths were reported
SELECT
    dr.drugname,
    COUNT(DISTINCT sc.primaryid) AS death_cases
FROM   silver_cases           sc
JOIN   silver_drug_reactions  dr ON sc.primaryid = dr.primaryid
WHERE  sc.source_quarter = '2026Q1'
  AND  dr.source_quarter = '2026Q1'
  AND  sc.outc_cod       = 'DE'
GROUP BY dr.drugname
ORDER BY death_cases DESC
LIMIT  20;
