-- ============================================================
-- sql/case_review_rescued_signals.sql
-- PharmaSignalLake — case-level review of the top 10 rescued signals (2026Q1)
--
-- WHAT THIS IS
--   Ad hoc checks behind the clinical-plausibility assessment in
--   docs/validation-results.md. They test whether a signal's report count (n)
--   reflects distinct patients: duplicate reports, literature cases submitted
--   by several manufacturers, and who reported (stimulated reporting).
--
-- SCOPE
--   The pair list is specific to the 2026Q1 run (query I in
--   sql/verify_signal_fix.sql, ordered by chi-squared). For another quarter,
--   replace the WHERE pairs with that run's query I output.
--
-- PATHS mirror 00_config.py. Run each query in its own %sql cell
--   (or highlight + Ctrl+Shift+Enter). Results are recorded in
--   docs/validation-results.md, not here.
--
-- CAVEAT (P1): concat_ws skips NULLs, so reports missing age / sex / event_dt
--   collapse into one fingerprint. Low fingerprint counts need P2-style
--   row review before calling anything a duplicate.
-- ============================================================


-- ------------------------------------------------------------
-- P1. Duplicate fingerprint per rescued pair
--     cases should equal n from query I.
--     distinct_fingerprints << cases  → possible duplicates (check P2)
--     literature_reports ≈ cases      → likely one published case
--     distinct_senders > 1            → same case from several manufacturers
-- ------------------------------------------------------------
SELECT dr.drugname, dr.pt,
       COUNT(DISTINCT d.primaryid) AS cases,
       COUNT(DISTINCT concat_ws('|', d.age, d.age_cod, d.sex, d.event_dt, d.reporter_country)) AS distinct_fingerprints,
       COUNT(DISTINCT d.mfr_sndr) AS distinct_senders,
       SUM(CASE WHEN d.lit_ref IS NOT NULL AND trim(d.lit_ref) <> '' THEN 1 ELSE 0 END) AS literature_reports
FROM delta.`/Volumes/workspace/default/pharma_raw/silver/silver_drug_reactions` dr
JOIN delta.`/Volumes/workspace/default/pharma_raw/bronze/bronze_demo` d
  ON dr.primaryid = d.primaryid
WHERE (dr.drugname = 'RHOPHYLAC'       AND dr.pt = 'RHESUS ANTIBODIES POSITIVE')
   OR (dr.drugname = 'ADSTILADRIN'     AND dr.pt = 'INSTILLATION SITE DISCHARGE')
   OR (dr.drugname = 'MACI'            AND dr.pt = 'GRAFT DELAMINATION')
   OR (dr.drugname = 'CABLIVI'         AND dr.pt = 'ADAMTS13 ACTIVITY ABNORMAL')
   OR (dr.drugname = 'CARIPRAZINE'     AND dr.pt = 'CONGENITAL EPIBLEPHARON')
   OR (dr.drugname = 'PARAGARD T 380A' AND dr.pt = 'FOREIGN BODY IN UROGENITAL TRACT')
   OR (dr.drugname = 'PARAGARD T 380A' AND dr.pt = 'REPRODUCTIVE COMPLICATION ASSOCIATED WITH DEVICE')
   OR (dr.drugname = 'XIAFLEX'         AND dr.pt = 'FRACTURE OF PENIS')
   OR (dr.drugname = 'ZOLGENSMA'       AND dr.pt = 'RED BLOOD CELL SCHISTOCYTES PRESENT')
   OR (dr.drugname = 'SIMVASTATIN'     AND dr.pt = 'GLYCOGEN STORAGE DISEASE TYPE IX')
GROUP BY dr.drugname, dr.pt
ORDER BY cases DESC;


-- ------------------------------------------------------------
-- P2. Case listing for low-fingerprint pairs
--     Read the rows: same event date / age / country / literature reference
--     across reports → likely the same patient or exposure.
-- ------------------------------------------------------------
SELECT dr.drugname, dr.pt, d.primaryid, d.caseid, d.event_dt, d.fda_dt, d.rept_cod,
       d.mfr_sndr, d.lit_ref, d.age, d.age_cod, d.sex, d.occp_cod, d.reporter_country
FROM delta.`/Volumes/workspace/default/pharma_raw/silver/silver_drug_reactions` dr
JOIN delta.`/Volumes/workspace/default/pharma_raw/bronze/bronze_demo` d
  ON dr.primaryid = d.primaryid
WHERE (dr.drugname = 'CARIPRAZINE' AND dr.pt = 'CONGENITAL EPIBLEPHARON')
   OR (dr.drugname = 'SIMVASTATIN' AND dr.pt = 'GLYCOGEN STORAGE DISEASE TYPE IX')
   OR (dr.drugname = 'ZOLGENSMA'   AND dr.pt = 'RED BLOOD CELL SCHISTOCYTES PRESENT')
   OR (dr.drugname = 'RHOPHYLAC'   AND dr.pt = 'RHESUS ANTIBODIES POSITIVE')
ORDER BY dr.drugname, d.event_dt;


-- ------------------------------------------------------------
-- P3. Reporter occupation for the high-count device pairs
--     occp_cod: MD physician · PH pharmacist · HP other health professional
--               LW lawyer · CN consumer
--     A large CN + LW share is consistent with stimulated reporting.
-- ------------------------------------------------------------
SELECT dr.pt, d.occp_cod, COUNT(DISTINCT d.primaryid) AS cases
FROM delta.`/Volumes/workspace/default/pharma_raw/silver/silver_drug_reactions` dr
JOIN delta.`/Volumes/workspace/default/pharma_raw/bronze/bronze_demo` d
  ON dr.primaryid = d.primaryid
WHERE dr.drugname = 'PARAGARD T 380A'
  AND dr.pt IN ('FOREIGN BODY IN UROGENITAL TRACT', 'REPRODUCTIVE COMPLICATION ASSOCIATED WITH DEVICE')
GROUP BY dr.pt, d.occp_cod
ORDER BY dr.pt, cases DESC;
