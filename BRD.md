# Business Requirements Document (BRD)

**Project:** PharmaSignalLake  
**Full Title:** PharmaSignalLake: A Pharmacovigilance Signal Detection Lakehouse  
**Author:** Will  
**Version:** 2.5  
**Date:** May 2026 (Revised 2026-10-06)  
**Status:** Draft

> **Revision 2.5** — NFR-01: records that per-stage start, end and duration are written in UTC to the
> `99_run_pipeline.py` output (in-memory, not persisted); the 60-minute limit is unchanged. Verified in a
> Databricks run on 2026-10-06 (DEVELOPER Issue 25, DECISIONS D10).
>
> **Revision 2.4** — Claims aligned to the verified state of `fix/signal-correctness`: extensibility is a
> design goal verified on 2026Q1 only (second-quarter run deferred until after release); PRR/ROR are
> computed for all pairs and the Evans minimum applies to classification; `primaryid`/`caseid` wording
> corrected; schema validation marked planned, not implemented; reports-not-patients constraint added (D9).
>
> **Revision 2.3** — FR-21 implements the amended D8 (exclusive panel filter is `is_exclusive`, not
> `haldane_applied`); patient-level duplicate detection added to Out of Scope and Future Enhancements (D9).
>
> **Revision 2.2** — FR-21 implements D8 (main leaderboard = non-exclusive `SIGNAL` rows; separate
> "Exclusive reports" panel) and the row-retention success metric is reworded (see Change Log).
>
> **Revision 2.1** — Updated FR-06, FR-14, FR-15, FR-16, Section 5 Scope, and the Evans Criteria
> glossary entry to reflect the signal-correctness fix on `fix/signal-correctness`
> (see `DECISIONS.md` D1-D4 for full rationale and 2026Q1 confirmation evidence).

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Business Problem](#2-business-problem)
3. [Project Objectives](#3-project-objectives)
4. [Stakeholders](#4-stakeholders)
5. [Scope](#5-scope)
6. [Data Sources](#6-data-sources)
7. [Functional Requirements](#7-functional-requirements)
8. [Non-Functional Requirements](#8-non-functional-requirements)
9. [Success Metrics](#9-success-metrics)
10. [Assumptions and Constraints](#10-assumptions-and-constraints)
11. [Risks](#11-risks)
12. [Glossary](#12-glossary)
13. [Change Log](#change-log)

---

## 1. Executive Summary

Post-market drug safety surveillance is a regulatory obligation for every pharmaceutical company operating globally. Adverse event reports submitted to the FDA accumulate every quarter into the Adverse Event Reporting System (FAERS) — a rich, publicly available dataset that, in raw form, is too large and too messy to analyze manually.

**PharmaSignalLake** is a cloud-native data lakehouse pipeline built on Databricks and Apache Spark (PySpark) that ingests raw FDA FAERS quarterly data, cleanses and integrates it across seven source file types, computes industry-standard disproportionality safety signals (PRR and ROR), and surfaces results through a Power BI executive dashboard.

The pipeline is built on the **Medallion Architecture** (Bronze → Silver → Gold) with **Delta Lake** as the storage layer, enabling ACID-compliant, auditable, and incrementally extensible processing. The solution is designed to process one quarter and extend to additional quarters; verified on 2026Q1.

---

## 2. Business Problem

Pharmaceutical safety teams and regulatory affairs departments face three compounding problems:

**Volume.** A single FAERS quarter contains approximately 600,000 to 900,000 individual case safety reports. Each case spans seven related data files. Manual review is not feasible at this scale.

**Signal obscurity.** A drug causing a rare but serious adverse reaction may account for only 0.1% of reports for that drug — invisible to anyone scanning raw data, but detectable by disproportionality algorithms that compare reporting ratios across all drugs in the database.

**Speed.** Regulatory agencies expect timely signal detection and response. A pipeline that takes days to produce quarterly analysis is a liability risk. A lakehouse pipeline running on cloud compute can produce the same analysis in under an hour.

This project addresses all three problems with a structured, reproducible, scalable technical solution.

---

## 3. Project Objectives

| ID | Objective |
|----|-----------|
| OBJ-01 | Ingest all seven FAERS quarterly source files into a governed Bronze layer without data loss |
| OBJ-02 | Cleanse, standardize, deduplicate, and integrate source files into a unified Silver layer |
| OBJ-03 | Compute PRR and ROR disproportionality metrics for all drug-event pairs |
| OBJ-04 | Classify signals using Evans Criteria and surface results in a structured Gold layer |
| OBJ-05 | Deliver a Power BI dashboard with three report pages: Signal Leaderboard, Demographics, and Case Outcomes |
| OBJ-06 | Design the pipeline to be quarter-agnostic and extensible to N quarters via configuration only |
| OBJ-07 | Produce professional documentation (BRD, ARCHITECTURE.md, SETUP.md, README) suitable for a portfolio |

---

## 4. Stakeholders

| Role | Responsibility in this Project |
|------|-------------------------------|
| **Portfolio Owner (You)** | Architect, developer, and documentarian of the end-to-end solution |
| **Pharmacovigilance Analyst** *(simulated)* | Primary consumer of the Signal Leaderboard dashboard |
| **Regulatory Affairs Manager** *(simulated)* | Receives signal classification reports; uses Outcome and Demographics pages |
| **Data Engineering Lead** *(simulated)* | Reviews pipeline architecture, modular notebook design, and data quality checkpoints |
| **Recruiter / Hiring Manager** *(real)* | Evaluates the portfolio for Databricks, PySpark, and clinical data competency |

---

## 5. Scope

### In Scope

- Ingestion of all seven FDA FAERS file types: DEMO, DRUG, INDI, OUTC, REAC, RPSR, THER
- Processing of one quarter minimum (2026Q1); pipeline extensible to additional quarters (extensibility verified on 2026Q1 only)
- Deduplication on `caseid` (the case entity key), retaining the highest `caseversion`; `primaryid` is used for joins
- PRR and ROR computation for all drug-event pairs; the Evans minimum (≥ 3 cases) applies to signal classification
- Signal classification: signal or no signal (binary), per full Evans Criteria (case count, PRR, and chi-squared)
- Power BI dashboard with three report pages
- Full documentation suite

### Out of Scope

- Real-time or streaming ingestion (batch only)
- MedDRA drug dictionary normalization (basic text normalization only)
- WHO VigiAccess or EudraVigilance integration
- Automated scheduling or job orchestration (not supported by Databricks Free Edition)
- Direct database connections from Power BI (Power BI reads from exported Gold CSV/Parquet)
- Patient-level (cross-report) duplicate detection, see D9

### Future Enhancements

- Fingerprint-based duplicate detection across reports (age, sex, event date, country, drug, reaction, `lit_ref`), validated against a gold standard before use (see D9)

---

## 6. Data Sources

**Primary Source:** FDA Adverse Event Reporting System (FAERS)  
**URL:** https://www.fda.gov/drugs/questions-and-answers-fdas-adverse-event-reporting-system-faers/fda-adverse-event-reporting-system-faers-latest-quarterly-data-files  
**Access:** Public. No registration or license required.  
**Format:** ASCII pipe-delimited (`$` separator), one ZIP file per quarter  
**License:** U.S. Government open data, no restrictions on use

### File Inventory

| File | Description | Key Fields | Approx. Rows (1 Quarter) |
|------|-------------|-----------|--------------------------|
| **DEMO** | Patient demographics per case | `primaryid`, `age`, `sex`, `reporter_country`, `occp_cod`, `event_dt` | ~400,000 |
| **DRUG** | All drugs reported per case | `primaryid`, `drug_seq`, `role_cod`, `drugname`, `prod_ai`, `route`, `dose_amt` | ~1,700,000 |
| **REAC** | Adverse reactions (MedDRA PT) | `primaryid`, `pt`, `drug_rec_act` | ~1,300,000 |
| **INDI** | Drug indications | `primaryid`, `indi_drug_seq`, `indi_pt` | ~1,100,000 |
| **OUTC** | Case outcomes | `primaryid`, `outc_cod` | ~300,000 |
| **RPSR** | Report sources | `primaryid`, `rpsr_cod` | ~12,000 |
| **THER** | Therapy start/end dates | `primaryid`, `dsg_drug_seq`, `start_dt`, `end_dt`, `dur` | ~400,000 |

### Key Relationships

- `primaryid` is the master join key across all seven files
- `caseid` is the case identifier; `primaryid` includes the case version. Use `primaryid` for joins; `caseid` is the case key used for dedup (D4)
- `drug_seq` / `dsg_drug_seq` / `indi_drug_seq` link drug-level records across DRUG, THER, and INDI
- `role_cod` in DRUG distinguishes Primary Suspect (`PS`), Secondary Suspect (`SS`), Concomitant (`C`), and Interacting (`I`) drugs

---

## 7. Functional Requirements

### Bronze Layer — Raw Ingestion

| ID | Requirement |
|----|-------------|
| FR-01 | The system shall read all seven FAERS `$`-delimited source files from a Unity Catalog Volume using PySpark |
| FR-02 | Each file shall be written to a Bronze Delta table partitioned by `source_quarter` |
| FR-03 | Bronze ingestion shall preserve all source columns without transformation |
| FR-04 | Each Bronze record shall include a `load_timestamp` and `source_quarter` audit column |
| FR-05 | Bronze ingestion shall log the row count per file and fail with a descriptive error if row count is zero |

### Silver Layer — Cleansing and Integration

| ID | Requirement |
|----|-------------|
| FR-06 | DEMO shall be deduplicated on `caseid` (the case entity key — `primaryid` already includes `caseversion` and is unique per row), retaining the record with the highest `caseversion`. All downstream child tables (DRUG, REAC, OUTC, RPSR, THER) shall be filtered to only the surviving `primaryid`s |
| FR-07 | DRUG `drugname` and `prod_ai` fields shall be standardized to uppercase with leading/trailing whitespace removed |
| FR-08 | REAC `pt` (Preferred Term) field shall be standardized to uppercase |
| FR-09 | Invalid or null `age` values shall be flagged with a data quality indicator column rather than dropped |
| FR-10 | A unified Silver case table shall join DEMO, OUTC, and RPSR on `primaryid`. OUTC and RPSR shall be aggregated to one row per `primaryid` before joining, so the case table has exactly one row per case |
| FR-11 | A Silver drug-reaction table shall join DRUG and REAC on `primaryid`, filtered to Primary Suspect drugs (`role_cod = 'PS'`) only |
| FR-12 | Silver tables shall include `silver_load_timestamp` and `source_quarter` audit columns |

### Gold Layer — Signal Detection

| ID | Requirement |
|----|-------------|
| FR-13 | The system shall compute, for each drug-event pair: case count (A), drug total (B), event total (C), and total reports (N) |
| FR-14 | PRR shall be computed as: `(A / B) / ((C - A) / (N - B))`. If any of the four contingency-table cells (A, B-A, C-A, N-B-C+A) is zero, the Haldane-Anscombe correction (+0.5 to all four cells) shall be applied before this division, and recorded in a `haldane_applied` flag |
| FR-15 | ROR shall be computed as: `(A / (B - A)) / ((C - A) / (N - B - C + A))`, with the same Haldane-Anscombe correction as FR-14 applied when any cell is zero |
| FR-16 | Signal classification shall apply full Evans Criteria: cases ≥ 3 AND PRR ≥ 2 AND chi-squared (Yates-corrected) ≥ 4. Chi-squared shall be computed on the raw, uncorrected cells, not the Haldane-adjusted ones |
| FR-16a | The chi-squared statistic (Yates-corrected, computed on raw cells) shall be stored in a `chi_sq` column; it is null when any marginal total is zero |
| FR-17 | Classified signals shall be written to a Gold Delta table: `gold_signals`, including the `chi_sq` and `haldane_applied` columns. `signal_flag` shall be `SIGNAL` or `NO_SIGNAL` only |
| FR-18 | The Gold layer shall also contain an aggregated demographics summary table and an outcome distribution table |

### Reporting Layer — Power BI

| ID | Requirement |
|----|-------------|
| FR-19 | Gold layer data shall be exported to CSV or Parquet for Power BI Desktop consumption |
| FR-20 | The Power BI report shall contain three pages: Signal Leaderboard, Patient Demographics, and Case Outcomes |
| FR-21 | Power BI shall derive `is_exclusive` as `case_count = event_total`. The main Signal Leaderboard shall display `SIGNAL` pairs where NOT `is_exclusive` (49,878 on 2026Q1), ranked by PRR, with ROR and case count as secondary columns and `haldane_applied` as a tooltip flag. Exclusive pairs (`SIGNAL` AND `is_exclusive`, 106 on 2026Q1) shall appear in a separate "Exclusive reports" panel, not in the main leaderboard, and PRR shall not be shown for them (D8, amended 2026-09-30). No pipeline change is needed: the export already carries `case_count` and `event_total` |

---

## 8. Non-Functional Requirements

| ID | Category | Requirement |
|----|----------|-------------|
| NFR-01 | Performance | The full pipeline (Bronze through Gold) shall complete within 60 minutes for a single quarter on Databricks Free Edition compute. Per-stage start, end and duration are recorded in UTC in the `99_run_pipeline.py` output (in-memory, not persisted) |
| NFR-02 | Scalability | Adding a new quarter shall require only a configuration change — no notebook code changes |
| NFR-03 | Reliability | All notebooks shall include row-count validation checkpoints after each major transformation |
| NFR-04 | Maintainability | All PySpark transformations shall be written as named, documented functions |
| NFR-05 | Auditability | Every table at every layer shall carry `load_timestamp` and `source_quarter` on every row |
| NFR-06 | Reproducibility | A single orchestration notebook shall execute the full pipeline end-to-end from a clean state |
| NFR-07 | Portability | All notebooks shall be committed to a GitHub repository for version control and portfolio presentation |

---

## 9. Success Metrics

| Metric | Target |
|--------|--------|
| All seven FAERS source files ingested successfully | 7 of 7 |
| `silver_cases` contains exactly one row per distinct `caseid` | Rows = distinct `caseid` (verified: 397,224 = 397,224) |
| Drug-event pairs with computed PRR and ROR | All pairs (PRR/ROR computed for every pair; the ≥ 3 case minimum applies to classification) |
| Signal classification coverage | 100% of pairs classified |
| Signal-fix verification | All V1–V10 checks in `06_validate_fix.py` and all `sql/verify_signal_fix.sql` checks pass |
| Power BI dashboard load time on Gold data | < 10 seconds |
| Pipeline extensibility | Designed so a new quarter is processed by config change; verified on 2026Q1 only. Second-quarter run planned after the initial release |
| Documentation completeness | BRD, ARCHITECTURE.md, SETUP.md, README all present in GitHub |

---

## 10. Assumptions and Constraints

### Assumptions

- FDA FAERS quarterly data continues to be publicly available at the official FDA URL
- FAERS file schema remains consistent across quarters (schema validation is planned, not yet implemented; PROJECT-LOG I4)
- Basic drug name normalization (uppercase + trim) is sufficient for portfolio-level signal detection without a full MedDRA drug dictionary
- Databricks Free Edition provides adequate daily compute quota to process one quarter per session
- Power BI Desktop (free) is the reporting tool

### Constraints

| Constraint | Detail |
|-----------|--------|
| **Platform** | Databricks Free Edition — no automated job scheduling |
| **Compute** | Serverless compute with daily quota limits |
| **Budget** | $0 |
| **Drug normalization** | Text normalization only; no WHO or MedDRA drug dictionary |
| **Scope** | Minimum one quarter (2026Q1); designed to scale to N quarters (extensibility verified on 2026Q1 only) |
| **Unit of count** | `case_count` counts reports, not patients; patient-level duplicate detection is out of scope (D9) |

---

## 11. Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| FAERS file schema changes between quarters | Medium | High | Schema validation at Bronze ingestion; alert on column count mismatch: planned, not yet implemented (PROJECT-LOG I4) |
| Daily compute quota exceeded | Medium | Medium | Process one quarter per session; checkpoint results to Delta after each layer |
| Drug name inconsistency reduces signal accuracy | High | Medium | Document limitation clearly in README; apply uppercase + trim as baseline |
| Free Edition account deactivated due to inactivity | Low | High | Push notebooks to GitHub after every session |
| Signal math implementation error | Low | High | Unit tests (`tests/test_signals.py`) on hand-computed 2×2 tables, plus an independent plain-SQL recompute of every pair (`06_validate_fix.py` V9) — both implemented |

---

## 12. Glossary

| Term | Definition |
|------|------------|
| **Adverse Event (AE)** | Any undesirable experience associated with the use of a medical product |
| **FAERS** | FDA Adverse Event Reporting System — the US post-market drug safety database |
| **ICSR** | Individual Case Safety Report — the unit record in pharmacovigilance databases |
| **MedDRA PT** | Medical Dictionary for Regulatory Activities Preferred Term — standardized adverse event terminology |
| **Pharmacovigilance** | The science of monitoring, detecting, and preventing adverse effects of drugs after market approval |
| **PRR** | Proportional Reporting Ratio — measures how often a drug-event pair is reported relative to all events for that drug, compared to the same ratio across all other drugs |
| **ROR** | Reporting Odds Ratio — an odds-ratio-based signal metric; more statistically stable than PRR at low case counts |
| **Signal** | A statistical pattern suggesting a potential causal relationship between a drug and an adverse event, requiring further clinical review |
| **Evans Criteria** | Minimum threshold for signal validity: ≥ 3 cases AND PRR ≥ 2 AND chi-squared (Yates-corrected) ≥ 4 |
| **Chi-squared (Yates)** | Significance test on the 2×2 drug-event table with continuity correction: `N·(abs(ad − bc) − N/2)² / ((a+b)(c+d)(a+c)(b+d))`. Computed on raw cells; ≥ 4 is the Evans threshold (~p < 0.05) |
| **Haldane-Anscombe Correction** | A statistical correction (+0.5 added to all four contingency-table cells) applied when any cell is zero, so PRR and ROR remain computable instead of undefined |
| **Medallion Architecture** | A three-layer data design pattern: Bronze (raw), Silver (cleansed), Gold (aggregated/curated) |
| **Delta Lake** | An open-source storage layer that adds ACID transactions, versioning, and schema enforcement to Parquet files |
| **primaryid** | The unique identifier for a specific version of a case report in FAERS. Use it for joins; `caseid` is the case key used for dedup (D4) |
| **role_cod** | Drug role code in FAERS DRUG file: PS = Primary Suspect, SS = Secondary Suspect, C = Concomitant, I = Interacting |
| **DBU** | Databricks Unit — the billing/quota metric for Databricks compute consumption |

---

*End of BRD — Version 2.5 — PharmaSignalLake*

---

## Change Log

| Version | Date | Changes | Why |
|---------|------|---------|-----|
| 2.0 | May 2026 | Initial requirements | — |
| 2.1 | 2026-09-29 | Scope and glossary: removed "weak signal"; classification is `SIGNAL` / `NO_SIGNAL` (D1). FR-06: dedup on `caseid`, keep max `caseversion`, child tables filtered to surviving `primaryid`s (D4); Scope bullet updated to match. FR-10: OUTC/RPSR aggregated per `primaryid` (D7). FR-14/FR-15: Haldane-Anscombe correction for zero cells (D2). FR-16: full Evans with chi-squared (D3); added FR-16a (`chi_sq` column). FR-17: `chi_sq` and `haldane_applied` columns, binary flag. Success Metrics: added V1–V10 and `verify_signal_fix.sql` pass criterion. Risks: signal-math mitigation now implemented (unit tests + independent SQL recompute). Glossary: updated Evans, added Chi-squared. Author set to Will | v2.0 deduplicated on `primaryid` (a no-op), left zero-cell pairs with null PRR (labelled NO_SIGNAL), and classified on PRR alone with an unsupported weak tier. See DECISIONS.md D1–D7 and DEVELOPER.md Issues 9, 13, 14 |
| 2.2 | 2026-09-29 | FR-21: main leaderboard = non-exclusive `SIGNAL` rows; separate "Exclusive reports" panel for `haldane_applied = true` pairs (D8). Success Metrics: "Row retention ≥ 95%" replaced by "`silver_cases` contains exactly one row per distinct `caseid` (verified: 397,224 = 397,224)" | Haldane-corrected PRRs for exclusive pairs are not interpretable magnitudes and would fill every top slot. The old retention metric penalized correct dedup and fan-out removal. See DECISIONS.md D8 |
| 2.3 | 2026-09-30 | FR-21: implements the amended D8. Power BI derives `is_exclusive` (`case_count = event_total`); exclusive panel = `SIGNAL` AND `is_exclusive` (106); main leaderboard = `SIGNAL` AND NOT `is_exclusive` (49,878), `haldane_applied` as a tooltip flag. Scope: patient-level (cross-report) duplicate detection added to Out of Scope; new Future Enhancements list. Footer version corrected. Success metrics unchanged | Filtering the exclusive panel on `haldane_applied` would have mislabeled 53 `SIGNAL` pairs that are not exclusive (`b = 0`, not `c = 0`; source query not saved, see DEVELOPER Issue 22). The case-level review showed `case_count` counts reports, not patients (D9). See DECISIONS.md D8 (amended) and D9 |
| 2.4 | 2026-10-05 | Executive Summary and Success Metrics: extensibility reworded to "designed for; verified on 2026Q1 only"; Scope bullet and Constraints "Scope" row note the same. OBJ-03, Scope bullet and Success Metrics: PRR/ROR computed for all pairs, the Evans minimum applies to classification. Key Relationships and glossary: `primaryid` for joins, `caseid` is the dedup key (D4). Assumptions and Risks: schema validation marked planned, not implemented. Constraints: added "Unit of count" row (D9). | Second-quarter verification is deferred until after release, and the BRD must not claim it. Schema validation (I4) is not implemented |
| 2.5 | 2026-10-06 | NFR-01: per-stage start, end and duration are recorded in UTC in the `99_run_pipeline.py` output (in-memory, not persisted). The 60-minute limit is unchanged. | T6 verified in a Databricks run on 2026-10-06; see DEVELOPER Issue 25, DECISIONS D10 |
