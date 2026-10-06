# Data Dictionary

**Project:** PharmaSignalLake
**Version:** 1.3
**Date:** May 2026 (Revised 2026-10-06)

> **Revision 1.3** — Condensed for readability; no schema changes. Bronze tables list only their own columns (shared keys and types stated once), tiny code sets are inline, and the illustrative `gold_demographics` counts were removed (they exceeded the 397,224 cases in 2026Q1).
> **Revision 1.2** — Dedup and join-key wording aligned to D4 (dedup on `caseid`; join on `primaryid`).
> **Revision 1.1** — `gold_signals` gained `chi_sq`/`haldane_applied` and a binary `signal_flag` (D1–D3); `silver_cases` is one row per case with multi-valued `outc_cod`/`rpsr_cod` (D7; `DEVELOPER.md` Issues 13–14).

The schema reference for every table in the pipeline: Bronze source files, Silver integration tables, Gold analytics outputs.

## Table of Contents

1. [Source Files — Bronze Layer](#1-source-files--bronze-layer)
2. [Silver Tables](#2-silver-tables)
3. [Gold Tables](#3-gold-tables)
4. [Code Reference Tables](#4-code-reference-tables)
5. [Key Relationships](#5-key-relationships)
6. [Data Quality Notes](#6-data-quality-notes)

---

## 1. Source Files — Bronze Layer

Conventions for every Bronze table:
- **All source columns are STRING.** No casting at this layer.
- **Audit columns added at ingestion:** `load_timestamp` (TIMESTAMP, UTC) and `source_quarter` (STRING, e.g. `"2026Q1"`).
- **Every file except DEMO also carries** `primaryid` (links to DEMO) and `caseid` (base case identifier). They are not repeated in the tables below.
- **Non-null columns** are named under each table. Everything else is nullable.
- **File names** use the 2-digit year, e.g. `DEMO26Q1.txt` (DEVELOPER Issue 4).

### 1.1 DEMO — Demographics

**File:** `DEMO{QUARTER}.txt` · **Bronze table:** `bronze_demo` · **Grain:** one row per case report version (`primaryid`) · **Non-null:** `primaryid`, `caseid`, `caseversion`

| Column | Description |
|--------|-------------|
| `primaryid` | Unique identifier for a case version: `{caseid}{caseversion}`. Use for joins; `caseid` is the case key used for dedup (D4). |
| `caseid` | Base case identifier. A case can have several versions, so `caseid` alone is not unique. |
| `caseversion` | Version number of this case report. Higher = more recent. |
| `i_f_code` | Initial (`I`) or Follow-up (`F`). See [Section 4.7](#47-i_f_code--initialfollow-up). |
| `event_dt` | Date the event occurred, YYYYMMDD (may be partial: YYYY or YYYYMM). |
| `mfr_dt` | Date the manufacturer received the report, YYYYMMDD. |
| `init_fda_dt` | Date the case was first submitted to the FDA, YYYYMMDD. |
| `fda_dt` | Date the case was entered into FAERS, YYYYMMDD. |
| `rept_cod` | Report type. See [Section 4.8](#48-rept_cod--report-type). |
| `auth_num` | Authorization number (regulatory use). |
| `mfr_num` | Manufacturer's internal case reference. |
| `mfr_sndr` | Manufacturer that submitted the report. |
| `lit_ref` | Literature reference, if the case came from a published paper. |
| `age` | Patient age as a numeric string; interpret with `age_cod`. |
| `age_cod` | Unit for `age`. See [Section 4.5](#45-age_cod--age-unit). |
| `age_grp` | FDA age group code (E = Elderly, A = Adult, T = Teen, …). Less reliable than computing from `age` + `age_cod`. Raw letter codes need decoding in Power BI. |
| `sex` | Patient sex. See [Section 4.6](#46-sex--patient-sex). |
| `e_sub` | Electronic submission flag (`Y`/`N`). |
| `wt` / `wt_cod` | Patient weight (numeric string) and its unit (`KG`, `LBS`). |
| `rept_dt` | Date this version was submitted, YYYYMMDD. |
| `to_mfr` | Date the FDA forwarded the report to the manufacturer. |
| `occp_cod` | Reporter occupation. See [Section 4.4](#44-occp_cod--reporter-occupation). |
| `reporter_country` | ISO country code of the reporter. |
| `occr_country` | ISO country code where the event occurred; may differ from `reporter_country`. |

### 1.2 DRUG — Drug Information

**File:** `DRUG{QUARTER}.txt` · **Bronze table:** `bronze_drug` · **Grain:** one row per drug per case (`primaryid` + `drug_seq`) · **Non-null:** `primaryid`, `caseid`, `drug_seq`

| Column | Description |
|--------|-------------|
| `drug_seq` | Sequence number of the drug within a case. Links to `dsg_drug_seq` in THER and `indi_drug_seq` in INDI. |
| `role_cod` | Suspected role. See [Section 4.1](#41-role_cod--drug-role). Signal detection uses `PS` only. |
| `drugname` | Drug name as reported: free text, inconsistent spelling. |
| `prod_ai` | Active ingredient(s). More standardized than `drugname`, still needs normalization. |
| `val_vbm` | Drug name validated against the FDA database (`1` = validated). |
| `route` | Route of administration (e.g. `Oral`, `Intravenous`). |
| `dose_vbm` | Dose as reported (free text). |
| `cum_dose_chr` / `cum_dose_unit` | Cumulative dose (free text) and its unit. |
| `dechal` | Dechallenge: did the reaction stop when the drug was withdrawn? `Y` / `N` / `U`. |
| `rechal` | Rechallenge: did the reaction recur when restarted? `Y` / `N` / `U`. |
| `lot_num` / `exp_dt` | Lot number and expiry date. Rarely populated. |
| `nda_num` | FDA NDA number. |
| `dose_amt` / `dose_unit` | Numeric dose and its unit (`MG`, `MCG`, `ML`). |
| `dose_form` | Dosage form (`Tablet`, `Capsule`, `Solution`). |
| `dose_freq` | Frequency (`QD`, `BID`, `TID`). |

### 1.3 REAC — Adverse Reactions

**File:** `REAC{QUARTER}.txt` · **Bronze table:** `bronze_reac` · **Grain:** one row per reaction per case (`primaryid` + `pt`) · **Non-null:** `primaryid`, `caseid`

| Column | Description |
|--------|-------------|
| `pt` | MedDRA Preferred Term for the reaction. The primary field for signal detection. |
| `drug_rec_act` | Action taken in response (e.g. `Drug withdrawn`, `Dose reduced`). |

### 1.4 INDI — Drug Indications

**File:** `INDI{QUARTER}.txt` · **Bronze table:** `bronze_indi` · **Grain:** one row per indication per drug per case · **Join key:** `primaryid` + `indi_drug_seq` · **Non-null:** `primaryid`, `caseid`, `indi_drug_seq`

| Column | Description |
|--------|-------------|
| `indi_drug_seq` | Links to `drug_seq` in DRUG: which drug this indication applies to. |
| `indi_pt` | MedDRA term for the indication (why the drug was taken). Common value: `Product used for unknown indication`. |

INDI is ingested to Bronze but not read by Silver.

### 1.5 OUTC — Case Outcomes

**File:** `OUTC{QUARTER}.txt` · **Bronze table:** `bronze_outc` · **Grain:** one row per outcome per case · **Non-null:** `primaryid`, `caseid`

| Column | Description |
|--------|-------------|
| `outc_cod` | Outcome code. See [Section 4.2](#42-outc_cod--case-outcome). A case can have several rows (e.g. `HO` and `LT`). |

### 1.6 RPSR — Report Sources

**File:** `RPSR{QUARTER}.txt` · **Bronze table:** `bronze_rpsr` · **Grain:** one row per report source per case · **Non-null:** `primaryid`, `caseid`

| Column | Description |
|--------|-------------|
| `rpsr_cod` | Who submitted the report. See [Section 4.3](#43-rpsr_cod--report-source). |

### 1.7 THER — Therapy Dates

**File:** `THER{QUARTER}.txt` · **Bronze table:** `bronze_ther` · **Grain:** one row per therapy period per case (`primaryid` + `dsg_drug_seq`) · **Join key:** `primaryid` + `dsg_drug_seq` · **Non-null:** `primaryid`, `caseid`, `dsg_drug_seq`

| Column | Description |
|--------|-------------|
| `dsg_drug_seq` | Links to `drug_seq` in DRUG: which drug this period applies to. |
| `start_dt` | Therapy start, YYYYMMDD (may be partial). |
| `end_dt` | Therapy end, YYYYMMDD (may be partial). Null if ongoing at report time. |
| `dur` / `dur_cod` | Reported duration (numeric string) and unit (`YR`, `MON`, `WK`, `DY`; same codes as `age_cod`). |

---

## 2. Silver Tables

Silver tables are cleansed, deduplicated and integrated, with columns cast to proper types. Every Silver table carries two audit columns: `source_quarter` (STRING, from Bronze) and `silver_load_timestamp` (TIMESTAMP, UTC). Both are non-null.

### 2.1 silver_cases

**Built from:** DEMO (deduplicated) + OUTC + RPSR · **Grain:** one row per case (`primaryid`) · **Purpose:** case-level table for demographics and outcomes

| Column | Type | Nullable | Source | Notes |
|--------|------|----------|--------|-------|
| `primaryid` | STRING | No | DEMO | Join key |
| `caseid` | STRING | No | DEMO | |
| `caseversion` | STRING | No | DEMO | Highest version kept after dedup |
| `i_f_code` | STRING | Yes | DEMO | `I` / `F` |
| `event_dt` | STRING | Yes | DEMO | Not cast to date: too many partial values |
| `age` | STRING | Yes | DEMO | Original value |
| `age_cod` | STRING | Yes | DEMO | Original unit |
| `age_years` | DOUBLE | Yes | Derived | Age in years via the `age_cod` multiplier |
| `age_group` | STRING | No | Derived | `NEONATE_INFANT` / `CHILD` / `ADOLESCENT` / `ADULT` / `ELDERLY` / `UNKNOWN` |
| `age_quality` | STRING | No | Derived | `VALID` / `MISSING` / `MISSING_UNIT` / `INVALID_NEGATIVE` / `INVALID_OVER_120` |
| `sex` | STRING | Yes | DEMO | `M`, `F`, or null |
| `reporter_country` | STRING | Yes | DEMO | ISO code |
| `occr_country` | STRING | Yes | DEMO | ISO code |
| `occp_cod` | STRING | Yes | DEMO | Reporter occupation |
| `wt` / `wt_cod` | STRING | Yes | DEMO | Weight and unit; not normalized (too sparse) |
| `outc_cod` | STRING | Yes | OUTC | Comma-separated outcome codes (e.g. `DE,HO`); null if the case has no outcome record |
| `rpsr_cod` | STRING | Yes | RPSR | Comma-separated report source codes; null if no source record |
| `source_quarter`, `silver_load_timestamp` | STRING, TIMESTAMP | No | Audit | |

### 2.2 silver_drug_reactions

**Built from:** DRUG (PS only) + REAC · **Grain:** one row per (case, Primary Suspect drug, reaction) · **Purpose:** source for all PRR/ROR computation

| Column | Type | Nullable | Source | Notes |
|--------|------|----------|--------|-------|
| `primaryid` | STRING | No | DRUG | Join key |
| `drug_seq` | STRING | No | DRUG | Links to THER and INDI |
| `drugname` | STRING | Yes | DRUG | Uppercased, trimmed |
| `prod_ai` | STRING | Yes | DRUG | Uppercased, trimmed |
| `role_cod` | STRING | No | DRUG | Always `PS` |
| `route`, `dose_amt`, `dose_unit`, `dose_form`, `dose_freq` | STRING | Yes | DRUG | As reported |
| `pt` | STRING | Yes | REAC | MedDRA term, uppercased, trimmed |
| `drug_rec_act` | STRING | Yes | REAC | Action taken |
| `source_quarter`, `silver_load_timestamp` | STRING, TIMESTAMP | No | Audit | |

### 2.3 silver_therapy

**Built from:** THER · **Grain:** one row per therapy period per case (`primaryid` + `dsg_drug_seq`) · **Purpose:** exposure duration and therapy timelines

| Column | Type | Nullable | Source | Notes |
|--------|------|----------|--------|-------|
| `primaryid`, `caseid` | STRING | No | THER | |
| `dsg_drug_seq` | STRING | No | THER | Links to `drug_seq` in DRUG |
| `start_dt`, `end_dt` | STRING | Yes | THER | Original strings |
| `start_date` | DATE | Yes | Derived | Parsed from `start_dt`; null if invalid |
| `end_date` | DATE | Yes | Derived | Parsed from `end_dt`; null if invalid or absent |
| `duration_days` | INTEGER | Yes | Derived | `end_date − start_date`; null if either is missing |
| `dur`, `dur_cod` | STRING | Yes | THER | Reported duration and unit |
| `date_quality` | STRING | No | Derived | `VALID` / `MISSING_START` / `INVALID_START_FORMAT` / `INVALID_END_FORMAT` / `NO_END_DATE` / `END_BEFORE_START` |
| `source_quarter`, `silver_load_timestamp` | STRING, TIMESTAMP | No | Audit | |

---

## 3. Gold Tables

Gold tables are aggregated and analytics-ready, and feed Power BI. Every Gold table carries `source_quarter` (STRING) and `gold_load_timestamp` (TIMESTAMP, UTC), both non-null.

### 3.1 gold_signals

**Built from:** silver_drug_reactions · **Grain:** one row per (drug, reaction) pair per quarter · **Purpose:** PRR/ROR signal results, the primary output

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `drugname` | STRING | No | Standardized drug name (uppercase) |
| `pt` | STRING | No | MedDRA term (uppercase) |
| `case_count` | INTEGER | No | **A**: distinct cases with this drug AND this reaction (counts reports, not patients; D9) |
| `drug_total` | INTEGER | No | **B**: distinct cases with this drug (any reaction) |
| `event_total` | INTEGER | No | **C**: distinct cases with this reaction (any drug) |
| `total_reports` | INTEGER | No | **N**: distinct cases in the dataset |
| `prr` | DOUBLE | No | Proportional Reporting Ratio, 4 decimals. Haldane-corrected when any cell is zero. Never null. |
| `ror` | DOUBLE | No | Reporting Odds Ratio, 4 decimals. Same correction as `prr`. Never null. |
| `chi_sq` | DOUBLE | Yes | Yates-corrected χ², 4 decimals, on the raw (uncorrected) cells. Null if any marginal is zero. |
| `haldane_applied` | BOOLEAN | No | `true` if any cell (A, B−A, C−A, N−B−C+A) was zero and +0.5 was added to all four before `prr`/`ror`. |
| `signal_flag` | STRING | No | `SIGNAL` / `NO_SIGNAL`. See [Section 4.9](#49-signal_flag--signal-classification). |
| `source_quarter`, `gold_load_timestamp` | STRING, TIMESTAMP | No | Audit |

`is_exclusive` (`case_count = event_total`) is not a pipeline column. Power BI derives it (D8).

**Signal math:**

```
PRR = (A / B) / ((C - A) / (N - B))
ROR = (A / (B - A)) / ((C - A) / (N - B - C + A))
```

If any of the four cells (`A`, `B−A`, `C−A`, `N−B−C+A`) is zero, add 0.5 to all four before computing PRR/ROR. `chi_sq` always uses the raw cells.

### 3.2 gold_demographics

**Built from:** silver_cases · **Grain:** one row per (dimension, value) pair per quarter · **Purpose:** flat demographics table for Power BI slicers

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `dimension` | STRING | No | `AGE_GROUP`, `SEX`, `REPORTER_COUNTRY` or `OCCUPATION` |
| `dimension_value` | STRING | No | Value within the dimension (e.g. `ADULT`, `FEMALE`, `US`, `MD`) |
| `case_count` | BIGINT | No | Distinct cases with this value |
| `source_quarter`, `gold_load_timestamp` | STRING, TIMESTAMP | No | Audit |

### 3.3 gold_outcomes

**Built from:** silver_cases (`outc_cod` and `rpsr_cod` exploded to one row per code) · **Grain:** one row per (dimension, code) pair per quarter · **Purpose:** Case Outcomes dashboard page

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `dimension` | STRING | No | `OUTCOME` or `REPORTER_SOURCE` |
| `code` | STRING | No | Raw code (e.g. `DE`, `HO`, `HP`, `CSM`) |
| `label` | STRING | No | Readable label (e.g. `Death`, `Healthcare Professional`) |
| `case_count` | BIGINT | No | Distinct cases with this code |
| `source_quarter`, `gold_load_timestamp` | STRING, TIMESTAMP | No | Audit |

---

## 4. Code Reference Tables

### 4.1 role_cod — Drug Role

Source: `DRUG.role_cod`. Only `PS` enters `silver_drug_reactions`.

| Code | Label | Used in signal detection |
|------|-------|--------------------------|
| `PS` | Primary Suspect | Yes |
| `SS` | Secondary Suspect | No |
| `C` | Concomitant | No |
| `I` | Interacting | No |

### 4.2 outc_cod — Case Outcome

Source: `OUTC.outc_cod`. A case can have several outcomes (e.g. hospitalized and died).

| Code | Label | Severity |
|------|-------|----------|
| `DE` | Death | Critical |
| `LT` | Life-Threatening | Critical |
| `HO` | Hospitalization (initial or prolonged) | Serious |
| `DS` | Disability or Permanent Damage | Serious |
| `CA` | Congenital Anomaly / Birth Defect | Serious |
| `RI` | Required Intervention to Prevent Permanent Impairment | Serious |
| `OT` | Other Serious | Serious |

### 4.3 rpsr_cod — Report Source

Source: `RPSR.rpsr_cod`.

`HP` Healthcare Professional · `CSM` Consumer · `FGN` Foreign (outside the US) · `LIT` Literature · `UF` User Facility (hospital, clinic, nursing home) · `DDR` Drug Distributor · `OTH` Other

### 4.4 occp_cod — Reporter Occupation

Source: `DEMO.occp_cod`.

`MD` Physician · `PH` Pharmacist · `HP` Other Healthcare Professional · `LW` Lawyer · `CN` Consumer / Non-healthcare Professional · `OT` Other

### 4.5 age_cod — Age Unit

Source: `DEMO.age_cod`. Used to normalize `age` to years in Silver.

| Code | Label | To years |
|------|-------|----------|
| `YR` | Years | × 1 |
| `MON` | Months | ÷ 12 |
| `WK` | Weeks | ÷ 52 |
| `DY` | Days | ÷ 365 |
| `HR` | Hours | ÷ 8,760 |
| `DEC` | Decades | × 10 |

### 4.6 sex — Patient Sex

Source: `DEMO.sex`. `M` Male · `F` Female · `UNK` Unknown · null = not reported

### 4.7 i_f_code — Initial / Follow-up

Source: `DEMO.i_f_code`. `I` Initial (first submission) · `F` Follow-up (update to an earlier submission)

### 4.8 rept_cod — Report Type

Source: `DEMO.rept_cod`. `EXP` Expedited (15-day) · `PER` Periodic · `DIR` Direct from consumer · `VOL` Voluntary

### 4.9 signal_flag — Signal Classification

Source: `gold_signals.signal_flag` (derived).

| Value | Rule | Meaning |
|-------|------|---------|
| `SIGNAL` | `case_count >= 3` AND `prr >= 2.0` AND `chi_sq (Yates) >= 4.0` | Meets full Evans Criteria; warrants clinical review |
| `NO_SIGNAL` | All other pairs | No disproportionate reporting, or fails the χ² test |

---

## 5. Key Relationships

```
DEMO (1) ── (M) DRUG   via primaryid
DEMO (1) ── (M) REAC   via primaryid
DEMO (1) ── (M) INDI   via primaryid + indi_drug_seq → drug_seq
DEMO (1) ── (M) OUTC   via primaryid
DEMO (1) ── (M) RPSR   via primaryid
DEMO (1) ── (M) THER   via primaryid + dsg_drug_seq → drug_seq
DRUG (1) ── (1) THER   via primaryid + drug_seq = dsg_drug_seq
DRUG (1) ── (M) INDI   via primaryid + drug_seq = indi_drug_seq
```

**Rule:** join on `primaryid`. `caseid` is the case key used for dedup (D4). One `caseid` maps to many `primaryid`s (versions), so joining on `caseid` without deduplicating produces duplicate rows.

---

## 6. Data Quality Notes

Known characteristics of FAERS data and how the pipeline handles them.

| Issue | Fields | Pipeline handling |
|-------|--------|-------------------|
| Duplicate case versions | All files via `primaryid` | Keep max `caseversion` per `caseid`; child tables semi-joined to surviving `primaryid`s (D4) |
| Inconsistent drug names | `DRUG.drugname`, `prod_ai` | UPPER + TRIM. No drug vocabulary (WHO Drug / RxNorm); known limitation, documented in README |
| Inconsistent reaction terms | `REAC.pt` | UPPER + TRIM |
| Partial dates | `DEMO.event_dt`, `THER.start_dt`, `THER.end_dt` | Flagged in `date_quality`; not dropped |
| Missing age units | `DEMO.age_cod` | `MISSING_UNIT` in `age_quality`; `age_years` null |
| Extreme ages | `DEMO.age` | `INVALID_OVER_120` / `INVALID_NEGATIVE`; not dropped |
| Multiple outcomes or sources per case | `OUTC.outc_cod`, `RPSR.rpsr_cod` | Comma-separated per case in `silver_cases`; exploded to one row per code in `gold_outcomes` |
| PRR/ROR zero-cell edge cases | `gold_signals` | Haldane-Anscombe correction when any cell is zero; see `haldane_applied`. No null PRR/ROR remain |
| Reports are not patients | `gold_signals.case_count` | Not handled in the pipeline (D9); manual case review for top signals |

---

*End of Data Dictionary — Version 1.3 — PharmaSignalLake*
