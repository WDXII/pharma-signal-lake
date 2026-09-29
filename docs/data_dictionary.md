# Data Dictionary

**Project:** PharmaSignalLake
**Version:** 1.1
**Date:** May 2026 (Revised 2026-09-29)

> **Revision 1.1** — Updated `gold_signals` (added `chi_sq`/`haldane_applied`, binary
> `signal_flag`), Section 4.9, and Section 6 to match `DECISIONS.md` D1-D3; updated
> `silver_cases`' grain and `outc_cod`/`rpsr_cod` columns to match D7 (see `DEVELOPER.md`
> Issues 13-14).

This document is the authoritative schema reference for every table in the pipeline —
from raw Bronze source files through Silver integration to Gold analytics outputs.

---

## Table of Contents

1. [Source Files — Bronze Layer](#1-source-files--bronze-layer)
   - [DEMO — Demographics](#11-demo--demographics)
   - [DRUG — Drug Information](#12-drug--drug-information)
   - [REAC — Adverse Reactions](#13-reac--adverse-reactions)
   - [INDI — Drug Indications](#14-indi--drug-indications)
   - [OUTC — Case Outcomes](#15-outc--case-outcomes)
   - [RPSR — Report Sources](#16-rpsr--report-sources)
   - [THER — Therapy Dates](#17-ther--therapy-dates)
2. [Silver Tables](#2-silver-tables)
   - [silver_cases](#21-silver_cases)
   - [silver_drug_reactions](#22-silver_drug_reactions)
   - [silver_therapy](#23-silver_therapy)
3. [Gold Tables](#3-gold-tables)
   - [gold_signals](#31-gold_signals)
   - [gold_demographics](#32-gold_demographics)
   - [gold_outcomes](#33-gold_outcomes)
4. [Code Reference Tables](#4-code-reference-tables)
   - [role_cod — Drug Role](#41-role_cod--drug-role)
   - [outc_cod — Case Outcome](#42-outc_cod--case-outcome)
   - [rpsr_cod — Report Source](#43-rpsr_cod--report-source)
   - [occp_cod — Reporter Occupation](#44-occp_cod--reporter-occupation)
   - [age_cod — Age Unit](#45-age_cod--age-unit)
   - [sex — Patient Sex](#46-sex--patient-sex)
   - [i_f_code — Initial/Follow-up](#47-i_f_code--initialfollow-up)
   - [rept_cod — Report Type](#48-rept_cod--report-type)
   - [signal_flag — Signal Classification](#49-signal_flag--signal-classification)
5. [Key Relationships](#5-key-relationships)
6. [Data Quality Notes](#6-data-quality-notes)

---

## 1. Source Files — Bronze Layer

All Bronze tables inherit two additional audit columns added at ingestion:

| Audit Column | Type | Description |
|---|---|---|
| `load_timestamp` | TIMESTAMP | UTC timestamp when the row was ingested into Bronze |
| `source_quarter` | STRING | Quarter identifier, e.g. `"2026Q1"` |

All source columns land as **STRING** type at Bronze — no casting is applied at this layer.

---

### 1.1 DEMO — Demographics

**File:** `DEMO{QUARTER}.txt`
**Bronze table:** `bronze_demo`
**Grain:** One row per case report version (`primaryid`)
**Join key:** `primaryid`

| Column | Type (Bronze) | Nullable | Description |
|--------|--------------|----------|-------------|
| `primaryid` | STRING | No | Unique identifier for a case version. Format: `{caseid}{caseversion}`. Always use this for joins — not `caseid`. |
| `caseid` | STRING | No | Base case identifier. A case can have multiple versions; `caseid` alone is not unique. |
| `caseversion` | STRING | No | Version number of this case report. Higher = more recent update. |
| `i_f_code` | STRING | Yes | Initial (`I`) or Follow-up (`F`) report. See [Section 4.7](#47-i_f_code--initialfollow-up). |
| `event_dt` | STRING | Yes | Date the adverse event occurred. Format: YYYYMMDD (may be partial, e.g. YYYY or YYYYMM). |
| `mfr_dt` | STRING | Yes | Date the manufacturer received the report. Format: YYYYMMDD. |
| `init_fda_dt` | STRING | Yes | Date the case was first submitted to the FDA. Format: YYYYMMDD. |
| `fda_dt` | STRING | Yes | Date the case was entered into the FAERS database. Format: YYYYMMDD. |
| `rept_cod` | STRING | Yes | Report type code. See [Section 4.8](#48-rept_cod--report-type). |
| `auth_num` | STRING | Yes | Authorization number for the report (regulatory use). |
| `mfr_num` | STRING | Yes | Manufacturer's internal case reference number. |
| `mfr_sndr` | STRING | Yes | Manufacturer name that submitted the report. |
| `lit_ref` | STRING | Yes | Literature reference if the case originated from a published paper. |
| `age` | STRING | Yes | Patient age as a numeric string. Must be interpreted with `age_cod`. |
| `age_cod` | STRING | Yes | Unit for the `age` value. See [Section 4.5](#45-age_cod--age-unit). |
| `age_grp` | STRING | Yes | FDA-assigned age group code (E = Elderly, A = Adult, T = Teen, etc.). Less reliable than computing from `age` + `age_cod`. |
| `sex` | STRING | Yes | Patient sex. See [Section 4.6](#46-sex--patient-sex). |
| `e_sub` | STRING | Yes | Electronic submission flag (`Y`/`N`). |
| `wt` | STRING | Yes | Patient weight as a numeric string. Interpret with `wt_cod`. |
| `wt_cod` | STRING | Yes | Unit for `wt`. Common values: `KG`, `LBS`. |
| `rept_dt` | STRING | Yes | Date this version of the report was submitted. Format: YYYYMMDD. |
| `to_mfr` | STRING | Yes | Date FDA forwarded the report to the manufacturer. |
| `occp_cod` | STRING | Yes | Reporter's occupation code. See [Section 4.4](#44-occp_cod--reporter-occupation). |
| `reporter_country` | STRING | Yes | ISO country code of the reporter. |
| `occr_country` | STRING | Yes | ISO country code where the adverse event occurred. May differ from `reporter_country`. |

---

### 1.2 DRUG — Drug Information

**File:** `DRUG{QUARTER}.txt`
**Bronze table:** `bronze_drug`
**Grain:** One row per drug per case (`primaryid` + `drug_seq`)
**Join key:** `primaryid`

| Column | Type (Bronze) | Nullable | Description |
|--------|--------------|----------|-------------|
| `primaryid` | STRING | No | Links to DEMO. |
| `caseid` | STRING | No | Base case identifier. |
| `drug_seq` | STRING | No | Sequential number identifying each drug within a case. Links to `dsg_drug_seq` in THER and `indi_drug_seq` in INDI. |
| `role_cod` | STRING | Yes | Drug's suspected role in the adverse event. See [Section 4.1](#41-role_cod--drug-role). Signal detection uses `PS` only. |
| `drugname` | STRING | Yes | Drug name as reported — free text, no controlled vocabulary. Highly inconsistent spelling across reports. |
| `prod_ai` | STRING | Yes | Active ingredient(s) of the drug. More standardized than `drugname` but still requires normalization. |
| `val_vbm` | STRING | Yes | Validation flag for drug name against FDA database (`1` = validated). |
| `route` | STRING | Yes | Route of administration (e.g., `Oral`, `Intravenous`, `Subcutaneous`). |
| `dose_vbm` | STRING | Yes | Dose as reported — free text string. |
| `cum_dose_chr` | STRING | Yes | Cumulative dose (character/free text). |
| `cum_dose_unit` | STRING | Yes | Unit for cumulative dose. |
| `dechal` | STRING | Yes | Dechallenge — did the reaction stop when the drug was withdrawn? `Y` = yes, `N` = no, `U` = unknown. |
| `rechal` | STRING | Yes | Rechallenge — did the reaction recur when the drug was restarted? `Y` = yes, `N` = no, `U` = unknown. |
| `lot_num` | STRING | Yes | Drug lot/batch number. Rarely populated. |
| `exp_dt` | STRING | Yes | Drug expiry date. Rarely populated. |
| `nda_num` | STRING | Yes | FDA NDA (New Drug Application) number for the drug. |
| `dose_amt` | STRING | Yes | Numeric dose amount. |
| `dose_unit` | STRING | Yes | Unit for `dose_amt` (e.g., `MG`, `MCG`, `ML`). |
| `dose_form` | STRING | Yes | Dosage form (e.g., `Tablet`, `Capsule`, `Solution`). |
| `dose_freq` | STRING | Yes | Dosing frequency (e.g., `QD` = once daily, `BID` = twice daily, `TID` = three times daily). |

---

### 1.3 REAC — Adverse Reactions

**File:** `REAC{QUARTER}.txt`
**Bronze table:** `bronze_reac`
**Grain:** One row per adverse reaction per case (`primaryid` + `pt`)
**Join key:** `primaryid`

| Column | Type (Bronze) | Nullable | Description |
|--------|--------------|----------|-------------|
| `primaryid` | STRING | No | Links to DEMO. |
| `caseid` | STRING | No | Base case identifier. |
| `pt` | STRING | Yes | MedDRA Preferred Term — the standardized medical term for the adverse reaction. This is the primary field used in signal detection. |
| `drug_rec_act` | STRING | Yes | Drug reaction action taken — what was done in response to the reaction (e.g., `Drug withdrawn`, `Dose reduced`). |

---

### 1.4 INDI — Drug Indications

**File:** `INDI{QUARTER}.txt`
**Bronze table:** `bronze_indi`
**Grain:** One row per indication per drug per case
**Join key:** `primaryid` + `indi_drug_seq`

| Column | Type (Bronze) | Nullable | Description |
|--------|--------------|----------|-------------|
| `primaryid` | STRING | No | Links to DEMO. |
| `caseid` | STRING | No | Base case identifier. |
| `indi_drug_seq` | STRING | No | Links to `drug_seq` in DRUG — identifies which drug this indication applies to. |
| `indi_pt` | STRING | Yes | MedDRA Preferred Term for the indication (the reason the drug was taken). Common value: `Product used for unknown indication`. |

---

### 1.5 OUTC — Case Outcomes

**File:** `OUTC{QUARTER}.txt`
**Bronze table:** `bronze_outc`
**Grain:** One row per outcome per case (a case can have multiple outcomes)
**Join key:** `primaryid`

| Column | Type (Bronze) | Nullable | Description |
|--------|--------------|----------|-------------|
| `primaryid` | STRING | No | Links to DEMO. |
| `caseid` | STRING | No | Base case identifier. |
| `outc_cod` | STRING | Yes | Outcome code for this case. See [Section 4.2](#42-outc_cod--case-outcome). A case can have multiple rows here (e.g., both `HO` and `LT`). |

---

### 1.6 RPSR — Report Sources

**File:** `RPSR{QUARTER}.txt`
**Bronze table:** `bronze_rpsr`
**Grain:** One row per report source per case (a case can have multiple sources)
**Join key:** `primaryid`

| Column | Type (Bronze) | Nullable | Description |
|--------|--------------|----------|-------------|
| `primaryid` | STRING | No | Links to DEMO. |
| `caseid` | STRING | No | Base case identifier. |
| `rpsr_cod` | STRING | Yes | Report source code — who submitted the report. See [Section 4.3](#43-rpsr_cod--report-source). |

---

### 1.7 THER — Therapy Dates

**File:** `THER{QUARTER}.txt`
**Bronze table:** `bronze_ther`
**Grain:** One row per drug therapy period per case (`primaryid` + `dsg_drug_seq`)
**Join key:** `primaryid` + `dsg_drug_seq`

| Column | Type (Bronze) | Nullable | Description |
|--------|--------------|----------|-------------|
| `primaryid` | STRING | No | Links to DEMO. |
| `caseid` | STRING | No | Base case identifier. |
| `dsg_drug_seq` | STRING | No | Links to `drug_seq` in DRUG — identifies which drug this therapy period applies to. |
| `start_dt` | STRING | Yes | Therapy start date. Format: YYYYMMDD (may be partial). |
| `end_dt` | STRING | Yes | Therapy end date. Format: YYYYMMDD (may be partial). Null if therapy was ongoing at time of report. |
| `dur` | STRING | Yes | Therapy duration as reported (numeric string). Interpret with `dur_cod`. |
| `dur_cod` | STRING | Yes | Unit for `dur`. Common values: `YR`, `MON`, `WK`, `DY`. Same codes as `age_cod`. |

---

## 2. Silver Tables

Silver tables are **cleansed, deduplicated, and integrated**. All columns are cast to appropriate types.
Every Silver table includes:

| Audit Column | Type | Description |
|---|---|---|
| `source_quarter` | STRING | Quarter identifier carried from Bronze |
| `silver_load_timestamp` | TIMESTAMP | UTC timestamp when the row was written to Silver |

---

### 2.1 silver_cases

**Built from:** DEMO + OUTC + RPSR
**Grain:** One row per case (`primaryid`)
**Purpose:** Unified case-level table for demographic analysis and outcome reporting

| Column | Type | Nullable | Source | Notes |
|--------|------|----------|--------|-------|
| `primaryid` | STRING | No | DEMO | Primary join key |
| `caseid` | STRING | No | DEMO | |
| `caseversion` | STRING | No | DEMO | Highest version retained after deduplication |
| `i_f_code` | STRING | Yes | DEMO | `I` = Initial, `F` = Follow-up |
| `event_dt` | STRING | Yes | DEMO | Not cast to date — too many partial values |
| `age` | STRING | Yes | DEMO | Original value retained |
| `age_cod` | STRING | Yes | DEMO | Original unit retained |
| `age_years` | DOUBLE | Yes | Derived | Age normalized to years using `age_cod` multiplier |
| `age_group` | STRING | No | Derived | `NEONATE_INFANT` / `CHILD` / `ADOLESCENT` / `ADULT` / `ELDERLY` / `UNKNOWN` |
| `age_quality` | STRING | No | Derived | `VALID` / `MISSING` / `MISSING_UNIT` / `INVALID_NEGATIVE` / `INVALID_OVER_120` |
| `sex` | STRING | Yes | DEMO | `M`, `F`, or null |
| `reporter_country` | STRING | Yes | DEMO | ISO country code |
| `occr_country` | STRING | Yes | DEMO | ISO country code |
| `occp_cod` | STRING | Yes | DEMO | Reporter occupation code |
| `wt` | STRING | Yes | DEMO | Weight — not normalized (too sparse) |
| `wt_cod` | STRING | Yes | DEMO | Weight unit |
| `outc_cod` | STRING | Yes | OUTC | Comma-separated list of outcome codes if the case has more than one (e.g. `DE,HO`); null if no outcome record exists for this case |
| `rpsr_cod` | STRING | Yes | RPSR | Comma-separated list of report source codes if the case has more than one; null if no source record exists for this case |
| `source_quarter` | STRING | No | Audit | |
| `silver_load_timestamp` | TIMESTAMP | No | Audit | |

---

### 2.2 silver_drug_reactions

**Built from:** DRUG (PS only) + REAC
**Grain:** One row per (case, Primary Suspect drug, adverse reaction) combination
**Purpose:** Source table for all PRR/ROR signal detection computations

| Column | Type | Nullable | Source | Notes |
|--------|------|----------|--------|-------|
| `primaryid` | STRING | No | DRUG | Primary join key |
| `drug_seq` | STRING | No | DRUG | Links to THER and INDI |
| `drugname` | STRING | Yes | DRUG | Uppercased and trimmed |
| `prod_ai` | STRING | Yes | DRUG | Uppercased and trimmed |
| `role_cod` | STRING | No | DRUG | Always `PS` in this table |
| `route` | STRING | Yes | DRUG | Route of administration |
| `dose_amt` | STRING | Yes | DRUG | |
| `dose_unit` | STRING | Yes | DRUG | |
| `dose_form` | STRING | Yes | DRUG | |
| `dose_freq` | STRING | Yes | DRUG | |
| `pt` | STRING | Yes | REAC | MedDRA Preferred Term — uppercased and trimmed |
| `drug_rec_act` | STRING | Yes | REAC | Action taken in response to the reaction |
| `source_quarter` | STRING | No | Audit | |
| `silver_load_timestamp` | TIMESTAMP | No | Audit | |

---

### 2.3 silver_therapy

**Built from:** THER
**Grain:** One row per drug therapy period per case (`primaryid` + `dsg_drug_seq`)
**Purpose:** Exposure duration analysis; supports therapy timeline visualizations

| Column | Type | Nullable | Source | Notes |
|--------|------|----------|--------|-------|
| `primaryid` | STRING | No | THER | |
| `caseid` | STRING | No | THER | |
| `dsg_drug_seq` | STRING | No | THER | Links to `drug_seq` in DRUG |
| `start_dt` | STRING | Yes | THER | Original string retained |
| `end_dt` | STRING | Yes | THER | Original string retained |
| `start_date` | DATE | Yes | Derived | Parsed from `start_dt` (YYYYMMDD). Null if format invalid. |
| `end_date` | DATE | Yes | Derived | Parsed from `end_dt` (YYYYMMDD). Null if format invalid or no end date. |
| `duration_days` | INTEGER | Yes | Derived | `end_date - start_date` in days. Null if either date is missing. |
| `dur` | STRING | Yes | THER | Reported duration (numeric string) |
| `dur_cod` | STRING | Yes | THER | Reported duration unit |
| `date_quality` | STRING | No | Derived | `VALID` / `MISSING_START` / `INVALID_START_FORMAT` / `INVALID_END_FORMAT` / `NO_END_DATE` / `END_BEFORE_START` |
| `source_quarter` | STRING | No | Audit | |
| `silver_load_timestamp` | TIMESTAMP | No | Audit | |

---

## 3. Gold Tables

Gold tables are **aggregated and analytics-ready**. They feed directly into Power BI.
Every Gold table includes:

| Audit Column | Type | Description |
|---|---|---|
| `source_quarter` | STRING | Quarter identifier |
| `gold_load_timestamp` | TIMESTAMP | UTC timestamp when the row was written to Gold |

---

### 3.1 gold_signals

**Built from:** silver_drug_reactions
**Grain:** One row per unique (drug, reaction) pair per quarter
**Purpose:** PRR/ROR signal detection results — primary output of the pipeline

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `drugname` | STRING | No | Standardized drug name (uppercase) |
| `pt` | STRING | No | MedDRA Preferred Term (uppercase) |
| `case_count` | INTEGER | No | **A** — number of distinct cases reporting this drug AND this reaction |
| `drug_total` | INTEGER | No | **B** — total distinct cases reporting this drug (any reaction) |
| `event_total` | INTEGER | No | **C** — total distinct cases reporting this reaction (any drug) |
| `total_reports` | INTEGER | No | **N** — total distinct cases in the entire dataset |
| `prr` | DOUBLE | No | Proportional Reporting Ratio. Haldane-Anscombe corrected when any contingency-table cell is zero (see `haldane_applied`) — never null. Rounded to 4 decimal places. |
| `ror` | DOUBLE | No | Reporting Odds Ratio. Same Haldane-Anscombe correction as `prr` — never null. Rounded to 4 decimal places. |
| `chi_sq` | DOUBLE | Yes | Yates-corrected chi-squared statistic, computed on the raw (non-Haldane-adjusted) cells. Null if any of its four marginals is zero. Rounded to 4 decimal places. |
| `haldane_applied` | BOOLEAN | No | `true` if any of the four contingency-table cells (A, B-A, C-A, N-B-C+A) was zero and the Haldane-Anscombe correction (+0.5 to all four cells) was applied before computing `prr`/`ror`. |
| `signal_flag` | STRING | No | `SIGNAL` / `NO_SIGNAL` (binary). See [Section 4.9](#49-signal_flag--signal-classification). |
| `source_quarter` | STRING | No | Audit |
| `gold_load_timestamp` | TIMESTAMP | No | Audit |

**Signal math reference:**

```
PRR = (A / B) / ((C - A) / (N - B))
ROR = (A / (B - A)) / ((C - A) / (N - B - C + A))
```

If any of the four contingency-table cells (`A`, `B-A`, `C-A`, `N-B-C+A`) is zero, add
0.5 to all four cells (Haldane-Anscombe correction) before computing PRR/ROR above —
`chi_sq` always uses the raw, uncorrected cells.

---

### 3.2 gold_demographics

**Built from:** silver_cases
**Grain:** One row per (dimension, dimension_value) pair per quarter
**Purpose:** Flattened demographics table for Power BI slicer-friendly analysis

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `dimension` | STRING | No | Category of the demographic breakdown. Values: `AGE_GROUP`, `SEX`, `REPORTER_COUNTRY`, `OCCUPATION` |
| `dimension_value` | STRING | No | The specific value within that dimension (e.g., `ADULT`, `FEMALE`, `US`, `MD`) |
| `case_count` | BIGINT | No | Number of distinct cases with this dimension value |
| `source_quarter` | STRING | No | Audit |
| `gold_load_timestamp` | TIMESTAMP | No | Audit |

**Example rows:**

| dimension | dimension_value | case_count |
|---|---|---|
| AGE_GROUP | ADULT | 412,300 |
| SEX | FEMALE | 389,100 |
| REPORTER_COUNTRY | US | 510,000 |
| OCCUPATION | MD | 220,400 |

---

### 3.3 gold_outcomes

**Built from:** silver_cases
**Grain:** One row per (dimension, code) pair per quarter
**Purpose:** Outcome and reporter source distribution for the Case Outcomes dashboard page

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `dimension` | STRING | No | `OUTCOME` or `REPORTER_SOURCE` |
| `code` | STRING | No | Raw code value (e.g., `DE`, `HO`, `HP`, `CSM`) |
| `label` | STRING | No | Human-readable label (e.g., `Death`, `Hospitalization`, `Healthcare Professional`) |
| `case_count` | BIGINT | No | Number of distinct cases with this code |
| `source_quarter` | STRING | No | Audit |
| `gold_load_timestamp` | TIMESTAMP | No | Audit |

---

## 4. Code Reference Tables

### 4.1 role_cod — Drug Role

Source: `DRUG.role_cod`

| Code | Label | Used in Signal Detection |
|------|-------|--------------------------|
| `PS` | Primary Suspect | ✅ Yes — only PS drugs enter `silver_drug_reactions` |
| `SS` | Secondary Suspect | ❌ No |
| `C` | Concomitant | ❌ No |
| `I` | Interacting | ❌ No |

---

### 4.2 outc_cod — Case Outcome

Source: `OUTC.outc_cod`

| Code | Label | Severity |
|------|-------|----------|
| `DE` | Death | Critical |
| `LT` | Life-Threatening | Critical |
| `HO` | Hospitalization — Initial or Prolonged | Serious |
| `DS` | Disability or Permanent Damage | Serious |
| `CA` | Congenital Anomaly / Birth Defect | Serious |
| `RI` | Required Intervention to Prevent Permanent Impairment | Serious |
| `OT` | Other Serious | Serious |

> A single case can have multiple outcome records (e.g., a patient was both hospitalized and died).

---

### 4.3 rpsr_cod — Report Source

Source: `RPSR.rpsr_cod`

| Code | Label |
|------|-------|
| `HP` | Healthcare Professional |
| `CSM` | Consumer |
| `FGN` | Foreign (submitted from outside the US) |
| `LIT` | Literature (from a published medical paper) |
| `UF` | User Facility (hospital, clinic, nursing home) |
| `DDR` | Drug Distributor |
| `OTH` | Other |

---

### 4.4 occp_cod — Reporter Occupation

Source: `DEMO.occp_cod`

| Code | Label |
|------|-------|
| `MD` | Physician |
| `PH` | Pharmacist |
| `HP` | Other Healthcare Professional |
| `LW` | Lawyer |
| `CN` | Consumer / Non-healthcare Professional |
| `OT` | Other |

---

### 4.5 age_cod — Age Unit

Source: `DEMO.age_cod`
Used to normalize `DEMO.age` to years in Silver.

| Code | Label | Multiplier to Years |
|------|-------|---------------------|
| `YR` | Years | × 1.0 |
| `MON` | Months | ÷ 12 |
| `WK` | Weeks | ÷ 52 |
| `DY` | Days | ÷ 365 |
| `HR` | Hours | ÷ 8,760 |
| `DEC` | Decades | × 10 |

---

### 4.6 sex — Patient Sex

Source: `DEMO.sex`

| Code | Label |
|------|-------|
| `M` | Male |
| `F` | Female |
| `UNK` | Unknown |
| *(null)* | Not reported |

---

### 4.7 i_f_code — Initial / Follow-up

Source: `DEMO.i_f_code`

| Code | Label | Description |
|------|-------|-------------|
| `I` | Initial | First submission of this case |
| `F` | Follow-up | Update to a previously submitted case |

---

### 4.8 rept_cod — Report Type

Source: `DEMO.rept_cod`

| Code | Label |
|------|-------|
| `EXP` | Expedited Report (15-day) |
| `PER` | Periodic Report |
| `DIR` | Direct Report from Consumer |
| `VOL` | Voluntary Report |

---

### 4.9 signal_flag — Signal Classification

Source: `gold_signals.signal_flag` (derived)

| Value | Rule | Interpretation |
|-------|------|----------------|
| `SIGNAL` | `case_count >= 3` AND `prr >= 2.0` AND `chi_sq (Yates) >= 4.0` | Meets full Evans Criteria. Warrants clinical review. |
| `NO_SIGNAL` | All other pairs | No disproportionate reporting detected, or fails the chi-squared significance test. |

---

## 5. Key Relationships

```
DEMO (1) ──────────── (M) DRUG       via primaryid
DEMO (1) ──────────── (M) REAC       via primaryid
DEMO (1) ──────────── (M) INDI       via primaryid + indi_drug_seq → drug_seq
DEMO (1) ──────────── (M) OUTC       via primaryid
DEMO (1) ──────────── (M) RPSR       via primaryid
DEMO (1) ──────────── (M) THER       via primaryid + dsg_drug_seq → drug_seq
DRUG (1) ──────────── (1) THER       via primaryid + drug_seq = dsg_drug_seq
DRUG (1) ──────────── (M) INDI       via primaryid + drug_seq = indi_drug_seq
```

**Critical rule:** Always join on `primaryid`, never on `caseid` alone.
`caseid` is a many-to-one relationship with `primaryid` (one case, multiple versions).
Joining on `caseid` without deduplication produces duplicate rows.

---

## 6. Data Quality Notes

These are known characteristics of FAERS data. All are documented in the pipeline.

| Issue | Affected Fields | Pipeline Handling |
|-------|----------------|-------------------|
| **Duplicate case versions** | All files via `primaryid` | Silver deduplication: keep max `caseversion` per `primaryid` |
| **Inconsistent drug names** | `DRUG.drugname`, `DRUG.prod_ai` | Silver normalization: UPPER + TRIM |
| **Inconsistent reaction terms** | `REAC.pt` | Silver normalization: UPPER + TRIM |
| **Partial dates** | `DEMO.event_dt`, `THER.start_dt`, `THER.end_dt` | Flagged in `date_quality`; not dropped |
| **Missing age units** | `DEMO.age_cod` | Flagged as `MISSING_UNIT` in `age_quality`; `age_years` set to null |
| **Extreme age values** | `DEMO.age` | Flagged as `INVALID_OVER_120` or `INVALID_NEGATIVE`; not dropped |
| **Multiple outcomes per case** | `OUTC.outc_cod` | Aggregated to a comma-separated list per case in Silver (`silver_cases.outc_cod`); exploded back to one row per code in `gold_outcomes` |
| **Multiple sources per case** | `RPSR.rpsr_cod` | Same handling as outcomes — comma-separated in `silver_cases.rpsr_cod`, exploded in `gold_outcomes` |
| **No drug vocabulary control** | `DRUG.drugname` | Known limitation. Production systems use WHO drug dictionary. Documented in README. |
| **PRR/ROR denominator edge cases** | `gold_signals` | Haldane-Anscombe correction (+0.5 to all four contingency-table cells) applied when any cell is zero, before computing PRR/ROR; see `haldane_applied`. No null PRR/ROR values remain. |

---

*End of Data Dictionary — Version 1.1 — PharmaSignalLake*
