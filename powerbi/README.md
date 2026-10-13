# Power BI Dashboard

The Power BI report for PharmaSignalLake. It reads the three Gold-layer CSV exports and presents drug–reaction safety signals from FDA FAERS adverse-event reports (2026Q1).

> **Status: in progress.** The project file, data model and measures are in place. Report pages are being built. The table at the bottom shows what is done.

## What the dashboard shows

| Page | Source table | What it answers |
|---|---|---|
| **Signal Leaderboard** | `gold_signals` | Which drug–reaction pairs show the strongest statistical signal, ranked by PRR, plus a separate panel for pairs that rest on very few reports |
| **Patient Demographics** | `gold_demographics` | Who the reports describe: age group, sex, top 10 reporter countries (`EU` and unspecified excluded) |
| **Case Outcomes** | `gold_outcomes` | What happened in the reported cases and who reported them |

Signals are binary: `SIGNAL` or `NO SIGNAL` (Evans criteria: n ≥ 3, PRR ≥ 2.0 and Yates chi-square ≥ 4.0). There is no "weak signal" tier.

**How to read it:** counts are *reports*, not patients. A signal is a statistical lead for review. It is not evidence that a drug caused the event.

## Folder contents

```
powerbi/
├── PharmaSignals.pbip              ← open this in Power BI Desktop
├── PharmaSignals.Report/           ← pages and visuals (text files)
├── PharmaSignals.SemanticModel/    ← tables, columns, measures (TMDL text files)
├── screenshots/                    ← page images for the main README (added when pages are done)
└── README.md
```

The report is saved as a **Power BI Project (`.pbip`)**, not a `.pbix`. Every part of it is a text file, so changes show up as readable diffs in Git.

## Data model

Three tables, **no relationships** (by design). Each table is already aggregated to the grain of its page, and the only shared column is the quarter label. A relationship would add ambiguity and no value. Revisit this when a second quarter is added.

| Table | Rows (2026Q1) | Columns |
|---|---|---|
| `gold_signals` | 373,012 | drugname, pt, case_count, drug_total, event_total, total_reports, prr, ror, chi_sq, haldane_applied, signal_flag, source_quarter |
| `gold_demographics` | 34 | dimension, dimension_value, case_count, source_quarter |
| `gold_outcomes` | 11 | dimension, code, label, case_count, source_quarter |

### Derived columns

```dax
is_exclusive = gold_signals[case_count] = gold_signals[event_total]
```

True when every report of the event names this drug. These pairs can rank very high on few reports, so they get their own panel instead of the main leaderboard.

```dax
is_country = NOT ( gold_demographics[dimension_value] IN { "EU", "COUNTRY NOT SPECIFIED" } )
```

On `gold_demographics`, used only by the Top 10 reporter countries chart (D11). `EU` is a regional code and `COUNTRY NOT SPECIFIED` is a missing-data bucket, so neither is a country. The column is TRUE for rows of the other dimensions, which is harmless because each visual also filters on `dimension`. Not yet built: see Build status.

### Measures (on `gold_signals`)

| Measure | Meaning | Expected on 2026Q1 |
|---|---|---|
| `Cases` | N, the signal denominator: cases with at least one primary-suspect drug. Returns text via `FORMAT`, so it is display-only (Issue 26). Card title: "Reports analysed" | 397,209 |
| `Total Pairs` | All drug–reaction pairs | 373,012 |
| `Signal Pairs` | Pairs flagged `SIGNAL` | 49,984 |
| `Leaderboard Pairs` | `SIGNAL` and not exclusive | 49,878 |
| `Exclusive Signal Pairs` | `SIGNAL` and exclusive | 106 |
| `Haldane Pairs` | Pairs where the Haldane correction was applied | 7,197 |
| `Haldane %` | Haldane Pairs ÷ Total Pairs | 1.93% |

Check: 49,984 − 106 = 49,878.

`Cases` is 397,209, not 397,224. `silver_cases` holds 397,224 cases, but 15 have no primary-suspect drug, so they never enter the signal math (validation V8). The 397,224 total is the sum of any one dimension in `gold_demographics` (for example the AGE_GROUP rows).

## Setup

1. Install [Power BI Desktop](https://powerbi.microsoft.com/desktop) (Windows, free).
2. Run the pipeline (`notebooks/99_run_pipeline.py`) so `04_export.py` writes the three CSVs to the Unity Catalog Volume (`/Volumes/workspace/default/pharma_raw/exports/`).
3. Download `gold_signals.csv`, `gold_demographics.csv` and `gold_outcomes.csv` from the Databricks Catalog UI. Each should be one file, not a folder.
4. Put them in a local `exports/` folder. The CSVs are git-ignored on purpose.
5. Open `PharmaSignals.pbip`.
6. **Set the `ExportsFolder` parameter.** All three tables read from one folder parameter, which defaults to `C:\Projects\Claude\pharma-signal-lake\exports`. In Power BI Desktop: Home → Transform data → Manage parameters → set `ExportsFolder` to the folder that holds your three CSVs (no trailing backslash).
7. Refresh, then compare the card values with the "Expected" column above. If a number differs, check the column types first.

No credentials are stored in this project. It reads local CSV files only, with no Databricks connection.

## Theme

Light background with a pastel palette: `#9ADBCC`, `#C7F2D1`, `#E3FFD6`, `#FFFBF0`, `#D0F2F7`, `#9EB2DB`. Header band fill is `#9EB2DB`. Pages carry no pipeline or stack details. Those belong in `ARCHITECTURE.md`.

## Design rules

- **Binary signal classification only** (FR-16).
- **Page names follow the BRD** (FR-20).
- **Only charts the Gold schema supports** (FR-18). Nothing speculative.
- **Exclusive panel shows `drugname`, `pt` and `case_count` only.** It has no PRR or ROR, because a Haldane-corrected PRR is not an interpretable magnitude for these pairs (FR-21).
- **Haldane-corrected rows are flagged** in the leaderboard tooltip (FR-21).
- **Top 10 countries excludes `EU` and `COUNTRY NOT SPECIFIED`** and says so in a footnote (D11).
- **Optional Drug Name slicer on the leaderboard only.** No drug filter on the other pages, and no cross-filtering between visuals on the Demographics and Outcomes pages (D12).

## Build status

| Item | Status |
|---|---|
| Power BI Project saved (`.pbip`) | Done |
| Three tables loaded with types | Done |
| Measures defined | Done |
| `is_exclusive` column | Done |
| `is_country` column on `gold_demographics` | Not yet added. The Top 10 countries chart needs it (D11) |
| Theme and page header | Done (header band, page navigator) |
| Signal Leaderboard page | In progress (shell and "Reports analysed" card only; leaderboard and exclusive panel not built) |
| Patient Demographics page | Not started |
| Case Outcomes page | Not started |
| Counts verified against "Expected" | Partly. "Reports analysed" = 397,209 confirmed in Power BI. The other values were confirmed against the CSV exports (2026-10-14) but not yet checked on their cards |
| Screenshots | Not started |

Update this table as each item is finished. Screenshots go in `screenshots/` only after the counts match.

## Known limitations

- **Single quarter (2026Q1).** The pipeline supports more quarters through config. The report has not been extended yet.
- **Demographics and Outcomes cannot be filtered by drug.** Those Gold tables have no drug dimension (D12).
- **Report counts, not patient counts.** One patient can appear in several reports, and one case can carry several outcome codes. Outcome bars can add up to more than the case count.
