# DEVELOPER.md — PharmaSignalLake

Developer notes, known environment issues, and resolutions encountered during development.
This file is a living log — add new entries as issues are found and resolved.

---

## Table of Contents

1. [Environment Notes](#1-environment-notes)
2. [Known Issues and Resolutions](#2-known-issues-and-resolutions)
   - [Issue 1 — DBFS Root Disabled on Free Edition (Unity Catalog)](#issue-1--dbfs-root-disabled-on-free-edition-unity-catalog)

---

## 1. Environment Notes

| Item | Detail |
|------|--------|
| Platform | Databricks Free Edition (AWS) |
| Governance | Unity Catalog enforced by default — cannot be disabled on Free Edition |
| Compute | Serverless (no persistent cluster) |
| Databricks Runtime | 15.x (Spark 3.5) |
| Raw file storage | Unity Catalog Managed Volume (see Issue 1) |
| Delta table storage | Unity Catalog Managed Volume |
| Power BI | Desktop (local CSV import — no live connector) |

---

## 2. Known Issues and Resolutions

---

### Issue 1 — DBFS Root Disabled on Free Edition (Unity Catalog)

**Status:** Resolved  
**Date encountered:** June 2026  
**Affected files:** `notebooks/00_config.py`, `ARCHITECTURE.md`

#### Symptom

Running any notebook that references `/FileStore/...` or `dbfs:/FileStore/...` paths raised:

```
AnalysisException: DBFS_DISABLED
SQLSTATE: 56038
DBFS root access is not allowed in Unity Catalog-enabled workspaces.
```

#### Root Cause

Databricks Free Edition provisions workspaces with **Unity Catalog enabled by default**. Unity Catalog enforces stricter data governance and explicitly **disables DBFS root access** — the `/FileStore/` path (also written as `dbfs:/FileStore/`) is part of the DBFS root and is inaccessible in this mode.

The original `00_config.py` was written assuming DBFS paths, which is the correct assumption for legacy Databricks workspaces and Community Edition (retired January 2026). It is the wrong assumption for any workspace provisioned after Unity Catalog became the default.

#### Resolution

Created a Unity Catalog **Managed Volume** to serve as the raw file storage layer:

| Setting | Value |
|---------|-------|
| Catalog | `workspace` |
| Schema | `default` |
| Volume name | `pharma_raw` |
| Full volume path | `/Volumes/workspace/default/pharma_raw` |

Uploaded all 7 FAERS `.txt` files for 2026Q1 to:
```
/Volumes/workspace/default/pharma_raw/2026Q1/
    DEMO26Q1.txt
    DRUG26Q1.txt
    INDI26Q1.txt
    OUTC26Q1.txt
    REAC26Q1.txt
    RPSR26Q1.txt
    THER26Q1.txt
```

Updated `00_config.py` — all five base paths now point to the Volume:

```python
# Unity Catalog Volume — DBFS root is disabled on Free Edition (Unity Catalog default)
BASE_RAW    = f"/Volumes/workspace/default/pharma_raw/{QUARTER}"
BASE_BRONZE = "/Volumes/workspace/default/pharma_raw/bronze"
BASE_SILVER = "/Volumes/workspace/default/pharma_raw/silver"
BASE_GOLD   = "/Volumes/workspace/default/pharma_raw/gold"
BASE_EXPORT = "/Volumes/workspace/default/pharma_raw/exports"
```

All downstream path variables (`RAW_FILES`, `BRONZE`, `SILVER`, `GOLD`, `EXPORTS`) derive from these base paths and required no changes.

#### How to Create the Volume (for a new workspace)

If you need to recreate this setup from scratch:

1. In Databricks, go to **Catalog** (left sidebar)
2. Expand **workspace** catalog → **default** schema
3. Click **Create** → **Volume**
4. Set **Volume name** to `pharma_raw`, type **Managed**
5. Click **Create**
6. Go to **Data** → **Add Data** → **Upload to Volume**
7. Set target path to `/Volumes/workspace/default/pharma_raw/2026Q1/`
8. Upload all 7 FAERS `.txt` files

#### Adding a Second Quarter

When adding a new quarter, upload files to a new subfolder within the same volume:

```
/Volumes/workspace/default/pharma_raw/2025Q4/
    DEMO25Q4.txt
    ...
```

Then update `QUARTER` in `00_config.py`. No other changes needed.

#### Why Not Use a Different Catalog or Schema?

The `workspace` catalog is the default catalog provisioned by Databricks Free Edition. Using it avoids any additional catalog setup. In a production environment with multiple environments (dev/staging/prod), you would create a dedicated catalog (e.g., `pharma_dev`) and isolate schemas per environment.

#### Reference

- [Databricks Unity Catalog Volumes documentation](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [DBFS deprecation in Unity Catalog workspaces](https://docs.databricks.com/en/dbfs/index.html)

---

*Add new issues below this line using the same format.*
