# DEVELOPER.md — PharmaSignalLake

Developer notes, known environment issues, and resolutions encountered during development.
This file is a living log — add new entries as issues are found and resolved.

---

## Table of Contents

1. [Environment Notes](#1-environment-notes)
2. [VS Code + Databricks Sync Setup](#2-vs-code--databricks-sync-setup)
3. [Known Issues and Resolutions](#3-known-issues-and-resolutions)
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

## 2. VS Code + Databricks Sync Setup

Edit notebooks locally in VS Code and sync them to Databricks without manual copy-paste.
This is the recommended development workflow for this project.

### Step 1 — Generate a Personal Access Token (PAT) in Databricks

A PAT is how the VS Code extension authenticates to your workspace.

1. Log in to your Databricks workspace
2. Click your profile icon (top right) → **Settings**
3. In the left sidebar, click **Developer**
4. Under **Access tokens**, click **Manage**
5. Click **Generate new token**
6. Set a description (e.g., `vscode-pharma`) and an expiry (90 days is reasonable)
7. Click **Generate**
8. **Copy the token immediately** — it is only shown once

Store the token somewhere safe (password manager). If you lose it, generate a new one.

---

### Step 2 — Install the Databricks Extension in VS Code

1. Open VS Code
2. Go to the **Extensions** panel (`Ctrl+Shift+X`)
3. Search for `Databricks`
4. Install the extension published by **Databricks** (official, not third-party)
5. Reload VS Code if prompted

---

### Step 3 — Connect the Extension to Your Workspace

1. Click the **Databricks icon** in the VS Code left sidebar (appears after install)
2. Click **Configure Databricks**
3. Enter your workspace URL — format: `https://dbc-xxxxxxxx.cloud.databricks.com`
   - Find this in your browser address bar when logged into Databricks
4. When prompted for authentication, choose **Personal Access Token**
5. Paste the PAT you generated in Step 1
6. Click **Connect** — the extension will verify the connection

If successful, you will see your workspace name appear in the Databricks panel.

---

### Step 4 — Sync Your Local Notebooks to Databricks

1. In the Databricks panel, locate your workspace folder:
   `/Users/your@email.com/PharmaSignalLake/notebooks`
2. Right-click the `notebooks/` folder in VS Code Explorer
3. Select **Upload to Databricks Workspace**
4. Map it to your Databricks workspace path:
   `/Users/your@email.com/PharmaSignalLake/notebooks`

After the initial upload, you can sync individual files by right-clicking them and selecting **Upload to Databricks Workspace**.

---

### Step 5 — Run Notebooks from VS Code (Optional)

The extension allows you to run notebooks and see output inside VS Code:

1. Open a `.py` notebook file in VS Code
2. Click the **Run** button in the Databricks panel, or right-click → **Run on Databricks**
3. Select your serverless compute
4. Output streams back to VS Code

---

### Recommended Workflow

```
Edit .py file in VS Code
        │
        ▼
Right-click → Upload to Databricks Workspace
        │
        ▼
Run notebook in Databricks (browser or VS Code)
        │
        ▼
Commit and push to GitHub from VS Code Source Control
```

---

### Troubleshooting

| Problem | Fix |
|---------|-----|
| Extension shows "Connection failed" | Verify workspace URL has no trailing slash; re-paste the PAT |
| PAT rejected | Token may have expired — generate a new one in Databricks Settings → Developer |
| Upload greyed out | Ensure the file has a `.py` extension; Databricks extension only syncs Python notebooks |
| Notebook runs but uses old code | Re-upload the file before running — the extension does not auto-sync on save |

---

## 3. Known Issues and Resolutions

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
