# Manual download sources for ECOTOX, ToxRefDB, CPDB

If `python scripts/download_databases.py` fails (e.g. 403 Forbidden), download the files below and place them as indicated. Then run `python scripts/setup_chemical_db.py` to load them into SQLite.

---

## CPDB (Carcinogenic Potency Database)

**Working URL (single Excel, summary by chemical):**

- **https://cpdb.thomas-slone.org/xls/CPDBChemical.xls**

**Where to put it:**

- `data/raw_databases/CPDBChemical.xls`  
  **or**  
- `data/raw_databases/cpdb/CPDBChemical.xls`

**Alternate (zip; may block script downloads):**

- https://files.toxplanet.com/cpdb/cpdb_excel.zip  
  → Extract and put any `.xls`/`.xlsx` inside `data/raw_databases/cpdb/`

**Reference:** [CPDB Summary Table](https://cpdb.thomas-slone.org/chemicalsummary.html)

---

## ToxRefDB (EPA Toxicity Reference Database)

**Primary (EPA; often 403 when requested by script):**

- **https://www.epa.gov/sites/default/files/2016-10/toxrefdb_v2_0.xlsx**

**Where to put it:**

- `data/raw_databases/toxrefdb_v2_0.xlsx`

**Alternative – EPA Figshare (bundle with guide):**

- **https://epa.figshare.com/articles/dataset/Animal_Toxicity_Studies_Effects_and_Endpoints_Toxicity_Reference_Database_-_ToxRefDB_files_/6062545**  
  → Download the dataset; if it contains an Excel or CSV of ToxRefDB, place it in `data/raw_databases/` and name or copy it as `toxrefdb_v2_0.xlsx` if needed so the setup script finds it (script looks for `toxrefdb_v2_0.xlsx`).

**GitHub (documentation/scripts only; no single .xlsx in repo):**

- https://github.com/USEPA/CompTox-ToxRefDB  
  → `past_versions/v2_0` has user guide and pipeline, not the full Excel. Use EPA or Figshare link above for data.

**Reference:** [CompTox ToxRefDB list](https://comptox.epa.gov/dashboard/chemical-lists/TOXREFDB2)

---

## ECOTOX (EPA ecotoxicity)

**No direct bulk-download URL.** You must use the EPA site and (if required) create an account:

- **https://cfpub.epa.gov/ecotox/**  
  → Use “Search” or “Download” to export results (e.g. by chemical or endpoint).  
  → Save as Excel or CSV.

**Where to put it:**

- `data/raw_databases/ecotox/`  
  → Any `.xlsx` or `.csv` you export (e.g. `ecotox_results.xlsx` or `ECOTOX_export.csv`).  
  → The setup script will use the first Excel/CSV it finds in that folder.

**Reference:** [ECOTOX help](https://cfpub.epa.gov/ecotox/help.cfm)

---

## Quick checklist

| Database  | Save as / place |
|-----------|------------------|
| **CPDB**  | `data/raw_databases/CPDBChemical.xls` or `data/raw_databases/cpdb/CPDBChemical.xls` |
| **ToxRefDB** | `data/raw_databases/toxrefdb_v2_0.xlsx` |
| **ECOTOX** | Any `.xlsx` or `.csv` in `data/raw_databases/ecotox/` |

Then run:

```bash
python scripts/setup_chemical_db.py
```
