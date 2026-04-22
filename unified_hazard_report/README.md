# Unified hazard report

This folder is a **standalone add-on**: it does not change existing v1.4 modules. It builds a **long-form** table that merges:

1. **Legacy pipeline** — `ChemicalAssessmentService` (same as the Streamlit app): PubChem (GHS H/P codes, toxicity lines), DSSTox, ToxValDB (SQLite), CPDB when available.
2. **Offline IUCLID** — `ingest.offline_echa_loader.load_echa_from_offline()` for dossier UUIDs, optional GHS rows from `Document.i6d`, and heuristic **study endpoint** snippets parsed from each dossier `.i6z`.

## Prerequisites

- Run from the **`quick-hazard-assessment-app`** directory (repository root of this app).
- `PYTHONPATH` should include `.` (the app root), e.g. PowerShell:

  ```powershell
  cd "…\quick-hazard-assessment-app"
  $env:PYTHONPATH = "."
  ```

- **Offline REACH data** (same as `scripts/test_offline_10.py`):

  - `OFFLINE_LOCAL_ARCHIVE` — path to the REACH Study Results `.zip` / folder of `.i6z`.
  - Optional: `OFFLINE_DOSSIER_INFO_XLSX` or place `reach_study_results*dossier_info*.xlsx` next to the ZIP so CAS/EC/name resolve.

- **Legacy data**: PubChem is fetched over the network. Local **SQLite** DSSTox/ToxVal (`data/chemical_db.sqlite`) improves ToxVal coverage when built with `scripts/setup_chemical_db.py`.

## CLI

```powershell
python unified_hazard_report\main.py --cas-list "50-00-0,67-64-1" --output data\unified_report_sample.csv --format csv
```

Or a CAS file (one CAS per line, `#` comments allowed):

```powershell
python unified_hazard_report\main.py --cas-file my_cas.txt --output report.json --format json
```

Excel:

```powershell
python unified_hazard_report\main.py --cas-list "50-00-0" --output report.xlsx --format excel
```

(`pip install openpyxl` for Excel.)

## Output columns

Each row is **one** legacy hazard line **or** one IUCLID endpoint / classification row:

| Column | Meaning |
|--------|---------|
| `cas` | Normalized CAS |
| `source_type` | `legacy`, `iuclid`, or `warning` |
| `source_name` | e.g. `PubChem`, `ToxValDB`, `REACH_study_i6d`, `REACH_i6d_CL` |
| `hazard_code` | H/P code when present (legacy GHS or IUCLID CL) |
| `hazard_statement` | Free text (ToxVal summary, CL text, etc.) |
| `endpoint_name` | IUCLID XML tag / label, or `GHS` for PubChem codes |
| `endpoint_value` | Numeric or text result when applicable |
| `units` | Units when known |
| `uuid` | IUCLID dossier UUID when row comes from REACH |

## Smoke test

```powershell
python unified_hazard_report\test_report.py
```

Prints row counts and which legacy / IUCLID sources contributed for `50-00-0` and `67-64-1`.

## Implementation notes

- There is **no** `get_document(uuid)` API in this repo; dossiers are read as **ZIP `.i6z`** files on disk. `iuclid_extractor.py` opens `Document.i6d` and applies lightweight XML / regex heuristics (not a full IUCLID schema mapping).
- For programmatic use: `from unified_hazard_report import unified_lookup, generate_report` after setting `PYTHONPATH` to the app root.
- Importing `ChemicalAssessmentService` pulls **Streamlit** transitively (`utils/chemical_db`). You may see harmless warnings like *No runtime found* or *missing ScriptRunContext* when running CLI scripts outside the Streamlit app.
