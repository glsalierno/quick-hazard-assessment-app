# Testing the public (anonymized) copy locally

This folder (`quick-hazard-assessment-app-public`) is intended for GitHub publication. It should not contain machine-specific absolute paths in source files.

## 1. Environment

From this directory:

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## 2. Point to your existing ECHA / IUCLID files

Use the same ZIP and format folder you use for the main project. Prefer **absolute** paths in environment variables (or `Resolve-Path` as in `local_data/README.md`).

```powershell
$env:OFFLINE_LOCAL_ARCHIVE = "D:\data\reach_study_results_dossiers_23-05-2023.zip"
$env:OFFLINE_DOSSIER_INFO_XLSX = "D:\data\reach_study_results-dossier_info_23-05-2023.xlsx"
$env:IUCLID_FORMAT_DIR = "D:\data\IUCLID6_6_format_9.0.0"
```

Optional: copy those files under `local_data/` and use `Resolve-Path` so commands stay short.

If `IUCLID_FORMAT_DIR` is unset, the app still starts; IUCLID codes may remain raw or show `(unmapped)` until the format bundle is configured.

## 3. Run Streamlit

```powershell
streamlit run app.py
```

## 4. What to verify vs. the private working copy

- Main hazard flow, SDS upload, and PubChem/DSSTox behavior match.
- **REACH / IUCLID** panel: two sections — rows with structured hazard data, and aggregated counts for rows missing structured fields. There is **no** “hide unreliable studies” filter in this build.
- Rebuild snippet cache from the panel or via `python scripts/rebuild_iuclid_cache_two_uuids.py --cas "<CAS>" --refresh` after changing archives.

## 5. Dead-code check (maintainers)

```powershell
pip install -r requirements-dev.txt
python -m vulture . --min-confidence 100 --exclude ".venv,venv,__pycache__,cache,models,.git"
```

Treat lower-confidence vulture output as advisory (Streamlit and CLI entry points often look “unused” to static analysis).
