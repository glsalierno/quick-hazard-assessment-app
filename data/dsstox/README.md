# DSSTox Local Mapping Data

This folder holds the **EPA DSSTox CAS → DTXSID mapping file** used for identifier lookup in the Quick Hazard Assessment Streamlit app. No API key is required when using this local file.

## Source

- **Dataset:** [DSSTox Identifiers Mapped to CAS Numbers and Names](https://epa.figshare.com/articles/dataset/DSSTox_Identifiers_Mapped_to_CAS_Numbers_and_Names_File_11_14_2016/5588566)
- **Provider:** U.S. EPA (EPA Figshare)
- **Recommended format:** CSV

## File to Use

Place the mapping file in this directory with one of these names:

- `cas_dtxsid_mapping.csv` (recommended)

Expected columns (column names may vary; the app supports common variants):

- **CAS identifier:** `CASRN` or `CAS`
- **DSSTox ID:** `DTXSID`

## How to Update

1. Download the latest mapping from the EPA Figshare link above (or the current version from [EPA CompTox Chemicals Dashboard](https://comptox.epa.gov/dashboard)).
2. Save as CSV in this folder as `cas_dtxsid_mapping.csv`.
3. If the CSV uses different column names, ensure they include CAS and DTXSID equivalents; the loader in `utils/dsstox_local.py` can be adjusted to match (e.g. `CASRN`/`CAS`, `DTXSID`).

## Date of Download

Record the date you downloaded the file (e.g. in this README or in the file name) so you know how current the mapping is.

## If the File Is Missing

The app runs in **PubChem-only mode** if no mapping file is found: hazard data and properties still come from PubChem, but DTXSID will not be shown. No API key or DSSTox file is required for basic use.

## File Size and Git LFS

If the CSV is large (> a few MB), consider using [Git LFS](https://git-lfs.github.com/) so the repository stays light. Add in the repo root:

```gitattributes
data/dsstox/*.csv filter=lfs diff=lfs merge=lfs -text
```

Then run `git lfs track "data/dsstox/*.csv"` and commit as usual.
