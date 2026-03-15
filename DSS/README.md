# DSS — DSSTox local database

This folder holds the **EPA DSSTox CAS → DTXSID mapping** used by the Quick Hazard Assessment Streamlit app. No API key is required when using this local file.

## Source

- **Dataset:** [DSSTox Identifiers Mapped to CAS Numbers and Names](https://epa.figshare.com/articles/dataset/DSSTox_Identifiers_Mapped_to_CAS_Numbers_and_Names_File_11_14_2016/5588566) (EPA Figshare)
- **Alternative:** [CompTox Chemistry Dashboard Content File – DSSTox](https://figshare.com/articles/dataset/The_CompTox_Chemistry_Dashboard_Content_File_DSSTox2015_10_19/4836413) (Excel)
- **Format:** CSV (preferred) or Excel (`.xlsx`)

## File to use

Place the mapping file in this folder (`DSS/`) with one of these names:

- `cas_dtxsid_mapping.csv` (recommended)
- Any `.csv` or `.xlsx` in `DSS/` — the app will use the first one it finds with valid CAS/DTXSID columns.

Expected columns (names may vary):

- **CAS:** `CASRN` or `CAS`
- **DSSTox ID:** `DTXSID` (or `DSSTox_Substance_Id`)

## Git LFS (for publishing to GitHub)

The DSSTox file can be large. This repo is set up to track it with **Git LFS** so cloning stays fast and GitHub accepts the file.

1. **Install Git LFS** (one-time):  
   [https://git-lfs.com](https://git-lfs.com) — then run:
   ```bash
   git lfs install
   ```

2. **Tracking is already configured** in the repo root `.gitattributes`:
   - `DSS/*.csv` and `DSS/*.xlsx` are tracked with LFS.

3. **Add your file and commit:**
   ```bash
   cp /path/to/your/cas_dtxsid_mapping.csv DSS/
   git add DSS/cas_dtxsid_mapping.csv
   git commit -m "Add DSSTox CAS-DTXSID mapping (LFS)"
   git push
   ```

4. **Clone on another machine:**  
   Run `git lfs install` once, then `git clone` — LFS will pull the file automatically.

## Updating the mapping

1. Download the latest CSV/Excel from EPA (links above).
2. Replace the file in `DSS/` (same name or update the file you use).
3. Commit and push; LFS will store the new version.

## If the file is missing

The app runs in **PubChem-only mode**: hazard data still comes from PubChem, but DTXSID will not be shown. No DSSTox file or API key is required for basic use.
