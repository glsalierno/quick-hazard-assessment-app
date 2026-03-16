
# Quick Hazard Assessment — Streamlit App

Interactive web app for **chemical hazard assessment** from **PubChem** and **DSSTox local** (no API key required). Part of the [quick_hazard_assessment](https://github.com/glsalierno/quick_hazard_assessment) ecosystem.

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://quick-hazard-assessment-app.streamlit.app)

---

## Features

- **Input:** CAS number (e.g. `67-64-1`) or chemical name
- **DSSTox local:** CAS → DTXSID lookup from a local mapping file (no EPA API key)
- **PubChem:** Properties, GHS H/P codes with phrase legends, flash point, vapor pressure, IUPAC name, SMILES
- **Molecular structure:** 2D rendering at the top of the report (client-side [smiles-drawer](https://github.com/reymond-group/smiles-drawer))
- **Graceful fallback:** If the DSSTox file is missing, the app runs in **PubChem-only** mode
- **Download:** Report as CSV
- **Citation:** Zenodo DOI reminder for research use

*Enhanced predictions with OPERA QSAR are available in the [command-line version](https://github.com/glsalierno/quick_hazard_assessment); OPERA is not included in this Streamlit deployment.*

---

## Run locally

1. **Clone and enter the repo**
   ```bash
   git clone https://github.com/glsalierno/quick-hazard-assessment-app.git
   cd quick-hazard-assessment-app
   ```

2. **Create a virtual environment and install dependencies**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate   # Windows
   # source .venv/bin/activate  # Linux/macOS
   pip install -r requirements.txt
   ```

3. **DSSTox mapping (optional but recommended)**
   - Download the [EPA Figshare CAS–DTXSID mapping](https://epa.figshare.com/articles/dataset/DSSTox_Identifiers_Mapped_to_CAS_Numbers_and_Names_File_11_14_2016/5588566) (CSV).
   - Place it in the **`DSS/`** folder (e.g. `DSS/cas_dtxsid_mapping.csv`).
   - See **`DSS/README.md`** for column names, Excel support, and update instructions.
   - If the file is missing, the app runs in PubChem-only mode.

4. **Run the app**
   ```bash
   streamlit run app.py
   ```
   Open the URL shown in the terminal (usually http://localhost:8501).

---

## Deploy on Streamlit Community Cloud

1. Push this app to a GitHub repo (e.g. under `quick_hazard_assessment`, in a branch like `feature/streamlit-app` or in a subfolder).
2. Go to [share.streamlit.io](https://share.streamlit.io), sign in with GitHub, and deploy.
3. Set **Main file path** to `app.py` and **Root directory** to the folder that contains `app.py` (e.g. repo root or `GHhaz2`).
4. If you use the DSSTox file: the repo is **Git LFS–ready** (see below). Add the file to `DSS/`, commit, and push; LFS will store it. Or omit it and run in PubChem-only mode.

Update the badge URL in this README to your deployed app URL (e.g. `https://your-app-name.streamlit.app`).

---

## Publishing to GitHub (Git LFS)

The DSSTox mapping in **`DSS/`** can be large. The repo uses **Git LFS** so GitHub accepts it and clones stay fast.

1. **Install Git LFS** (one-time): [git-lfs.com](https://git-lfs.com) → then run:
   ```bash
   git lfs install
   ```
2. **Tracking is already set** in `.gitattributes`: `DSS/*.csv` and `DSS/*.xlsx` are tracked with LFS.
3. **Add your DSSTox file and push:**
   ```bash
   # Copy your mapping into DSS/, then:
   git add DSS/cas_dtxsid_mapping.csv
   git add .
   git commit -m "Add DSSTox mapping (LFS)"
   git push origin main
   ```
4. **New clones:** Run `git lfs install` once on each machine; `git clone` will then pull LFS files automatically.

See **`DSS/README.md`** for download links and update instructions.

---

## Project layout

```
├── app.py                 # Main Streamlit app
├── config.py              # App and path settings
├── requirements.txt
├── .gitattributes         # Git LFS tracking for DSS/*.csv, DSS/*.xlsx
├── DSS/                   # DSSTox local database (LFS-tracked)
│   ├── README.md          # Source, LFS instructions, update steps
│   └── cas_dtxsid_mapping.csv   # (user-downloaded; add to repo via LFS)
├── data/
│   └── dsstox/            # Legacy path; app uses DSS/ now
│       └── README.md
└── utils/
    ├── dsstox_local.py     # DSSTox loader from DSS/ (cached, CSV/Excel)
    ├── cas_validator.py    # CAS validation/normalization
    ├── pubchem_client.py   # PubChem API wrapper
    ├── ghs_formatter.py    # GHS H/P phrase formatting
    └── smiles_drawer.py    # 2D structure (smiles-drawer)
```

---

## Citation

If this tool contributes to your research, please cite:

- **Zenodo:** [DOI 10.5281/zenodo.14636704](https://doi.org/10.5281/zenodo.14636704)
- **Repository:** [quick-hazard-assessment-app](https://github.com/glsalierno/quick-hazard-assessment-app)

---

## License

MIT (see [LICENSE](LICENSE)).
