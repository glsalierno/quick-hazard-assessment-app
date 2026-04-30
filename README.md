
# Quick Hazard Assessment — Streamlit App

Interactive web app for **chemical hazard assessment** from **PubChem** and **DSSTox** local data (no API key required for core lookups). Optional modules (offline REACH dossiers, local LLMs) are configured via environment variables.

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://quick-hazard-assessment-app.streamlit.app)

---

## Quick start (first run)

1. **Python 3.10+** recommended. Create a virtual environment and install dependencies:
   ```bash
   cd quick-hazard-assessment-app-public
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. **Optional — offline REACH / IUCLID:** download the REACH dossier ZIP and IUCLID format bundle from [IUCLID 6 downloads (ECHA)](https://iuclid6.echa.europa.eu/downloads), then set `OFFLINE_LOCAL_ARCHIVE` and `IUCLID_FORMAT_DIR` (see [Offline REACH / IUCLID](#offline-reach--iuclid-optional) below). If `IUCLID_FORMAT_DIR` is omitted, the app still runs; picklist codes may appear as raw values or with an `(unmapped)` label until you add the format folder.
3. **Optional — DSSTox / ToxValDB SQLite:** place the CAS–DTXSID CSV in `DSS/` and run `python scripts/setup_chemical_db.py` for faster lookups (see [Chemical database](#chemical-database-dsstox-toxvaldb)).
4. **Launch:**
   ```bash
   streamlit run app.py
   ```

### Testing this public build with your existing ECHA files

Keep large archives **outside** the repo if you prefer; point environment variables at them using **absolute** paths (recommended on Windows) or resolve a relative path in your shell first:

```powershell
cd path\to\quick-hazard-assessment-app-public
$env:OFFLINE_LOCAL_ARCHIVE = "D:\data\reach_study_results_dossiers_23-05-2023.zip"
$env:OFFLINE_DOSSIER_INFO_XLSX = "D:\data\reach_study_results-dossier_info_23-05-2023.xlsx"
$env:IUCLID_FORMAT_DIR = "D:\data\IUCLID6_6_format_9.0.0"
streamlit run app.py
```

Alternatively, copy those files under a folder such as `local_data/` inside the repo and set the variables to the resolved full paths. This build matches the main app’s behavior except the REACH panel does **not** offer a reliability filter (see endpoint tables there).

More detail: **[docs/PUBLIC_RELEASE_TESTING.md](docs/PUBLIC_RELEASE_TESTING.md)**.

---

## Features

- **Input:** CAS number (e.g. `67-64-1`) or chemical name
- **DSSTox local:** CAS → DTXSID lookup from a local mapping file (no EPA API key)
- **PubChem:** Properties, GHS H/P codes with phrase legends, flash point, vapor pressure, IUPAC name, SMILES
- **Molecular structure:** 2D rendering at the top of the report (client-side [smiles-drawer](https://github.com/reymond-group/smiles-drawer))
- **Graceful fallback:** If the DSSTox file is missing, the app runs in **PubChem-only** mode
- **Download:** Report as CSV
- **Citation:** Zenodo DOI reminder for research use

*Enhanced predictions with OPERA QSAR may be available in a separate command-line workflow; OPERA is not bundled with this Streamlit deployment.*

**SDS upload (v1.5):** **Hybrid**, **MarkItDown + regex**, and optional **parse-then-extract** (`markdown_gliner_regex`: regex + local **GLiNER2** on Markdown) — see [docs/SDS_EXTRACTION_PIPELINES.md](docs/SDS_EXTRACTION_PIPELINES.md) and `requirements-gliner2.txt`. Optional **local LLM** (Ollama) for other flows: [docs/OLLAMA_SETUP.md](docs/OLLAMA_SETUP.md).

---

## Run locally

1. **Clone and enter the repo**
   ```bash
   git clone <YOUR_REPOSITORY_URL>
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

   **Tip:** Test the app locally before deploying to Streamlit Cloud. Each Cloud redeploy clones the repo and fetches Git LFS files, which consumes your LFS bandwidth quota. Running locally avoids LFS entirely.

5. **Optional — Local LLM (Qwen / Gemma) for SDS extraction**
   - Install [Ollama](https://ollama.com) on your machine.
   - In a terminal: `ollama pull qwen2:0.5b` and/or `ollama pull gemma2:2b`.
   - The app uses `OLLAMA_HOST` and `OLLAMA_MODEL` (see [docs/OLLAMA_SETUP.md](docs/OLLAMA_SETUP.md)). Nothing is pushed to GitHub except instructions; models stay local.

6. **Run SDS examples (batch)**
   - If you have a folder of SDS PDFs (e.g. `sds_examples/` in the repo root), from the repo root run:
     ```bash
     python scripts/run_sds_examples.py [--limit N] [--compare]
     ```
   - `--limit N` processes at most N PDFs; `--compare` runs SDS vs PubChem for each extracted CAS.
   - Override the folder: `SDS_EXAMPLES_DIR` or `python scripts/run_sds_examples.py --dir "path/to/sds examples"`.
   - Test readers (OCR + extraction): `python scripts/test_sds_readers.py [--limit N]` to print text length, CAS, GHS, and quantitative fields per PDF.

7. **OCR for scanned SDS PDFs**
   - If embedded text is short (< 250 chars), the app runs **Tesseract** OCR automatically (via `pdf2image` + `pytesseract`). **EasyOCR** is used as a fallback for pages where Tesseract returns little text.
   - Install **Tesseract** and **Poppler** on your system and ensure both are on `PATH`, then `pip install pdf2image pytesseract easyocr`. See [docs/OCR_SETUP.md](docs/OCR_SETUP.md). Without Poppler, `pdf2image` cannot rasterize PDF pages (`Unable to get page count`).
   - Optional: **ocrmypdf** to produce searchable PDFs: `pip install ocrmypdf`, then `python scripts/make_searchable_pdf.py input.pdf [output.pdf]`.

8. **SDS CAS extraction (MarkItDown pipelines)**
   - Full rationale and list of **removed** parsers: **[docs/SDS_EXTRACTION_PIPELINES.md](docs/SDS_EXTRACTION_PIPELINES.md)**.
   - Install: `pip install "markitdown[pdf]"` (see `requirements.txt`). **Default:** **Hybrid** (`hybrid_md_ocr`). Valid values: `hybrid_md_ocr` | `markitdown_fast` | `markdown_gliner_regex`. Legacy env values (e.g. `default`, `ocr_tesseract`) are **remapped** to a supported pipeline.
   - Optional GLiNER2: `pip install -r requirements-gliner2.txt`. Env: `HAZQUERY_USE_GLINER2`, `HAZQUERY_GLINER2_MODEL`, `HAZQUERY_GLINER2_MAX_CHARS`.
   - Sidebar: **“SDS CAS extraction (v1.5 — MarkItDown pipelines)”** — pick a strategy; use **SDS extraction diagnostics** after upload to monitor regex vs GLiNER2.
   - Caching: `cache/{sha256}/` (see `utils/cache_manager.py`). Env: `HAZQUERY_EXTRACTION_PIPELINE`, `HAZQUERY_DEFAULT_SDS_PIPELINE`, `HAZQUERY_EXTRACTION_CACHE`, `HAZQUERY_POPPLER_PATH`, `HAZQUERY_OCR_ENGINE`, `HAZQUERY_TESSERACT_PSM`.
   - Benchmark: `python tests/test_extraction_pipelines.py --folder "sds_examples" --limit 20` → `reports/extraction_benchmark.csv` and `extraction_benchmark_summary.md`.

9. **Windows terminal PATH & pip script warnings**
   - If pip warns that scripts are installed outside `PATH`, open the integrated terminal **from this workspace** so `.vscode/settings.json` applies: it appends common **user** Python `Scripts` folders and sets `HF_HUB_DISABLE_SYMLINKS_WARNING=1` and `TF_ENABLE_ONEDNN_OPTS=0` to reduce Hugging Face / oneDNN noise.
   - For **system-wide** fixes, add to your user `PATH`: `%APPDATA%\Python\Python313\Scripts` (adjust version) or use a venv and `pip` only from that environment.

10. **SDS parsing agreement / accuracy report (batch)**
   - Compares **pure Docling + DistilBERT CAS** and **Docling-only composition** against the **unified SDS parser** (reference proxy, not human labels):
     ```bash
     python scripts/sds_parsing_accuracy_report.py --folder "sds_examples" --out-dir artifacts
     ```
   - Optional: `--limit N` for a subset. Outputs `artifacts/sds_parsing_accuracy_report.md`, `.csv`, and `sds_parsing_accuracy_summary.json` (micro/macro F1, pooled TP/FP/FN).

11. **IUCLID / offline REACH (optional)** — see [Offline REACH / IUCLID](#offline-reach--iuclid-optional) below.

---

## Offline REACH / IUCLID (optional)

The Streamlit app can read **offline REACH study-result dossiers** (`.i6z` inside a `.zip`) and decode IUCLID picklist codes when you install the **IUCLID format** phrase package.

### Obtaining the REACH study results archive

1. Open the official IUCLID download area: **[IUCLID 6 downloads (ECHA)](https://iuclid6.echa.europa.eu/downloads)**.
2. Download a **REACH study results dossiers** archive (file name like `reach_study_results_dossiers_*.zip`). This ZIP contains many `.i6z` dossier files.
3. On your machine, set the environment variable (or add to `.streamlit/secrets.toml` — see `.streamlit/secrets.example.toml`):

   | Variable | Meaning |
   |----------|---------|
   | `OFFLINE_LOCAL_ARCHIVE` | Full path to the `reach_study_results_dossiers_*.zip` file **or** to a folder that already contains `.i6z` files. |

The app extracts or scans that location and builds caches under `OFFLINE_CACHE_DIR` (default: `data/offline_cache/`).

### Obtaining the IUCLID format package (phrase mapping)

1. On the same **[IUCLID 6 downloads](https://iuclid6.echa.europa.eu/downloads)** page, download the **IUCLID 6 format** bundle (e.g. a ZIP named like `IUCLID6_6_format_9.0.0.zip`).
2. Extract it to a folder on disk.
3. Set:

   | Variable | Meaning |
   |----------|---------|
   | `IUCLID_FORMAT_DIR` | Path to the **extracted** format folder (the directory that contains `dcr.xml`, `*.properties`, etc.). |

If `IUCLID_FORMAT_DIR` is **not** set, the app still runs: numeric codes may show as raw values or with `(unmapped)` in the UI until you configure the format directory.

### Phrase decoder test and snippet cache rebuild

```bash
python scripts/test_iuclid_decoder.py
python scripts/rebuild_iuclid_cache_two_uuids.py --cas "71-43-2" --refresh
```

Replace the CAS as needed. Use `--refresh` to force re-parsing cached dossiers.

---

## Chemical database (DSSTox, ToxValDB)

For **faster** DSSTox and ToxValDB access, build the local SQLite database:

1. Add a CAS → DTXSID mapping file under `DSS/` (see `DSS/README.md` and [EPA Figshare DSSTox mapping](https://epa.figshare.com/articles/dataset/DSSTox_Identifiers_Mapped_to_CAS_Numbers_and_Names_File_11_14_2016/5588566)).
2. Optionally add COMPTOX ToxValDB Excel exports under `COMPTOX_Public (Data Excel Files Folder)/Data Excel Files/`.
3. Run:

   ```bash
   python scripts/setup_chemical_db.py
   ```

   This writes `data/chemical_db.sqlite`. The app **falls back** to CSV/XLSX in `DSS/` if SQLite is missing, but SQLite is **recommended** for speed.

---

## Environment variables (summary)

| Variable | Required | Description |
|----------|----------|-------------|
| `OFFLINE_LOCAL_ARCHIVE` | No | Path to REACH `reach_study_results_dossiers_*.zip` or folder of `.i6z` files. |
| `OFFLINE_DOSSIER_INFO_XLSX` | No | Optional Excel index for dossier metadata (CAS ↔ UUID). |
| `OFFLINE_CACHE_DIR` | No | Where offline snapshots and `offline_snippets_cache.db` live (default `data/offline_cache`). |
| `IUCLID_FORMAT_DIR` | No | Extracted IUCLID format bundle for picklist / phrase decoding. |
| `CHEMICAL_DB_PATH` | No | Override path to SQLite chemical DB (default `data/chemical_db.sqlite`). |
| `P2OASYS_MATRIX_PATH` | No | P2OASys hazard matrix Excel (default under `data/`). |
| `QSAR_TOOLBOX_PORT` | No | Local OECD QSAR Toolbox WebSuite port (Windows; optional). |
| `USE_PUBCHEM_CAS_VALIDATION` | No | `1` / `0` — validate extracted CAS against PubChem (default on). |
| `SHOW_ONLY_PUBCHEM_VERIFIED` | No | `1` hides SDS CAS not found in PubChem. |
| `MIN_CAS_CONFIDENCE` | No | Minimum confidence (0–1) to show SDS extractions in UI. |
| `HAZQUERY_DISABLE_DOCLING` | No | `1` to skip Docling on constrained hosts. |
| `OLLAMA_HOST`, `OLLAMA_MODEL` | No | Local LLM for optional SDS flows (see `docs/OLLAMA_SETUP.md`). |

Contributors can install dev tools (e.g. **vulture**) with `pip install -r requirements-dev.txt`.

---

## Deploy on Streamlit Community Cloud

1. Push this app to a GitHub repo (e.g. under `quick_hazard_assessment`, in a branch like `feature/streamlit-app` or in a subfolder).
2. Go to [share.streamlit.io](https://share.streamlit.io), sign in with GitHub, and deploy.
3. Set **Main file path** to `app.py` and **Root directory** to the folder that contains `app.py` (usually the repository root).
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

## Local SQLite database (optional, faster)

For **faster lookups**, you can build a single SQLite database that combines DSSTox identifiers and ToxValDB toxicity data.

1. **One-time setup**
   - Ensure **DSS** has a CAS–DTXSID CSV (e.g. `DSS/cas_dtxsid_mapping.csv`).
   - Optionally place the **COMPTOX ToxValDB Excel** files in  
     `COMPTOX_Public (Data Excel Files Folder)/Data Excel Files/` (each `.xlsx` will be read).
2. **Build the database**
   ```bash
   python scripts/setup_chemical_db.py
   ```
   This creates **`data/chemical_db.sqlite`** (DSSTox table and, if Excel files are present, ToxValDB table).
3. **Run the app**  
   If `data/chemical_db.sqlite` exists, the app uses it for DSSTox (and ToxValDB when the table is present) and falls back to CSV/API otherwise.

**Performance:** DSSTox lookups drop from seconds (CSV) to milliseconds (SQLite). ToxValDB queries are also served from SQLite when the table is built.

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
├── COMPTOX_Public (Data Excel Files Folder)/   # ToxValDB Excel files (optional; LFS)
│   └── Data Excel Files/*.xlsx
├── COMPTOX_Public (Data MySQL Dump File Folder)/   # MySQL dump (optional)
├── data/                  # Built SQLite DB (after setup_chemical_db.py)
│   └── chemical_db.sqlite
├── docs/
│   └── OLLAMA_SETUP.md    # How to install Ollama + Qwen/Gemma locally (models stay on your machine)
├── scripts/
│   ├── setup_chemical_db.py   # Build data/chemical_db.sqlite from DSS + COMPTOX
│   ├── run_sds_examples.py   # Batch run SDS extraction on PDFs in sds_examples/ (optional)
│   ├── make_searchable_pdf.py # Add text layer to a PDF (ocrmypdf + Tesseract)
│   └── test_sds_readers.py   # Test SDS extraction + OCR on example PDFs
└── utils/
    ├── chemical_db.py     # SQLite DSSTox + ToxValDB (fast lookups)
    ├── dsstox_local.py    # DSSTox loader from DSS/ (CSV/Excel fallback)
    ├── cas_validator.py   # CAS validation/normalization
    ├── pubchem_client.py  # PubChem API wrapper
    ├── ghs_formatter.py   # GHS H/P phrase formatting
    ├── smiles_drawer.py   # 2D structure (smiles-drawer)
    ├── sds_pdf_utils.py   # PDF text extraction for SDS uploads
    ├── sds_regex_extractor.py  # SDS field extraction (regex, Phase 1)
    └── sds_compare.py     # SDS vs PubChem comparison report
```

---

## Citation

If this tool contributes to your research, please cite:

- **Zenodo:** [DOI 10.5281/zenodo.19056294](https://doi.org/10.5281/zenodo.19056294)
- **Repository:** publish your own fork or tarball; do not rely on private paths in configuration.

---

## License

MIT (see [LICENSE](LICENSE)).
