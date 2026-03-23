
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

**v1.4 SDS upload:** **MarkItDown + regex** and **Hybrid** (MarkItDown → OCR if no CAS) only — see [docs/SDS_EXTRACTION_PIPELINES.md](docs/SDS_EXTRACTION_PIPELINES.md). Optional **local LLM** (Ollama) for other flows: [docs/OLLAMA_SETUP.md](docs/OLLAMA_SETUP.md).

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
   python -m streamlit run app.py
   ```
   Open the URL shown in the terminal (usually http://localhost:8501).

   **Tip:** Test the app locally before deploying to Streamlit Cloud. Each Cloud redeploy clones the repo and fetches Git LFS files, which consumes your LFS bandwidth quota. Running locally avoids LFS entirely.

5. **Optional — Local LLM (Qwen / Gemma) for SDS extraction**
   - Install [Ollama](https://ollama.com) on your machine.
   - In a terminal: `ollama pull qwen2:0.5b` and/or `ollama pull gemma2:2b`.
   - The app uses `OLLAMA_HOST` and `OLLAMA_MODEL` (see [docs/OLLAMA_SETUP.md](docs/OLLAMA_SETUP.md)). Nothing is pushed to GitHub except instructions; models stay local.

6. **Run SDS examples (batch)**
   - If you have a folder of SDS PDFs (e.g. `sds examples` next to the app), from the repo root run:
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

8. **SDS CAS extraction (two pipelines only)**
   - Full rationale and list of **removed** parsers: **[docs/SDS_EXTRACTION_PIPELINES.md](docs/SDS_EXTRACTION_PIPELINES.md)**.
   - Install: `pip install "markitdown[pdf]"` (see `requirements.txt`). **Default:** **Hybrid** (`hybrid_md_ocr`). Valid values: `hybrid_md_ocr` | `markitdown_fast`. Legacy env values (e.g. `default`, `ocr_tesseract`) are **remapped** to a supported pipeline.
   - Sidebar: **“SDS CAS extraction (v1.4 — two pipelines only)”** — **Hybrid** or **MarkItDown + regex**.
   - Caching: `cache/{sha256}/` (see `utils/cache_manager.py`). Env: `HAZQUERY_EXTRACTION_PIPELINE`, `HAZQUERY_DEFAULT_SDS_PIPELINE`, `HAZQUERY_EXTRACTION_CACHE`, `HAZQUERY_POPPLER_PATH`, `HAZQUERY_OCR_ENGINE`, `HAZQUERY_TESSERACT_PSM`.
   - Benchmark: `python tests/test_extraction_pipelines.py --folder "../sds examples" --limit 20` → `reports/extraction_benchmark.csv` and `extraction_benchmark_summary.md`.

9. **Windows terminal PATH & pip script warnings**
   - If pip warns that scripts are installed outside `PATH`, open the integrated terminal **from this workspace** so `.vscode/settings.json` applies: it appends common **user** Python `Scripts` folders and sets `HF_HUB_DISABLE_SYMLINKS_WARNING=1` and `TF_ENABLE_ONEDNN_OPTS=0` to reduce Hugging Face / oneDNN noise.
   - For **system-wide** fixes, add to your user `PATH`: `%APPDATA%\Python\Python313\Scripts` (adjust version) or use a venv and `pip` only from that environment.

10. **SDS parsing agreement / accuracy report (batch)**
   - Compares **pure Docling + DistilBERT CAS** and **Docling-only composition** against the **unified SDS parser** (reference proxy, not human labels):
     ```bash
     python scripts/sds_parsing_accuracy_report.py --folder "../sds examples" --out-dir artifacts
     ```
   - Optional: `--limit N` for a subset. Outputs `artifacts/sds_parsing_accuracy_report.md`, `.csv`, and `sds_parsing_accuracy_summary.json` (micro/macro F1, pooled TP/FP/FN).

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
│   ├── run_sds_examples.py   # Batch run SDS extraction on PDFs in sds examples folder
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
- **Repository:** [quick-hazard-assessment-app](https://github.com/glsalierno/quick-hazard-assessment-app)

---

## License

MIT (see [LICENSE](LICENSE)).
