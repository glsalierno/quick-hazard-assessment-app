<p align="center">
  <a href="https://quick-hazard-assessment-app.streamlit.app" title="Open the Streamlit app">
    <img src="readme-banner.svg" width="920" alt="Quick Hazard Assessment — chemical hazard reports from PubChem and DSSTox" />
  </a>
</p>

# Quick Hazard Assessment — Streamlit App

Interactive web app for **chemical hazard assessment** from **PubChem** and **DSSTox** local data (no API key required for core lookups). Optional modules (offline REACH dossiers, local LLMs) are configured via environment variables.

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://quick-hazard-assessment-app.streamlit.app)
[![Streamlit App v1.3](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://YOUR_V1.3_APP_URL.streamlit.app)
[![Streamlit App v2.0](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://YOUR_V2.0_APP_URL.streamlit.app)

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

**v1.4 SDS upload:** **MarkItDown + regex** and **Hybrid** (MarkItDown → OCR if no CAS) only — see [docs/SDS_EXTRACTION_PIPELINES.md](docs/SDS_EXTRACTION_PIPELINES.md). Optional **local LLM** (Ollama) for other flows: [docs/OLLAMA_SETUP.md](docs/OLLAMA_SETUP.md).

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

8. **SDS CAS extraction (two pipelines only)**
   - Full rationale and list of **removed** parsers: **[docs/SDS_EXTRACTION_PIPELINES.md](docs/SDS_EXTRACTION_PIPELINES.md)**.
   - Install: `pip install "markitdown[pdf]"` (see `requirements.txt`). **Default:** **Hybrid** (`hybrid_md_ocr`). Valid values: `hybrid_md_ocr` | `markitdown_fast`. Legacy env values (e.g. `default`, `ocr_tesseract`) are **remapped** to a supported pipeline.
   - Sidebar: **“SDS CAS extraction (v1.4 — two pipelines only)”** — **Hybrid** or **MarkItDown + regex**.
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

12. **P2OASys validation vs `fastP2OASys` reference CSVs (`--source fast`)**
    - Compares **category-level** expert scores (columns = chemical **names**, not CAS) to scores from this app’s pipeline: `ChemicalAssessmentService` → `build_hazard_data` → `compute_p2oasys_scores`, with the same optional **IARC / ODP–GWP / IPCC** and **IUCLID** merges as the P2OASys tab (no QSAR Toolbox in the script).
    - Default reference folder: sibling `../fastP2OASys/` under `hazquery` (override with `--reference-dir`).
    - Run from the repo root:
      ```bash
      python scripts/validate_p2oasys_vs_fast_reference.py --source fast --limit 50 -o data/p2oasys_validation.csv
      ```
    - **Output CSV columns:** `reference_name`, `resolved_cas`, `category`, `computed_score`, `reference_score`, `absolute_error`, `diff`, `pipeline_note` (e.g. `lookups+IUCLID` vs `PubChem-only`), `matrix_kind` (`official` vs `placeholder`), `error`.
    - **Interpretation:** Use the **official TURI matrix** (`P2OASYS_MATRIX_PATH` or `data/Hazard Matrix Group Review 9-19-23.xlsx`) for meaningful error metrics; the dev placeholder is layout-only. Rows with empty `computed_score` mean the matrix produced no category max for that bucket (often missing PubChem endpoints). Use `--iuclid-audit-dir path/to/dir` to dump `*_iuclid_normalized.csv` per CAS when dossiers exist (tuning IUCLID heuristics).
    - See also [docs/P2OASYS_LOOKUP_SOURCES.md](docs/P2OASYS_LOOKUP_SOURCES.md) (section 7).

13. **P2OASys CAS validation (offline expert, default for `run_full_validation`)**
    - **`--source expert`** (recommended for CAS lists): deterministic, no browser. For each CAS, loads **`../sds examples/scripts/lookup_p2oasys_by_cas.py`** (`get_best_match`) to (1) find rows in **`allP2OASys_*`** `P2OASys_Database_Results*.csv`, (2) match product **Name** to a column in **`fastP2OASys/P2OASys_Category_Scores_Data_*.csv`**, (3) read expert **top-level category** scores and compare to the app’s `_category_max` per category.
    - Environment variables (optional): **`P2OASYS_ARCHIVE_DIR`**, **`FAST_P2OASYS_DIR`**, **`P2OASYS_LOOKUP_SCRIPT`** (override path to `lookup_p2oasys_by_cas.py` if not under `GHhaz4/sds examples/scripts/`).
    - **Orchestrator** (`scripts/run_full_validation.py`, **`--source expert` by default**):
      ```bash
      python scripts/run_full_validation.py --cas-file cas_list.txt \\
        --archive-dir path/to/allP2OASys_120825 --fastp2oasys-dir path/to/fastP2OASys -o validation_report.csv
      ```
      **Check retrieval only** (no PubChem / matrix scoring): `python scripts/run_full_validation.py --check --cas-file cas_list.txt --archive-dir ... --fastp2oasys-dir ...` (optional `--strict`, `--json`). Same flags exist on `validate_p2oasys_vs_fast_reference.py` as `--check-retrieval` / `--dry-run`.
      Optional: `--summary summary.txt` (full validation only). For Playwright-based reference instead, use **`--source scraped`** and see below.
    - **Validator directly:**
      ```bash
      python scripts/validate_p2oasys_vs_fast_reference.py --source expert --cas 67-63-0 \\
        --archive-dir path/to/allP2OASys_120825 --fastp2oasys-dir path/to/fastP2OASys \\
        -o data/p2oasys_expert_validation_comparison.csv
      ```
    - **Expert mode output columns:** `CAS`, `category`, `computed_score`, `reference_score`, `absolute_error`, `source_of_lowest_value` (app pipeline + which expert column / matched name / archive evaluation), `matrix_kind`, `error`. CAS rows with no archive match or no expert column are skipped with a warning.

    **Optional: `--source scraped`** (compare-raw-data Playwright CSV)
    - The sibling **`../sds examples/scripts/fetch_p2oasys_category_scores.py`** builds a wide CSV: first column **`CAS`**, remaining columns = endpoint labels from the P2OASys web table. Respect [p2oasys.turi.org](https://p2oasys.turi.org) terms of use; install Playwright (`pip install playwright`, `python -m playwright install chromium`). Set **`P2OASYS_SCRAPER_SCRIPT`** if the script path differs.
    - Example:
      ```bash
      python scripts/run_full_validation.py --source scraped --cas-file cas_list.txt --auto-fetch -o validation_report.csv
      ```
      Or validate only:
      ```bash
      python scripts/validate_p2oasys_vs_fast_reference.py --source scraped --cas-file cas_list.txt \\
        --reference-csv data/p2oasys_category_scores.csv -o data/scraped_compare.csv
      ```
    - Edit **`SCRAPED_COLUMN_TO_COMPUTED_KEY`** in `scripts/validate_p2oasys_vs_fast_reference.py` so scraped headers map to matrix unit names or **`category:TopLevelCategoryName`**.

---

## Docker (Windows containers + OPERA + optional IUCLID)

This project includes a **Windows-container** `docker-compose.yml` so **OPERA** (`OPERA.exe`) mounts from `./opera` with defaults in `Dockerfile.windows`. **Offline IUCLID / REACH** is not in the image: you supply dossiers and the optional IUCLID format bundle under **`./data`** and set **`OFFLINE_LOCAL_ARCHIVE`** / **`IUCLID_FORMAT_DIR`** in `.env` to **`C:/app/data/...`** paths (same pattern as `CHEMICAL_DB_PATH`). See **Offline REACH / IUCLID** below and `.env.example`.

### Prerequisites

- Docker Desktop on Windows
- **Windows containers mode** enabled in Docker Desktop (required for `OPERA.exe`)
- Optional: local OPERA install from [NIEHS/OPERA releases](https://github.com/NIEHS/OPERA/releases)

### 1) Configure OPERA (automated helper)

From the repo root:

```bash
python scripts/setup_opera.py
```

What it does:
- checks common OPERA locations and existing `HAZQUERY_OPERA_EXE`
- queries latest release from GitHub API
- downloads host-appropriate artifact when possible
- updates `.env` with `HAZQUERY_OPERA_EXE`

Non-interactive mode:

```bash
python scripts/setup_opera.py --yes
```

If automation cannot perform a silent install (some `.exe`/`.msi` packages), install OPERA manually from [releases](https://github.com/NIEHS/OPERA/releases), then set:

```env
HAZQUERY_OPERA_EXE=C:\path\to\OPERA\application\OPERA.exe
```

### 2) Configure environment for Docker

Copy:

```bash
copy .env.example .env
```

Edit `.env` values as needed for your local paths.

### 3) Start the app with Docker Compose

```bash
docker compose up --build
```

Open [http://localhost:8501](http://localhost:8501).

### Data volume strategy (40+ GB friendly)

`docker-compose.yml` mounts local folders into the container:
- `./data -> C:/app/data`
- `./opera -> C:/app/opera`
- `./DSS -> C:/app/DSS`

This keeps large databases and OPERA assets on your host disk instead of image layers.

**IUCLID offline:** put the REACH dossier archive (`.zip` / `.7z`) or a folder of `.i6z` files and, if you use phrase decoding, the extracted **IUCLID 6 format** tree under `./data` on the host. In `.env` use container paths, for example `OFFLINE_LOCAL_ARCHIVE=C:/app/data/reach_study_results_dossiers_23-05-2023.zip` and `IUCLID_FORMAT_DIR=C:/app/data/IUCLID6_6_format_9.0.0`. Compose passes these through (see `environment:` in `docker-compose.yml`); caches default to `C:/app/data/offline` and `C:/app/data/offline_cache` unless overridden.

### Linux/macOS note

The provided container stack targets Windows containers because OPERA CLI in this workflow is a Windows executable.
On Linux/macOS, run the app locally without OPERA, or set `HAZQUERY_OPERA_EXE` to a compatible native binary if available.

---

## Offline REACH / IUCLID (optional)

The Streamlit app can read **offline REACH study-result dossiers** (`.i6z` inside a `.zip`) and decode IUCLID picklist codes when you install the **IUCLID format** phrase package.

> **Demo vs full database:** Anything committed under `data/reach_demo/` is a **demo subset** for GitHub / Streamlit Cloud size limits. It is **not** the official full REACH export. **Most substances have no dossier** in that zip; study text, endpoints, and GHS-style rows can be **missing or incomplete** even when a dossier exists. The app uses **heuristic** XML parsing, not a certified IUCLID engine. **Do not** use the demo bundle for regulatory submissions, registration completeness, or as a substitute for ECHA’s own tools and downloads — use a **local** full archive when you need authoritative coverage.

### Downloading the full official IUCLID and REACH packages (ECHA)

Use ECHA’s **[IUCLID 6 downloads](https://iuclid6.echa.europa.eu/downloads)** page in a browser. That page lists the current **IUCLID 6 format** bundle and the **REACH study results dossiers** bulk export (names and versions change over time; pick the latest entries that match those descriptions). Accept ECHA’s terms if the site asks you to.

1. **IUCLID 6 format (phrase / picklist / XSD)** — Download the **IUCLID 6 format** ZIP (often named like `IUCLID6_6_format_*.zip`, on the order of ~100 MB). Extract it to a folder on your machine. Point **`IUCLID_FORMAT_DIR`** at the **extracted** folder (the directory that contains `dcr.xml` and the phrase / properties files). The same tree can be copied into this repo as `data/iuclid_format/IUCLID_6_9_0_0_format/` for demos only — it does **not** replace downloading from ECHA when you update or audit your local setup.
2. **REACH study results dossiers (full bulk)** — Download the **REACH study results dossiers** archive (`reach_study_results_dossiers_*.zip`). That file is **very large** (~10+ GB or more, depending on release) and contains the per-substance **`.i6z`** dossiers. Point **`OFFLINE_LOCAL_ARCHIVE`** at that `.zip` **or** at a folder where you have extracted the `.i6z` files.

**This repository vs the official downloads:** This repo ships **only a demo portion** of the dossier bulk (`data/reach_demo/reach_subset.zip` — a small hand-picked or script-built subset for GitHub / Streamlit Cloud). It may also ship a **copy of the format tree** under `data/iuclid_format/` for hosted decoding. Neither replaces the **full** REACH dossier archive from ECHA; for complete substance coverage you **must** download the bulk `reach_study_results_dossiers_*.zip` (or equivalent) yourself from the link above and set `OFFLINE_LOCAL_ARCHIVE` locally.

Configure paths via environment variables or `.streamlit/secrets.toml` (see `.streamlit/secrets.example.toml`):

| Variable | Meaning |
|----------|---------|
| `OFFLINE_LOCAL_ARCHIVE` | Full path to the official `reach_study_results_dossiers_*.zip` **or** to a folder that already contains `.i6z` files. |
| `IUCLID_FORMAT_DIR` | Full path to the **extracted** IUCLID 6 format folder from step 1. |

The app extracts or scans the archive location and builds caches under `OFFLINE_CACHE_DIR` (default: `data/offline_cache/`).

### IUCLID offline data for Streamlit Cloud

The full REACH dossier bulk (~10+ GB) **must not** be committed to GitHub. For **Streamlit Community Cloud**, commit **as much as is practical** within GitHub / LFS limits: the format tree plus a zip of selected `.i6z` dossiers. That zip is still a **demo database** relative to full REACH — **most CAS numbers will have no dossier**, and fields may be **missing or incomplete** even when present.

| Path | Purpose |
|------|---------|
| `data/iuclid_format/IUCLID_6_9_0_0_format/` | Extracted **IUCLID 6 format** tree (phrase / picklist / XSD; ~100 MB). Copy from your local `IUCLID 6 9.0.0_format` folder; use a **name without spaces**. This is the **format** package (decoding), not the dossier bulk. |
| `data/reach_demo/reach_subset.zip` | Zip of selected `.i6z` dossiers — **demo / UI / teaching** only. Increase count until file size limits bite; coverage stays **non-exhaustive**. See **`data/reach_demo/README.md`**. |

The REACH / IUCLID panel is **supplementary** and, when using these committed paths, **non-authoritative** (see the disclaimer blockquote above).

After those exist on `v2.0`, the app **defaults** `OFFLINE_LOCAL_ARCHIVE` and `IUCLID_FORMAT_DIR` on Cloud when Secrets leave them unset (overridable; set `HAZQUERY_DISABLE_REPO_IUCLID_DEFAULTS=1` to turn off). Prepare assets locally with:

```bash
python scripts/prepare_iuclid_demo.py --format-src "PATH/TO/IUCLID 6 9.0.0_format" --i6z-dir "PATH/TO/FOLDER_WITH_I6Z" --limit 20
```

More detail: **`data/echa_cloud/README.txt`** and **`.streamlit/secrets.example.toml`**.

### Run locally with offline REACH (Windows)

After you download the ECHA **REACH study results** archive, you can start the app without editing `.env`:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_streamlit_with_offline_reach.ps1 `
  -ReachArchive "C:\path\to\reach_study_results_dossiers_23-05-2023.zip"
```
(Use `pwsh` instead of `powershell` if you have PowerShell 7 installed.)

Optional: `-Port 8502`. Then open **Hazard assessment** → expand **REACH / IUCLID (offline dossier)** after assessing a CAS.

### IUCLID format package (phrase mapping) — recap

The **IUCLID 6 format** ZIP is obtained from the same **[IUCLID 6 downloads](https://iuclid6.echa.europa.eu/downloads)** page as in [Downloading the full official IUCLID and REACH packages (ECHA)](#downloading-the-full-official-iuclid-and-reach-packages-echa); set **`IUCLID_FORMAT_DIR`** to the extracted folder (the directory that contains `dcr.xml`, `*.properties`, etc.).

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
| `P2OASYS_MATRIX_PATH` | No | P2OASys hazard matrix Excel (default under `data/`). If missing, a dev placeholder is auto-written unless `P2OASYS_DISABLE_AUTO_PLACEHOLDER=1`. |
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
5. **`packages.txt`:** Put **only** valid Debian package names, **one per line**. Do not use `#` comments — Streamlit Community Cloud does not ignore them; tokens from comment lines are passed to `apt-get` and the dependency step fails.

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
