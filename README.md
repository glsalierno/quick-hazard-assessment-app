
# Quick Hazard Assessment — Streamlit App

Interactive web app for **chemical hazard assessment** from **PubChem**, **DSSTox**, and optional local/network sources (no API key required for core lookups). **P2OASys** Auto6 drafts combine measured evidence (SDS, IUCLID, CAMEO, lookups) with optional **OPERA** (local) and **ECOSAR** (EPI Suite HTTP API — no Windows EPI install required).

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://quick-hazard-assessment-app.streamlit.app)

**Branch note:** active development for GHaz7 / v7 P2OASys lives on branch **`v7`**.

---

## Features

### Core report
- **Input:** CAS number (e.g. `67-64-1`) or chemical name
- **DSSTox local:** CAS → DTXSID from a local mapping file (no EPA CompTox API key)
- **PubChem:** Properties, GHS H/P codes with phrase legends, flash point, vapor pressure, IUPAC name, SMILES
- **CAMEO Chemicals NFPA 704:** local desktop sqlite or bundled `data/cameo_nfpa.sqlite` (not a website scrape)
- **Molecular structure:** 2D rendering (client-side [smiles-drawer](https://reymond-group.github.io/smilesDrawer/))
- **Graceful fallback:** Missing DSSTox → **PubChem-only** mode
- **Download:** Report as CSV; Zenodo DOI reminder for research use

### SDS upload (v1.4)
- Pipelines: **Hybrid** (`hybrid_md_ocr`) and **MarkItDown + regex** only — [docs/SDS_EXTRACTION_PIPELINES.md](docs/SDS_EXTRACTION_PIPELINES.md)
- Optional OCR (Tesseract / EasyOCR + Poppler) — [docs/OCR_SETUP.md](docs/OCR_SETUP.md)
- Optional local LLM (Ollama) for other flows — [docs/OLLAMA_SETUP.md](docs/OLLAMA_SETUP.md)

### P2OASys human-in-the-loop
- Sidebar page **P2OASys Assessment** (`pages/05_P2OASys_Assessment.py`): Generate draft → review Auto6 subcategory scores (2/4/6/8/10) → export
- **Process Factors** and **Life Cycle Factors** are manual-only (never auto-filled)
- Expert / auto ribbon from `data/p2oasys_score_lookup.sqlite` when present

### Auto P2OASys evidence stack (priority)
| Priority | Source | What it fills | Needs |
|----------|--------|---------------|--------|
| 1 | Lookups (IARC, ODP/GWP, IPCC, CAA §112 HAP/NESHAP) | Atmospheric / chronic lists | CSV / parquet under config paths |
| 2 | SDS (upload / structured) | Acute tox, flash, VP, eco phrases, GHS | PDF + extraction stack |
| 3 | IUCLID / REACH offline | Measured acute/chronic/eco when dossiers cached | Large local archive (optional) |
| 4 | CAMEO NFPA | Health / Fire diamonds | Bundled or desktop sqlite |
| 5 | **OPERA** (local CLI) | Fate (LogP, BCF, ReadyBiodeg), CATMoS LD50 gap-fill | Windows `OPERA.exe` **or** `opera_precompute.sqlite` |
| 6 | **ECOSAR** (EPI Suite **HTTP API**) | Aquatic LC50/EC50 + ChV (predicted) when measured eco missing | **Network only** — see below |
| — | Acid rain / pH heuristics | Atmospheric acid rain; Physical pH | Formula / pKa / SMARTS (no extra install) |

Scores are **never invented as integers in clients** — clients emit evidence (mg/L, phrases); `utils/p2oasys_scorer.py` applies the TURI matrix bands.

---

## ECOSAR / EPI Suite API (works alone in this GitHub repo)

**Yes — the GitHub clone can use ECOSAR without installing EPA EPI Suite or Ecowinnt.exe.**

| Mode | How | Local EPI Suite 4.x? |
|------|-----|----------------------|
| **Default (recommended)** | HTTPS to [EPI Suite CLI API](https://episuite.dev/api) (`ecosar.organics` → ECOSAR **v2.20** JSON) | **No** |
| Offline / air-gap | Point `HAZQUERY_EPISUITE_API_BASE` at a local [pyepisuite](https://pypi.org/project/pyepisuite/) ≥1.3 JAR reverse-proxy | Optional JAR download |
| Desktop Ecowinnt.exe | GUI-only (ECOSAR v1.11); **not** used by this app | Installed on some lab PCs; ignore for automation |

Implementation: `utils/ecosar_client.py` (uses `requests`, already in `requirements.txt`). Results are tagged **`predicted`** and merged **after** IUCLID/SDS. Cache: `data/ecosar_cache.sqlite` (gitignored).

```bash
# Smoke test (needs outbound HTTPS)
python -c "from utils.ecosar_client import fetch_ecosar_extra_sources; print(fetch_ecosar_extra_sources('71-43-2'))"
```

| Variable | Default | Meaning |
|----------|---------|---------|
| `HAZQUERY_EPISUITE_API_BASE` | `https://episuite.dev/api` | API root (OpenAPI) |
| `HAZQUERY_EPISUITE_API_KEY` | (empty) | Optional Bearer token |
| `HAZQUERY_SKIP_ECOSAR` / `HAZQUERY_ECOSAR=0` | off | Disable network ECOSAR |
| `ECOSAR_CACHE_DB_PATH` | `data/ecosar_cache.sqlite` | On-disk cache |

Details: [docs/ECOSAR_EPI_SUITE_FEASIBILITY.md](docs/ECOSAR_EPI_SUITE_FEASIBILITY.md). Harvest overlay: `python scripts/overlay_ecosar_on_auto.py`.

**Teams / DoSS note:** the slim **DoSS on-demand** app does **not** call ECOSAR live — it reads **precomputed** Auto6 / overall scores from `p2oasys_score_lookup.sqlite`. That is simpler for Teams packaging (no API dependency at display time). Recompute autos in this repo (or hazquery overlays), then copy/refresh the sqlite the Teams DoSS points at.

**Streamlit Community Cloud:** ECOSAR works if the host allows outbound HTTPS to `episuite.dev`. **OPERA does not** (Windows GUI/CLI). Prefer precompute sqlite + API ECOSAR on Cloud; set `HAZQUERY_SKIP_ECOSAR=1` if the platform blocks egress.

---

## OPERA (local QSAR — optional)

- Prefer non-parallel `…\OPERA\application\OPERA.exe`; set `HAZQUERY_OPERA_EXE` / `OPERA_JAVA_HOME` as needed.
- Batch cache: `data/opera_precompute.sqlite` (`scripts/precompute_opera_for_cas_list.py`).
- Headless merge uses precompute when the exe is missing (`utils/p2oasys_extras_merge.py`).

---

## Run locally

1. **Clone and enter the repo**
   ```bash
   git clone https://github.com/glsalierno/quick-hazard-assessment-app.git
   cd quick-hazard-assessment-app
   git checkout v7   # P2OASys / ECOSAR stack
   ```

2. **Create a virtual environment and install dependencies**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate   # Windows
   # source .venv/bin/activate  # Linux/macOS
   pip install -r requirements.txt
   ```
   Core P2OASys + ECOSAR need only what is in `requirements.txt` (`requests`, `pandas`, `openpyxl`, Streamlit, …). Heavy optional stacks (Docling, torch, EasyOCR) are listed there for SDS; on constrained hosts set `HAZQUERY_DISABLE_DOCLING=1`.

3. **DSSTox mapping (optional but recommended)**
   - Download the [EPA Figshare CAS–DTXSID mapping](https://epa.figshare.com/articles/dataset/DSSTox_Identifiers_Mapped_to_CAS_Numbers_and_Names_File_11_14_2016/5588566) (CSV).
   - Place it in the **`DSS/`** folder (e.g. `DSS/cas_dtxsid_mapping.csv`).
   - See **`DSS/README.md`**. If missing, the app runs in PubChem-only mode.

4. **Run the app**
   ```bash
   streamlit run app.py
   ```
   Open the URL shown in the terminal (usually http://localhost:8501).

   **P2OASys assessment:** sidebar → **P2OASys Assessment**. Enter a CAS (and optional SDS), **Generate draft**, review subcategory dropdowns, export JSON/HTML.

   **Tip:** Test locally before Streamlit Cloud redeploys (LFS bandwidth).

5. **Optional — Local LLM (Qwen / Gemma) for SDS extraction** — [docs/OLLAMA_SETUP.md](docs/OLLAMA_SETUP.md).

6. **Run SDS examples (batch)** — `python scripts/run_sds_examples.py [--limit N] [--compare]`.

7. **OCR for scanned SDS PDFs** — [docs/OCR_SETUP.md](docs/OCR_SETUP.md).

8. **SDS CAS extraction** — [docs/SDS_EXTRACTION_PIPELINES.md](docs/SDS_EXTRACTION_PIPELINES.md).

9. **Windows PATH / pip warnings** — use the workspace terminal so `.vscode/settings.json` applies.

10. **SDS parsing agreement report** — `python scripts/sds_parsing_accuracy_report.py --folder "sds_examples" --out-dir artifacts`.

11. **IUCLID / offline REACH (optional)** — [Offline REACH / IUCLID](#offline-reach--iuclid-optional).

12–13. **P2OASys validation scripts** — see previous sections below (fast reference / expert CAS).

---

## Capabilities checklist

| Capability | In this GitHub app | Notes |
|------------|-------------------|--------|
| PubChem + GHS report | Yes | Always |
| DSSTox / ToxVal / CPDB | Yes | SQLite or CSV fallbacks |
| SDS PDF → CAS / fields | Yes | Hybrid / MarkItDown; OCR optional |
| CAMEO NFPA | Yes | Bundled slim sqlite or desktop path |
| P2OASys Auto6 draft + HITL | Yes | Official matrix Excel recommended |
| IARC / ODP / GWP / NESHAP | Yes | Lookup CSVs / atmo parquet |
| Acid rain + pH heuristics | Yes | No extra binary |
| OPERA fate / CATMoS | Optional | Local exe or precompute DB |
| **ECOSAR aquatic** | **Yes (API)** | No EPI Suite install; or skip / offline JAR |
| IUCLID measured studies | Optional | Large REACH zip + cache rebuild |
| QSAR Toolbox / VEGA | Optional | Windows WebSuite |
| DoSS row export | Sibling **DoSS on-demand** / Teams pack | Reads lookup sqlite; no live ECOSAR |

---

## P2OASys validation scripts

### vs `fastP2OASys` reference (`--source fast`)

Compares category-level expert scores to this app’s pipeline (`ChemicalAssessmentService` → `build_hazard_data` → `compute_p2oasys_scores`), with the same optional IARC / ODP–GWP / IPCC / IUCLID / ECOSAR merges as the P2OASys tab.

```bash
python scripts/validate_p2oasys_vs_fast_reference.py --source fast --limit 50 -o data/p2oasys_validation.csv
```

Use the **official TURI matrix** for meaningful errors. See [docs/P2OASYS_LOOKUP_SOURCES.md](docs/P2OASYS_LOOKUP_SOURCES.md).

### Offline expert CAS (`--source expert`, default for `run_full_validation`)

```bash
python scripts/run_full_validation.py --cas-file cas_list.txt \
  --archive-dir path/to/allP2OASys_120825 --fastp2oasys-dir path/to/fastP2OASys -o validation_report.csv
```

Retrieval-only: add `--check`. Optional Playwright scrape mode: `--source scraped` (respect [p2oasys.turi.org](https://p2oasys.turi.org) terms).

```bash
python scripts/validate_p2oasys_vs_fast_reference.py --source expert --cas 67-63-0 \
  --archive-dir path/to/allP2OASys_120825 --fastp2oasys-dir path/to/fastP2OASys \
  -o data/p2oasys_expert_validation_comparison.csv
```

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
| `CAMEO_SQLITE` | No | Optional path to CAMEO Chemicals `cameo.sqlite` or bundled `data/cameo_nfpa.sqlite`. |
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
├── config.py
├── requirements.txt       # Includes requests (ECOSAR API); no EPI Suite binary
├── pages/05_P2OASys_Assessment.py
├── DSS/                   # DSSTox mapping (Git LFS)
├── data/
│   ├── chemical_db.sqlite
│   ├── cameo_nfpa.sqlite
│   ├── p2oasys_harvest.sqlite
│   ├── p2oasys_score_lookup.sqlite
│   ├── opera_precompute.sqlite   # optional
│   └── ecosar_cache.sqlite       # runtime cache (gitignored)
├── docs/
│   ├── ECOSAR_EPI_SUITE_FEASIBILITY.md
│   ├── SDS_EXTRACTION_PIPELINES.md
│   ├── OCR_SETUP.md
│   └── OLLAMA_SETUP.md
├── scripts/
│   ├── overlay_ecosar_on_auto.py
│   ├── overlay_opera_iuclid_fate_eco.py
│   ├── run_priority62_auto_assess.py
│   ├── run_harvest_full_auto_assess.py
│   └── setup_chemical_db.py
└── utils/
    ├── ecosar_client.py          # EPI Suite HTTP API → aquatic extras
    ├── opera_client.py
    ├── p2oasys_extras_merge.py   # lookups → OPERA → IUCLID → ECOSAR → CAMEO
    ├── p2oasys_scorer.py
    └── …
```

---

## Citation

If this tool contributes to your research, please cite:

- **Zenodo:** [DOI 10.5281/zenodo.19056294](https://doi.org/10.5281/zenodo.19056294)
- **Repository:** publish your own fork or tarball; do not rely on private paths in configuration.

---

## License

MIT (see [LICENSE](LICENSE)).

## TCI SDS (Akamai-friendly defaults)

TCI HTTP SDS acquisition defaults to a **5s** shared min-interval, **documentSearch fallback off**, and a **12 min cooldown** after HTTP 403/401. Prefer the curated/learned product map and SDS cache; paste a product code (letter + 4 digits) once when live download is blocked. Details: `../docs/TCI_HTTP_WORKFLOW.md` and `../../../GHhaz5/report_tci_gentle.md`. Restart Streamlit after changing TCI env vars.

## P2OASys score lookup (ribbon / TCI skip)

Precomputed expert + auto category scores live in `data/p2oasys_score_lookup.sqlite` (Git LFS).

- **Expert harvest:** `data/p2oasys_harvest.sqlite` from p2oasys.turi.org pages 1–101. Single-CAS rows only; multi-CAS mixtures are parked and not inherited onto one CAS. Rebuild with `python scripts/build_p2oasys_harvest_sqlite.py`.
- **Lookup used by the app / DoSS:** seed expert from harvest, then auto-assess missing CAS (`python scripts/run_harvest_full_auto_assess.py`). Rebuild lookup with `python scripts/build_p2oasys_score_lookup.py`.
- UI: P2OASys Assessment page shows an Expert/Auto ribbon on CAS entry; TCI auto-fetch is skipped when scores are cached (opt-in override available).

