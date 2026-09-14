# P2OASys lookup tables: where to source IARC, ODP, GWP

Optional lookup tables (CAS → IARC, ODP, GWP) improve P2OASys scoring for **Chronic Human Effects** and **Atmospheric Hazard** when PubChem/SDS lack these values. Below are authoritative sources and how to build/use CSV tables the app can load.

---

## 1. IARC (carcinogen classification)

**Use in P2OASys:** Chronic Human Effects (IARC category: 1, 2A, 2B, 3, 4).

### iarc folder (fastP2OASys/iarc) — built-in

The app can load **IARC classifications** directly from the **iarc** folder (e.g. `fastP2OASys/iarc`). Place the official CSV or Excel there (e.g. *List of Classifications - IARC Monographs ... .csv* or *Agents Classified by the IARC Monographs, Volumes 1-140.xlsx*). The loader expects columns **CAS No.** (or *CAS No*) and **Group** (values 1, 2A, 2B, 3, 4). CSV is preferred; Excel used as fallback.

- **Config:** `config.IARC_DIR` (default: `../fastP2OASys/iarc` relative to the app, or set env `IARC_DIR`).
- **Loader:** `utils.iarc_lookup.load_iarc_from_iarc_folder(iarc_dir)` → `dict[normalized_cas, group]`.
- **Priority:** If `IARC_DIR` exists and contains a valid CSV/Excel, the app uses it; otherwise it falls back to the optional `P2OASYS_IARC_CSV_PATH` CSV.
- **Scraper:** To refresh data from the official IARC page, run `python scripts/scrape_iarc_classifications.py` from the app root. Saves CSV/JSON (and optionally Excel) into `IARC_DIR`, including `iarc_classifications_latest.csv` for the app. Requires `beautifulsoup4` (see `requirements.txt`).

### Other sources (for manual CSV)

| Source | Format | Notes |
|--------|--------|--------|
| **IARC List of Classifications** | Online spreadsheet | https://monographs.iarc.who.int/list-of-classifications — searchable by name, group, **CAS number**, volume, year. Export to CSV/Excel if the interface allows. |
| **Agents Classified by the IARC Monographs** | PDF | https://monographs.iarc.who.int/wp-content/uploads/2018/09/ClassificationsAlphaOrder.pdf — includes CAS, agent name, group, volume, year. Requires scraping or manual table extraction to build CAS → group. |
| **IARC Publications** | Web / PDF | https://publications.iarc.fr — monographs by volume; CAS often in individual monograph PDFs. |

### Building a CSV

- **Columns:** `cas`, `iarc` (e.g. `1`, `2A`, `2B`, `3`, `4`).
- **CAS format:** Normalized (e.g. `67-64-1` or `67641`); app normalizes when loading.
- **Example:** `cas,iarc\n67-64-1,3\n50-00-0,1`

If the official list is only PDF, you can maintain a small CSV of high-interest compounds and extend it from the PDF or from the online spreadsheet export.

---

## 2. IPCC GWP 100-year (atmo folder) — built-in

**Use in P2OASys:** Atmospheric Hazard (GWP 100-year, kg CO2e/kg).

The app can load **IPCC GWP 100-year** directly from the **atmo** folder (e.g. `fastP2OASys/atmo`). That folder should contain the Federal LCA Commons parquet, e.g. `IPCC_v1.1.1_27ba917.parquet`, with columns `Indicator` (AR6-100, AR5-100, AR4-100), `CAS No`, and `Characterization Factor`. The loader prefers **AR6-100**, then AR5-100, then AR4-100. No CSV needed.

- **Config:** `config.ATMO_DIR` (default: `../fastP2OASys/atmo` relative to the app, or set env `ATMO_DIR`).
- **Loader:** `utils.atmo_gwp.load_ipcc_gwp_100_from_atmo(atmo_dir)` → `dict[normalized_cas, gwp_float]`.
- **Dependency:** `pyarrow` (for `pandas.read_parquet`) in `requirements.txt`.

---

## 3. ODP / GWP from CSV (atmospheric hazard)

**Use in P2OASys:** Atmospheric Hazard (ozone depletion potential, global warming potential) when not in atmo parquet.

### Sources

| Source | Format | Notes |
|--------|--------|--------|
| **EPA CompTox Chemicals Dashboard – Refrigerants** | Web / API | https://comptox.epa.gov/dashboard/chemical-lists/REFRIGERANTS — list of refrigerants with CAS; property data (GWP/ODP) may be in the dashboard or via API. |
| **NIST Refrigerants Database** | NIST report / tables | NIST publishes tables with CAS, ASHRAE designation, **GWP** (100-yr CO2-eq), **ODP** (vs CFC-11). See e.g. *Properties of Refrigerants* (NIST) — may be PDF or spreadsheet. |
| **EPA SNAP (Significant New Alternatives Policy)** | Web / PDF | https://www.epa.gov/snap — substitute refrigerants; documents include CAS, ODP, GWP for many compounds. |
| **GWP-ODP Calculator (UNEP)** | Web tool | https://www.ozonaction.org/gwpodpcalculator/ — conversion tool; underlying data may be exportable or documented. |
| **IPCC / WMO** | Reports | GWP values from IPCC (e.g. Myhre et al.); ODP from WMO. Typically used as primary references; CAS-indexed tables are built by NIST/EPA from these. |

### Building a CSV

- **Columns:** `cas`, `odp`, `gwp` (numeric; ODP relative to CFC-11, GWP relative to CO2 over 100 yr).
- **Example:** `cas,odp,gwp\n75-45-6,0,1\n811-97-0,0,0`

Start with a small table of common refrigerants and high-GWP/ODP compounds; extend from NIST tables or EPA SNAP lists.

---

## 4. Using lookup tables in the app

The app merges optional lookups into `hazard_data` for P2OASys:

- **IARC:** Set `config.IARC_DIR` to the iarc folder (default: `../fastP2OASys/iarc`). The app loads the first valid CSV or Excel with columns *CAS No.* and *Group*. If the folder is missing or empty, it falls back to optional `P2OASYS_IARC_CSV_PATH` CSV.
- **IPCC GWP 100-year:** Set `config.ATMO_DIR` to the atmo folder (default: `../fastP2OASys/atmo`). The app loads `IPCC_*.parquet` and uses AR6-100 (then AR5-100, AR4-100) by CAS. No CSV needed.
- **ODP/GWP CSV:** Optional CSV with columns `cas`, `odp`, `gwp`. Merged into `hazard_metrics["other_designations"]`. GWP from atmo (IPCC) overrides GWP from this CSV when both exist.

See **utils/iarc_lookup.py**: `load_iarc_from_iarc_folder(iarc_dir)`. **utils/atmo_gwp.py**: `load_ipcc_gwp_100_from_atmo(atmo_dir)`. **utils/lookup_tables.py**: `load_iarc_csv`, `load_odp_gwp_csv`, `get_lookup_extra_sources(cas, ..., ipcc_gwp_by_cas=...)`.

---

## 5. QSAR Toolbox (VEGA) — local, no API key

**Use in P2OASys:** Endpoint data (physicochemical, toxicity, fate) from the **OECD QSAR Toolbox** with **VEGA** (and optionally OPERA) add-ons. All run locally; no cloud API key.

- **Requires:** [QSAR Toolbox](https://qsartoolbox.org/download/) installed (Windows only), **WebSuite** running, and [PyQSARToolbox](https://github.com/glsalierno/PyQSARToolbox):  
  `pip install git+https://github.com/glsalierno/PyQSARToolbox.git`
- **Config:** Set `QSAR_TOOLBOX_PORT` to the port shown when WebSuite starts (e.g. `51946`). The app connects to `http://127.0.0.1:<port>`.
- **Flow:** Search by CAS (or SMILES), retrieve all endpoint data, merge into `extra_sources` for P2OASys and show in the Hazard tab as “QSAR Toolbox (VEGA)”.
- **Module:** `utils.qsar_toolbox_client` — `is_available(port)`, `fetch_by_cas(cas, port)`, `toolbox_results_to_extra_sources(rows)`.

---

## 6. Summary

| Table | Primary source | Suggested format |
|-------|----------------|------------------|
| **IARC** | IARC List of Classifications (online spreadsheet or PDF) | CSV: `cas,iarc` |
| **ODP/GWP** | NIST refrigerants table, EPA SNAP, CompTox refrigerants list | CSV: `cas,odp,gwp` |
| **QSAR Toolbox (VEGA)** | Local OECD Toolbox + VEGA/OPERA add-ons (Windows) | PyQSARToolbox + WebSuite running; set `QSAR_TOOLBOX_PORT` |

These tables and the optional QSAR Toolbox are **optional**. When present, the app uses them to fill gaps so P2OASys scoring is closer to expert assessment.

---

## 7. Expert P2OASys, CHEM21, and Hansen (lookup, not prediction)

The P2OASys tab **looks up** expert scores first. It does **not** auto-predict a P2OASys number.

| Table | Default path | If CAS missing |
|-------|----------------|----------------|
| **P2OASys expert** | `fastP2OASys/data/p2oasys_single_cas_clean.csv` (`P2OASYS_EXPERT_CSV` / `FASTP2OASYS_DIR`) | UI: **P2OASys assessment required** |
| **CHEM21 guide** | `fastP2OASys/CHEM21/vendor/solvent_flashcards/.../CHEM21_full.csv` | “CHEM21 rating not in solvent guide” |
| **Hansen δD/δP/δH** | DoSS `DOSS.xlsx` (`DOSS_XLSX`) and/or `HSPIP_CACHE_CSV` | Compute with [cas-to-HSPiP_data](https://github.com/glsalierno/cas-to-HSPiP_data) |

Automatic PubChem→matrix scoring remains behind an **Experimental** checkbox and is not used for decisions.

Module: `utils/solvent_reference_lookup.py`.

---

## 8. Related folders (same `hazquery` tree)

Use these alongside **Quick Hazard Assessment** for calibration checks and expert reference scores.

### `fastP2OASys` (`…/hazquery/fastP2OASys`)

- **`iarc/`** — IARC monographs CSV (e.g. *List of Classifications …*). Point **`IARC_DIR`** here (or copy into `quick-hazard-assessment-app/data/iarc`) so Chronic Human Effects matches the fast-P2OASys setup.
- **`atmo/`** — Federal LCA / IPCC-style parquet and metadata JSON. Point **`ATMO_DIR`** here (or under `data/atmo`) for Atmospheric Hazard GWP loading, same as fast-P2OASys.
- **Reference scores** — `P2OASys_Category_Scores_Data_March_18_2026.csv` and `P2OASys_Category_Scores_Data_March_18_2026 (1).csv` (expert / reference category and subcategory scores).
- **`compare_fast_p2oasys_to_reference.py`** — Compares PubChem-driven “fast” scores to those reference CSVs; expects the **same TURI matrix** as this app (`Hazard Matrix Group Review 9-19-23.xlsx`, historically under `hazquery/` — mirror that path or set **`P2OASYS_MATRIX_PATH`** consistently).

### `allP2OASys_120825` (`…/hazquery/allP2OASys_120825`)

- **Selenium scripts** (`p2oasys_query*.py`, `p2oasys_query_batch.py`) — Query **p2oasys.turi.org** “load from database” for expert **Evaluation** scores; batch CSV outputs (e.g. `P2OASys_Database_Results_*.csv`).
- **`p2oasys_scrape_subcategories_README.md`** — Scraper notes for pulling **subcategory** columns from the site when exposed.

This app does **not** call those scripts automatically; use them offline to validate or to build comparison datasets. For in-app scoring, still supply the **official matrix** file when you need TURI-aligned numbers (see placeholder warning in the P2OASys tab if only the dev workbook is present).

**In-repo validation:** run `python scripts/validate_p2oasys_vs_fast_reference.py` from `quick-hazard-assessment-app` (see README §12) to emit a comparison CSV against `fastP2OASys/P2OASys_Category_Scores_Data_March_18_2026.csv` (category rows × chemical-name columns).
