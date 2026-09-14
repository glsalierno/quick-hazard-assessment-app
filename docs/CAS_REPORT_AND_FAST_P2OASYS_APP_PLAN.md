# Plan: CAS Information Report + Fast P2OASys (First 6) App

**Goal:** Add two app experiences: (1) a **report of all available information** for a given CAS, and (2) **Fast P2OASys** for the **first 6 of 8** categories with expandable subcategory menus and a report table (Name, CAS, value per subcategory, source). Run a **quality test** of current code before pushing to GitHub.

---

## 1. Tab 1: CAS Information Report

**Purpose:** Single place to see every data source we have for a chemical (CAS).

### 1.1 Data sources to include (by priority)

| Source | What to show | Already in app? |
|--------|----------------|------------------|
| **PubChem** | Identifiers (CAS, SMILES, InChI, IUPAC), formula, MW, GHS (H/P, signal word), toxicities, flash point, vapor pressure, NFPA | ✓ |
| **DSSTox local** | DTXSID, preferred name, names list, molecular/structure fields | ✓ |
| **ToxValDB** | Category → records (value, units, species, route, study type) | ✓ |
| **CPDB** | TD50, route, species, experiments list | ✓ |
| **IARC** | Group (from `fastP2OASys/iarc` or optional CSV) | ✓ |
| **ODP/GWP** | ODP, GWP from optional CSV | ✓ |
| **IPCC GWP** | GWP 100-yr from atmo parquet | ✓ |
| **ECHA** | Harmonized/notified classifications (H-codes, hazard class) | Via hazard_scrapers |
| **Danish QSAR** | Predictions by endpoint (skin sens., mutagenicity, fish, etc.) | Via hazard_scrapers |
| **VEGA** | Predictions (if API key) | Via hazard_scrapers |
| **ICE** | In vivo / in vitro / properties (if API key) | Via hazard_scrapers |
| **SDS (regex)** | Extracted fields when user has uploaded SDS for this CAS | ✓ (optional) |

### 1.2 UI sketch

- **Input:** Same CAS/name input as main app (or reuse session `result_for`).
- **Layout:** Sections per source (e.g. "PubChem", "DSSTox", "ToxValDB", …). Each section shows:
  - Available: Yes/No (or "Not loaded" for optional DBs).
  - Key fields in a compact table or key-value list.
  - Optional: "Source" badge (e.g. "PubChem", "IARC folder", "ECHA").
- **Download:** Single "Download full CAS report" (JSON or CSV) combining all sections with a "source" column per row.

### 1.3 Implementation notes

- Reuse existing `result_data` (pubchem, dsstox_info, toxval_data, carc_potency_data, clean_cas).
- Optionally call `HazardDataAggregator.search_chemical(clean_cas, id_type="cas")` and include ECHA/Danish/VEGA/ICE in the report (with clear "external scraper" labeling and rate limits).
- IARC: keep using local DB lookup only; show "IARC: Group 2B" and source "IARC folder" or "IARC CSV".

---

## 2. Tab 2: Fast P2OASys (First 6 Categories)

**Purpose:** Focus on the first 6 P2OASys categories with expandable subcategory menus and a **report table**: Name, CAS, value per subcategory, and **source** for each value.

### 2.1 First 6 categories (from TURI matrix)

1. **Acute Human Effects** — Oral/Dermal/Inhalation toxicity (LD50/LC50, GHS H).
2. **Chronic Human Effects** — IARC, EPA carcinogen, etc.
3. **Ecological Hazards** — Aquatic LC50/EC50, GHS H aquatic.
4. **Environmental Fate & Transport** — Persistence, bioaccumulation (when we have data).
5. **Atmospheric Hazard** — ODP, GWP (from lookup/atmo).
6. **Physical Properties** — Flash point, vapor pressure, NFPA.

*Excluded from this tab:* Process Factors, Life Cycle Factors (categories 7–8).

### 2.2 Subcategories as expandable menus

- **Structure:** One expander per **category**; inside it, one sub-expander or section per **subcategory** (from matrix: e.g. "Oral Toxicity", "Inhalation Toxicity", "Carcinogenicity", "Aquatic toxicity", "Flammability", …).
- **Content per subcategory:** List of **endpoints/features** (e.g. "LD50 mg/kg", "GHS H phrases") with:
  - **Value** (e.g. "250 mg/kg", "H302, H312").
  - **Source** (e.g. "PubChem", "ToxValDB", "IARC folder", "ECHA", "Danish QSAR").
- **Report table (below or in a separate section):**  
  Columns: **Name**, **CAS**, then one column per **subcategory** (or per endpoint), then **Source** (or one "Source" column per subcategory).  
  Rows: one row per chemical (for single-CAS view, one row; for batch, multiple rows).  
  Each cell = value extracted for that subcategory; source can be in the same cell as "value (source: X)" or in a separate column.

### 2.3 Data flow

- Reuse `build_hazard_data(pubchem_data, toxval_data, carc_potency_data, extra_sources)`.
- `extra_sources` = IARC/ODP/GWP/IPCC from lookups + optional scraper results via `scraper_results_to_extra_sources(chemical_data)`.
- Run `compute_p2oasys_scores(hazard_data, matrix)` to get scores; **additionally** build a **value + source** map per subcategory from:
  - `hazard_data["toxicities"]`, `hazard_data["ghs"]`, `hazard_data["hazard_metrics"]` (and which key came from which source: PubChem vs toxval vs extra_sources vs scrapers).
- Track source when merging: e.g. tag each toxicity/ghs entry with `source` when building `hazard_data` (may require a small change in `build_hazard_data` or a parallel structure that records source per item).

### 2.4 Implementation notes

- **Matrix-driven subcategories:** Use `p2oasys_scorer.load_p2oasys_matrix()` and iterate only over the first 6 categories (by name or by sheet order). Subcategory list = keys of `matrix[category]` (excluding `_category_max` etc.).
- **Value extraction:** For each subcategory/unit, the scorer already maps hazard_data → score. For the report we need the **raw value** that led to that score (e.g. LD50 250, H302) and the **source**. Options:
  - (A) In `compute_p2oasys_scores`, optionally return a side structure "value_used" and "source" per (category, subcategory, unit).
  - (B) Build a separate "evidence" structure from `hazard_data` + known source tags (e.g. "toxicities[0] from PubChem", "toxicities[1] from ToxValDB") and match to matrix units.
- **Report table:** DataFrame with columns `Name`, `CAS`, then dynamic columns for each subcategory (value or "value (source: X)"), then optional `Source` column(s). Download as CSV.

---

## 3. Quality Test (Before Push to GitHub)

**Purpose:** Validate current codebase so we don’t push broken code.

### 3.1 Test script: `scripts/quality_test_pre_push.py`

**Checks:**

1. **Imports**
   - `config`, `utils.pubchem_client`, `utils.cas_validator`, `utils.hazard_for_p2oasys`, `utils.p2oasys_scorer`, `utils.p2oasys_aggregate`, `utils.lookup_tables`, `utils.iarc_lookup`, `utils.atmo_gwp`
   - Optional: `utils.hazard_scrapers` (HazardDataAggregator, scraper_results_to_extra_sources)
   - Optional: `utils.sds_regex_extractor`, `utils.carcinogenic_potency_client`

2. **Config paths**
   - `config.P2OASYS_MATRIX_PATH` (or default) exists or report "matrix missing".
   - `config.IARC_DIR` / `config.ATMO_DIR` (optional dirs) reported as present or missing.

3. **One-CAS flow**
   - Normalize CAS (e.g. `71-43-2`) with `cas_validator.normalize_cas_input`.
   - Fetch compound with `pubchem_client.get_compound_data(cas, input_type="cas")` (skip if network fails or no key).
   - Build `hazard_data = hazard_for_p2oasys.build_hazard_data(pubchem_data, ...)` with no toxval/carc/extra (minimal).
   - Load matrix and run `compute_p2oasys_scores(hazard_data, matrix)` (skip if matrix missing).
   - Aggregate with `p2oasys_aggregate.aggregate_category_scores(scores, "max")`.

4. **Lookups (when files/dirs exist)**
   - If IARC_DIR exists: `iarc_lookup.load_iarc_from_iarc_folder(IARC_DIR)`.
   - If ATMO_DIR exists: `atmo_gwp.load_ipcc_gwp_100_from_atmo(ATMO_DIR)`.
   - If ODP/GWP CSV path set: `lookup_tables.load_odp_gwp_csv(path)`.

5. **Hazard scrapers (optional)**
   - Instantiate `HazardDataAggregator(cache_dir=...)` and call `search_chemical("71-43-2", id_type="cas", sources=["ECHA"])` (or skip if requests fail).
   - Call `scraper_results_to_extra_sources(chemical_data)` and ensure result has keys `toxicities`, `ghs`.

6. **Exit code**
   - 0 if all critical checks pass; non-zero if import or config/matrix check fails. Optional: `--skip-network` to skip PubChem/scraper calls.

### 3.2 Running before push

```bash
cd quick-hazard-assessment-app
python scripts/quality_test_pre_push.py
# With network skipped (CI/local without internet):
python scripts/quality_test_pre_push.py --skip-network
```

**Note:** A missing P2OASys matrix file (`data/Hazard Matrix Group Review 9-19-23.xlsx`) does not fail the test; the script reports that the path is configured and the file can be added for the P2OASys tab.

---

## 4. Summary

| Item | Description |
|------|--------------|
| **Tab 1: CAS report** | Single report of all available info for a CAS (PubChem, DSSTox, ToxValDB, CPDB, IARC, ODP/GWP, IPCC, ECHA, Danish, VEGA, ICE, SDS), with source labels and optional full download. |
| **Tab 2: Fast P2OASys (first 6)** | First 6 categories as expandable menus (subcategories + endpoints); report table: Name, CAS, value per subcategory, source; download CSV. |
| **Quality test** | Script that checks imports, config, one-CAS flow, lookups, optional scrapers; run before `git push`. |

Next implementation steps: (1) Add `scripts/quality_test_pre_push.py` and run it; (2) Implement Tab 1 (CAS report) and Tab 2 (Fast P2OASys first 6 with expanders + report table) in `app.py`.
