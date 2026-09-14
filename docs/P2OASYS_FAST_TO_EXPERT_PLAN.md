# Fast P2OASys → Expert P2OASys: First 6 Categories

**Goal:** Bring fast-p2oasys (PubChem-only today) closer to expert p2oasys for the **first 6 of 8** P2OASys categories by focusing on subcategories with the **most available information**—either from uploadable databases or from SDS text via a low-dimensional LLM.

---

## 1. The 8 categories (TURI matrix)

| # | Category | In “first 6”? | Current fast-p2oasys data source |
|---|----------|----------------|----------------------------------|
| 1 | **Acute Human Effects** | ✓ | PubChem (GHS H, LD50/LC50 from toxicities) |
| 2 | **Chronic Human Effects** | ✓ | PubChem (IARC, EPA carcinogen from toxicities) |
| 3 | **Ecological Hazards** | ✓ | PubChem (aquatic LC50/EC50 from toxicities) |
| 4 | **Environmental Fate & Transport** | ✓ | Little/none in current code |
| 5 | **Atmospheric Hazard** | ✓ | Little/none in current code |
| 6 | **Physical Properties** | ✓ | PubChem (flash point, vapor pressure, NFPA) |
| 7 | Process Factors | No | N/A |
| 8 | Life Cycle Factors | No | N/A |

**Expert** p2oasys = assessor uses all available sources (SDS, ToxValDB, ECOTOX, CPDB, internal DBs, literature). **Fast** = single API (PubChem) + optional ToxRefDB for LD50; no SDS, no uploaded DBs, no LLM.

---

## 2. First 6 categories: subcategories and data availability

Inferred from `p2oasys_scorer.py` and matrix structure (Category → Subcategory → Feature/unit).

### 2.1 Acute Human Effects

| Subcategory | Feature / unit | Current source | High availability? | Notes |
|-------------|----------------|----------------|---------------------|--------|
| Inhalation Toxicity | LC50 (ppm / mg/m³), GHS H phrases | PubChem | **Yes** | SDS Section 11; ECOTOX/tox DBs |
| Oral Toxicity | LD50 (mg/kg), GHS H phrases | PubChem, ToxRefDB fallback | **Yes** | SDS Section 11; ToxValDB, CPDB |
| Dermal Toxicity | LD50 (mg/kg), GHS H phrases | PubChem | **Yes** | SDS Section 11; ToxValDB |

**Most popular / easiest to fill:** GHS H codes (Section 2/3 SDS + PubChem), then LD50/LC50 from Section 11 or from ToxValDB/ECOTOX/CPDB (already in app).

### 2.2 Chronic Human Effects

| Subcategory | Feature / unit | Current source | High availability? | Notes |
|-------------|----------------|----------------|---------------------|--------|
| Carcinogenicity | IARC, EPA Group, key phrases | PubChem | **Yes** | SDS Section 11; often “IARC 2B”, etc. |
| Other chronic | EPA, ACGIH, OSHA, Prop 65, key phrases | PubChem | **Moderate** | SDS Section 11; some DBs |

**Most popular:** IARC and EPA carcinogen class—frequently stated in SDS and in regulatory lists (uploadable or API).

### 2.3 Ecological Hazards

| Subcategory | Feature / unit | Current source | High availability? | Notes |
|-------------|----------------|----------------|---------------------|--------|
| Aquatic toxicity | LC50/EC50 (mg/L), GHS H (Aquatic) | PubChem | **Yes** | SDS Section 12; ECOTOX, ToxValDB |

**Most popular:** Acute aquatic LC50/EC50—standard in SDS and in ECOTOX (already used in app for some flows).

### 2.4 Environmental Fate & Transport

| Subcategory | Feature / unit | Current source | High availability? | Notes |
|-------------|----------------|----------------|---------------------|--------|
| (Matrix-defined) | Persistence, bioaccumulation, etc. | — | **Moderate** | SDS Section 9/12; some DBs (e.g. CompTox) |

**Most popular:** Persistence/biodegradation and bioaccumulation when present in SDS Section 9/12; fewer structured DBs than toxicity.

### 2.5 Atmospheric Hazard

| Subcategory | Feature / unit | Current source | High availability? | Notes |
|-------------|----------------|----------------|---------------------|--------|
| (Matrix-defined) | ODP, GWP, etc. | — | **Moderate** | SDS Section 9; specialized DBs |

**Most popular:** ODP/GWP when reported (often for refrigerants); can be in SDS or in a small lookup table.

### 2.6 Physical Properties

| Subcategory | Feature / unit | Current source | High availability? | Notes |
|-------------|----------------|----------------|---------------------|--------|
| Flammability | Flash point (°C), NFPA Fire, GHS | PubChem | **Yes** | SDS Section 9 |
| Other physical | Vapor pressure (mmHg), NFPA Health | PubChem | **Yes** | SDS Section 9 |

**Most popular:** Flash point and vapor pressure—very common in SDS Section 9 and in PubChem.

---

## 3. Subcategories that are “most popular” (prioritized for fast→expert)

By “most popular” we mean: **more often present in SDS and/or in databases the app already uses or could add.**

**Tier 1 (highest impact, most data):**

1. **GHS H/P codes** (Acute + Ecological) — SDS Section 2/3, PubChem; already extracted by v1.4 SDS regex.
2. **Flash point, vapor pressure** (Physical) — SDS Section 9, PubChem; v1.4 regex can extract.
3. **Acute oral/inhalation/dermal LD50/LC50** (Acute) — SDS Section 11, PubChem, ToxValDB, CPDB, ECOTOX.
4. **Aquatic LC50/EC50** (Ecological) — SDS Section 12, ECOTOX, ToxValDB; v1.4 regex extracts.
5. **IARC / EPA carcinogen** (Chronic) — SDS Section 11, PubChem; often short phrases.

**Tier 2 (good payoff, moderate availability):**

6. **NFPA / HMIS** (Physical) — Sometimes in SDS or datasheets; could be parsed or looked up.
7. **Persistence / biodegradation** (Environmental Fate) — SDS Section 9/12; key phrases.
8. **ODP / GWP** (Atmospheric) — SDS Section 9 or small lookup table for common compounds.

---

## 4. How to get closer to expert: two levers

### 4.1 Uploadable or linkable databases

- **ToxValDB, CPDB, ECOTOX:** Already in app; ensure their endpoints are **mapped into `hazard_data`** for P2OASys (LD50, LC50, aquatic, chronic) so `compute_p2oasys_scores` can use them, not only PubChem.
- **Optional upload:** Allow a **user CSV/Excel** (e.g. columns: CAS, endpoint, value, unit, route) that is merged into `toxicities` / `hazard_metrics` before scoring—covers “internal” or third-party data an expert would use.
- **Small lookup tables:** e.g. IARC by CAS, NFPA by CAS, ODP/GWP for common refrigerants—could be uploaded once and used when PubChem/SDS lack data.

### 4.2 Low-dimensional LLM to parse SDS

- **Goal:** From SDS text (or Section 2/3/9/11/12 blocks), extract a **small, fixed set of fields** that map into the Tier 1–2 subcategories (GHS, flash point, vapor pressure, LD50/LC50, aquatic LC50/EC50, IARC/EPA, NFPA, persistence phrases, ODP/GWP).
- **Why “low-dimensional”:** Output schema is a short list of named fields (not free text). Fits a small local model (e.g. Qwen/Gemma via Ollama) with a **structured prompt** or **langextract-style** schema so the model returns JSON with keys like `flash_point_c`, `vapor_pressure_mmhg`, `ghs_h_codes`, `ld50_oral_mg_kg`, `lc50_aquatic_mg_l`, `iarc`, `epa_carcinogen`, etc.
- **Flow:** SDS PDF → text (existing) → optional LLM extraction → merge into `hazard_data` (same structure as PubChem/ToxValDB) → `compute_p2oasys_scores`. Regex remains primary; LLM fills gaps when available and when Ollama is running.

---

## 5. Recommended next steps (v1.4)

1. **Merge existing app DBs into P2OASys**  
   Ensure ToxValDB, CPDB, and ECOTOX results (when already fetched in the app) are passed into `hazard_for_p2oasys` / `hazard_data` so the scorer uses them for the first 6 categories (e.g. LD50, LC50, aquatic, chronic). This gets fast-p2oasys closer to expert without new data entry.

2. **Use SDS-extracted data in P2OASys when CAS is from SDS**  
   When the user runs “SDS PDF comparison” and selects a CAS (or the app has SDS-extracted GHS, flash point, vapor pressure, aquatic LC50), **merge** those into `hazard_data` for that compound so the P2OASys tab can use them (and optionally show “Source: SDS” for those rows).

3. **Define a minimal “SDS → hazard_data” schema**  
   One JSON schema (or TypedDict) for the fields we care about for the first 6 categories (Tier 1 + selected Tier 2). Use it for: (a) regex output normalization, (b) future LLM extraction output validation and merge.

4. **Optional: LLM extraction from SDS text**  
   When Ollama is available, run a small prompt (or langextract) on SDS text (or Section 3/9/11/12) to fill the minimal schema; merge into `hazard_data` and score. Prefer models that are good at following format (e.g. Qwen with JSON instruction).

5. **Optional: User-uploaded hazard table**  
   Allow upload of a CSV (CAS, endpoint, value, unit) that is merged into `toxicities` / `hazard_metrics` for P2OASys only (or for the whole session) so experts can paste in data from other sources.

---

## 6. Summary

| Lever | Purpose |
|-------|--------|
| **Use ToxValDB/CPDB/ECOTOX in scorer** | Already-have data flows into first 6 categories; no new UI. |
| **Merge SDS regex output into hazard_data** | GHS, flash point, vapor pressure, aquatic LC50 from SDS improve Physical, Acute, Ecological. |
| **Minimal SDS schema** | Same shape for regex and LLM; clear contract for “SDS → P2OASys”. |
| **Ollama + structured extract** | Low-dim LLM parses SDS text for Tier 1–2 fields when local model is available. |
| **Upload CSV/Excel** | Expert-style “paste or upload” table for missing endpoints. |

Focusing on the **most popular subcategories** (GHS, flash point, vapor pressure, LD50/LC50, aquatic LC50, IARC/EPA) gives the largest gain for the first 6 categories with either **databases we can wire in or upload** or a **low-dimensional LLM** that parses SDS into that same set of fields.

---

## 7. Implementation status (v1.4)

- **ToxValDB + CPDB in P2OASys:** `utils/hazard_for_p2oasys.py` — `build_hazard_data(pubchem_data, toxval_data=..., carc_potency_data=..., extra_sources=...)` merges PubChem, ToxValDB toxicities, and CPDB TD50 into `hazard_data`. App uses it in the P2OASys tab.
- **IARC / ODP-GWP lookup tables:** `docs/P2OASYS_LOOKUP_SOURCES.md` (sources); `utils/lookup_tables.py` — `load_iarc_csv`, `load_odp_gwp_csv`, `get_lookup_extra_sources(cas, ...)`. Optional CSVs: `data/iarc_by_cas.csv`, `data/odp_gwp_by_cas.csv` (config: `P2OASYS_IARC_CSV_PATH`, `P2OASYS_ODP_GWP_CSV_PATH`). App merges lookup into `extra_sources` when files exist.
- **Low-dimensional LLM on SDS:** `utils/sds_llm_extractor.py` — schema `SDSHazardSchema`; `extract_hazard_from_sds_with_llm(text, host, model)` calls Ollama and returns normalized JSON; `sds_hazard_to_extra_sources(sds_hazard)` converts to `extra_sources` for `build_hazard_data(..., extra_sources=...)`. Use when Ollama is available (e.g. in SDS flow: after regex, optionally run LLM and merge). `is_ollama_available(host)` for availability check.
