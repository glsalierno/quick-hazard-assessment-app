# Professional Hazard Report Redesign Prompts for Cursor

Prompts to fix the hazard report: organized tabs by source, robust character encoding and data cleaning, professional tabbed display, headers/navigation, and main application flow. Use these when working on the hazard assessment result display.

---

## Prompt 1: Complete Hazard Report Restructuring with Source Tabs

- **Tab-Based Source Organization**: Tabs: Summary Dashboard, ECHA (Regulatory), Danish QSAR, VEGA QSAR, NIH ICE, EPA CompTox, NFPA, All Sources Raw.
- **Summary Dashboard**: Chemical identity card (CAS, Preferred Name, Formula, MW, DTXSID); key hazard indicators with source counts; quick hazard summary cards (Flammability, Acute Toxicity, Aquatic Hazard) with source attribution.
- **ECHA Tab**: Harmonized classifications (CLP) and Notified classifications in clean tables with Hazard Class, Category, H-Codes, P-Codes, Source, Confidence.
- **Danish QSAR Tab**: Model predictions (Consensus, Derek, CAESAR, etc.) in structured format with Applicability and Confidence.
- **VEGA Tab**: Model predictions with applicability domain.
- **NIH ICE Tab**: In vivo toxicity table (Species, Route, Endpoint, Value, Unit, Guideline); In vitro screening (ToxCast/Tox21) table.
- **EPA / NFPA / All Data**: Source-specific tables and full dataset with filters.

---

## Prompt 2: Robust Character Encoding & Data Cleaning

- **Character Encoding Handler**: `clean_text(text)` using `unicodedata.normalize('NFKD')`, replace problematic sequences (replacement char, curly quotes, en/em dash, stray Â), remove control characters, collapse whitespace.
- **DataFrame cleaning**: `clean_dataframe(df)` apply `clean_text` to all string columns.
- **Deduplication**: `deduplicate_hazard_data(df)` with configurable unique keys (CAS, Source, Endpoint, Value, Species, Route); keep first, log removed count.
- **Value parsing**: `parse_numeric_value(text)` extract number and unit (mg/kg, mg/L, ppm, °C, mmHg, %).
- **Source-specific parsers**: `parse_echa_data`, `parse_danish_qsar_data` to clean and structure per-source data.

---

## Prompt 3: Professional Tabbed Display Implementation

- **Main entry**: `display_hazard_report(df, chemical_name, cas)` — clean data, build source_dfs (ECHA, Danish_QSAR, VEGA, ICE, EPA, NFPA, Other), create tabs.
- **Summary tab**: Chemical identity metrics (CAS, Name, Formula, MW); Key Hazard Findings (GHS table, Physical properties table); Data Coverage by Source table.
- **Per-source tabs**: `display_echa_tab`, `display_danish_qsar_tab`, `display_vega_tab`, `display_ice_tab`, `display_epa_tab`, `display_nfpa_tab` with structured tables and fallback "No data" message.
- **All Data tab**: Filters (Source, Endpoint multiselect), filtered dataframe, Download CSV button.
- **Helpers**: `extract_formula`, `extract_molecular_weight`, `extract_ghs_summary`, `extract_property_summary` from source_dfs.

---

## Prompt 4: Professional Headers and Navigation

- **Page config**: Wide layout, expanded sidebar, professional title/icon.
- **Custom CSS**: Main header, subheader, section-header, metric-card, tab styling, dataframe container, source badges (ECHA, Danish, VEGA, ICE, EPA, NFPA colors).
- **Sidebar**: Navigation (Chemical Search, SDS Upload, Batch Processing, Data Sources, Documentation), Quick Stats, Support links.

---

## Prompt 5: Main Application Flow

- **Chemical Search**: Main header, search input (CAS/name/SMILES), search type selector; on query run retrieval and call `display_hazard_report`.
- **SDS Upload**: Subheader and upload section.
- **Batch Processing**: CSV upload or paste CAS list.
- **Data Sources**: Two-column source descriptions (ECHA, Danish QSAR, VEGA, NIH ICE, EPA CompTox, NFPA) with type, coverage, update, confidence.
- **Documentation**: Getting Started, Confidence Scores, Data Attribution expanders.

---

## How to Use

- Feed prompts 1–3 when refactoring the hazard result display (tabs, cleaning, summary).
- Use prompt 2 for any new ingestion or display path that shows hazard text/tables.
- Use prompts 4–5 when aligning app-wide UI (headers, sidebar, pages) with the hazard intelligence platform vision.
- Keep existing app entry point (e.g. CAS/name input and “Assess”) and data retrieval; map current result structure to the source_dfs and display functions above.
