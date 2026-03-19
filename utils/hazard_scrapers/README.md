# Unified hazard scrapers

Scrapers for **ECHA CHEM**, **Danish QSAR**, **VEGA QSAR**, and **NIH NICEATM ICE**. Outputs are standardized for P2OASys-style hazard scoring.

**IARC** is not scraped here; use the local IARC database (e.g. `fastP2OASys/iarc`) and `utils.iarc_lookup` / `utils.lookup_tables` for that.

## Usage

- **Aggregator**: `HazardDataAggregator.search_chemical(identifier, id_type='cas'|'smiles', sources=...)` returns `dict[source_name, list[HazardDataPoint]]`.
- **P2OASys table**: `aggregator.aggregate_for_p2oasys(chemical_data)` → DataFrame.
- **Merge into app**: `scraper_results_to_extra_sources(chemical_data)` → `extra_sources` dict for `build_hazard_data(..., extra_sources=...)` in `utils.hazard_for_p2oasys`.

## CLI

```bash
python scripts/run_unified_hazard_scraper.py --cas 71-43-2 50-00-0 --output results.csv
python scripts/run_unified_hazard_scraper.py --smiles "CCO" -o out.csv
python scripts/run_unified_hazard_scraper.py --file cas_list.txt --id-type cas -o batch.csv --sources ECHA Danish_QSAR
```

Optional env vars: `VEGA_API_KEY`, `ICE_API_KEY` (request from VEGA/ICE if needed).

## API keys

- **VEGA QSAR**: Free API key from [VEGA Hub](https://www.vegahub.eu/portfolio-item/vega-qsar-api/).
- **NIH NICEATM ICE**: Request from `ICE-support@niehs.nih.gov`.

ECHA and Danish QSAR use web scraping and do not require API keys.

## Cache

Responses are cached under `hazard_cache/` (or `--cache-dir`) for 7 days to avoid repeated requests.
