# SDS CAS Extraction Strategy

## Current pipeline (single parser)

1. **pypdf** — extract embedded text from PDF
2. **SDSParserEngine** — regex + section 3 pipe/delimiter tables → CAS
3. **Docling** — IBM Docling table extraction (composition tables), merged with step 2
4. **Robust extractor** (pdfplumber) — tables + text + optional reconstructor, merged with steps 2–3
5. **Checksum filter** — drop any CAS that fails checksum (hard gate)
6. **PubChem gate** — when on, only show CAS found in PubChem

## Configurable options (env vars)

| Env var | Default | Effect |
|---------|---------|--------|
| `USE_DOCLING` | 1 | IBM Docling for table structure (slower, better tables) |
| `USE_OCR` | 0 | Tesseract OCR for scanned PDFs |
| `USE_ROBUST_CAS_EXTRACTOR` | 1 | pdfplumber + reconstructor |
| `USE_RECONSTRUCTOR_AS_FALLBACK_ONLY` | 1 | Run reconstructor **only when** table/regex find 0 CAS (avoids fake CAS from random digits) |
| `RECONSTRUCTOR_USE_CONTEXT_FILTER` | 1 | Require CAS-like context (composition, ingredient) near digit sequences |
| `RECONSTRUCTOR_MAX_GAP` | 15 | Max chars between digit groups when reconstructing |
| `USE_PUBCHEM_CAS_VALIDATION` | 1 | Check CAS in PubChem |
| `SHOW_ONLY_PUBCHEM_VERIFIED` | 1 | Only show CAS found in PubChem |
| `MIN_CAS_CONFIDENCE` | 0 | Min confidence score to show |

## Strategy presets (in-app tester)

Use the sidebar **SDS extraction strategy** expander to test without restarting:

| Preset | Use case |
|--------|----------|
| **strict** | No invalid, no made-up; PubChem-verified only |
| **max_coverage** | More CAS, may include unverified; reconstructor runs always |
| **reconstructor_first** | Reconstructor runs before tables (digits → CAS) |
| **docling_only** | Rely on Docling tables |
| **pdfplumber_only** | No Docling (faster) |
| **no_pubchem_gate** | Show all checksum-valid; use when PubChem times out |

## Alternative pipelines (sidebar toggles)

- **Dual parser** — Parser A (above) + Parser B (`extract_sds_for_llm`), merge, DSSTox cross-ref
- **Pure CAS BERT** — Docling + DistilBERT only (requires trained model)

## How to test

1. Open the app, expand **SDS extraction strategy (test combos)** in the sidebar
2. Pick a preset (e.g. `max_coverage`) or leave as `(config default)`
3. Re-upload an SDS PDF
4. Compare CAS extracted with previous run
5. Use **SDS parser debug** and **CAS debug mode** to inspect extraction stages
