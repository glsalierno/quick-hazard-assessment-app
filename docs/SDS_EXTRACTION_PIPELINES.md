# SDS CAS extraction pipelines (v1.4+)

The Streamlit app’s **SDS PDF upload** path uses these supported extractors:

| ID | Behavior |
|----|----------|
| `hybrid_md_ocr` | **Hybrid (default):** PDF → **MarkItDown** → Markdown + **regex** for checksum-valid CAS. If **zero** CAS, fall back to **OCR** (Tesseract or EasyOCR, per sidebar) + regex. |
| `markitdown_fast` | **MarkItDown + regex only:** same first stage as hybrid, **no** OCR fallback (faster when you know the PDF has extractable text). |
| `markdown_gliner_regex` | **v1.5 parse-then-extract:** MarkItDown → Markdown → **regex CAS + H-codes**, merged with optional **GLiNER2** `extract_json` (local; install `requirements-gliner2.txt`). No OCR. Sidebar checkbox / `HAZQUERY_USE_GLINER2` can force regex-only stage 2. |

Install: `pip install "markitdown[pdf]"`. Hybrid OCR fallback also needs **Poppler** (for rasterizing PDF pages) and **Tesseract** or **EasyOCR** on `PATH`, or set `HAZQUERY_POPPLER_PATH` where applicable.

**Environment / session**

- `HAZQUERY_EXTRACTION_PIPELINE` — force pipeline for the process (values above; legacy values are remapped, see below).
- `HAZQUERY_DEFAULT_SDS_PIPELINE` — default when not set in session (from `config.DEFAULT_SDS_EXTRACTION_PIPELINE`).
- `HAZQUERY_EXTRACTION_CACHE` — `use` | `force` | `clear_once` (disk cache under `cache/`).
- `HAZQUERY_OCR_ENGINE`, `HAZQUERY_TESSERACT_PSM` — used when hybrid runs OCR (and in dev utilities).

---

## Parsers removed from the product (and why)

These options existed in earlier builds or experiments. They are **no longer selectable** in the UI and **no longer used** for SDS PDF upload. Code may still exist elsewhere for research scripts; the **live app path** is MarkItDown-only as above.

### 1. Classic unified parser (`default`)

**Was:** pypdf text → SDSParserEngine (regex + section tables) → optional Docling tables → pdfplumber “robust” path → merge → checksum + PubChem gate.

**Why removed:** On real SDS PDFs this stack was **less reliable** than MarkItDown-derived text for composition/CAS discovery: duplicated logic, heavy dependency on layout heuristics, and frequent misses or noise compared to **MarkItDown + regex**, which performed **excellently** on text- and table-heavy SDS in testing.

**Legacy:** `HAZQUERY_*=default` (or old session value `default`) is **mapped to `hybrid_md_ocr`** so bookmarks and env still work.

### 2. OCR-only (Tesseract / EasyOCR)

**Was:** Rasterize full PDF → OCR entire document → regex CAS.

**Why removed:** **OCR alone was not useful** on typical SDS sets: noisy text, poor CAS recovery vs. MarkItDown on digital PDFs, and high false-positive pressure. It remains valuable **only as a second stage** after MarkItDown finds nothing → hence **Hybrid** only.

**Legacy:** `ocr_tesseract` / `ocr_easyocr` → mapped to **`hybrid_md_ocr`**.

### 3. MarkItDown + DistilBERT on cells (`markitdown_bert`)

**Was:** MarkItDown markdown, then BERT token labeling on table cells for CAS-like spans.

**Why removed:** **Extra complexity and runtime** for **marginal gain** once MarkItDown + regex was already strong; requires a trained model and GPU-friendly stack. Simpler to standardize on **regex on MarkItDown output** only.

**Legacy:** `markitdown_bert` → mapped to **`markitdown_fast`**.

### 4. Standalone Docling + DistilBERT (`docling_bert`)

**Was:** “Pure BERT” path — Docling layout + DistilBERT CAS spans, bypassing the unified regex engine.

**Why removed:** Same theme: **heavier**, model-dependent, and **outperformed in practice** by MarkItDown + regex for the SDS corpus used in development. Not maintained as a user-facing upload path.

**Legacy:** `docling_bert` → mapped to **`hybrid_md_ocr`**.

### 5. pdfplumber-only unified path (`pdfplumber_regex`)

**Was:** Unified parser with Docling disabled (pdfplumber-focused).

**Why removed:** Subset of the classic stack; **inferior** to MarkItDown for the same reasons as (1). No separate UI value.

**Legacy:** `pdfplumber_regex` → mapped to **`hybrid_md_ocr`**.

### 6. Dual parser + cross-reference

**Was:** Run two parsers, merge CAS, optional PubChem name validation.

**Why removed:** **Maintenance cost** and coupling to legacy parsers; did not justify keeping once **Hybrid** covered text PDFs and OCR fallback. PubChem validation still applies **after** extraction on the CAS list returned by the supported pipelines.

**Note:** `HAZQUERY_PURE_CAS_BERT` / `HAZQUERY_DUAL_PARSER` and the old sidebar toggles were removed; they **do not** change SDS upload behavior anymore.

---

## GLiNER2 (optional, v1.5)

- **Install:** `pip install -r requirements-gliner2.txt` (adds `gliner2`; uses existing `torch` from the main stack when present).
- **Model:** `HAZQUERY_GLINER2_MODEL` (default `fastino/gliner2-base-v1`). First run downloads weights.
- **Disable inference:** `HAZQUERY_USE_GLINER2=0` or Streamlit sidebar **uncheck** “Run GLiNER2…”.
- **Regex stage (no model):** On ``markdown_gliner_regex``, diagnostics also include **P-codes**, **``properties_regex``** (signal word, flash/boiling point, vapor pressure, LD50/LC50/EC50-style snippets — first match per field), and the existing **H-code** list.
- **GLiNER schema:** CAS / name / H-code plus optional **signal_word**, **flash_point**, **oral_ld50_rat** strings when the model is enabled.
- **UX:** After an SDS upload, open **“SDS extraction diagnostics”** to inspect regex vs GLiNER CAS counts, code lists, ``properties_regex``, GLiNER timing/errors, and a truncated raw JSON preview.

## Summary

- **Ship:** `hybrid_md_ocr` (default), `markitdown_fast`, and **`markdown_gliner_regex`** (experimental / UX monitoring).
- **Discard (UI + upload path):** classic unified, OCR-only, MarkItDown+BERT, Docling+BERT-only, pdfplumber-only mode, dual parser — for the reasons above.
- **Backward compatibility:** unknown or legacy pipeline IDs normalize to **hybrid** or **markitdown_fast** via `utils.alternative_extraction.normalize_sds_pipeline_mode`.
