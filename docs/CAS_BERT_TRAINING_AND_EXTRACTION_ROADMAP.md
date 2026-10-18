# CAS BERT training pipeline & extraction roadmap

## Training pipeline (this repo)

### 1. Dataset format

`scripts/train_cas_bert.py` expects **`data/train_cas.json`** (or any path) as a **JSON array** of:

```json
{"text": "Acetone 67-64-1 100 wt%", "cas_substring": "67-64-1"}
```

- **`text`**: one line or table cell (DistilBERT `max_length` 128 in training).
- **`cas_substring`**: exact CAS as it appears in `text`, or `""` for negative examples.

### 2. Build labels from `sds examples` PDFs

**A) Regex-only (no API, fast)** — finds checksum-valid CAS per line + negative lines:

```bash
cd quick-hazard-assessment-app
python scripts/build_cas_bert_labels_from_sds.py ^
  --folder "..\sds examples" ^
  --limit 40 ^
  --mode regex ^
  --out data/train_cas_from_sds.json
```

**B) LLM-assisted (higher quality)** — uses a strong model to propose `{text, cas_substring}`, then checksum-filters:

**Ollama (local, no cloud cost):**

```bash
set OLLAMA_HOST=http://localhost:11434
set OLLAMA_LABEL_MODEL=qwen2.5:7b
python scripts/build_cas_bert_labels_from_sds.py --folder "..\sds examples" --limit 25 --mode llm --backend ollama --model qwen2.5:7b
```

**OpenAI:**

```bash
set OPENAI_API_KEY=...
python scripts/build_cas_bert_labels_from_sds.py --limit 25 --mode llm --backend openai --model gpt-4o-mini
```

**Merge** with hand-written seed data:

```bash
python scripts/build_cas_bert_labels_from_sds.py --mode regex --merge-base data/train_cas.json --out data/train_cas_merged.json
```

### 3. Train DistilBERT

```bash
python scripts/train_cas_bert.py --data data/train_cas_from_sds.json --out models/cas_bert --epochs 3 --batch-size 16
```

Set `HAZQUERY_CAS_BERT_MODEL` to the output dir if not `models/cas_bert`.

### 4. Evaluate

```bash
python -u scripts/test_pure_cas_sds_folder.py --folder "..\sds examples" --limit 20 --out artifacts/pure_cas_sds_test.csv
```

---

## Options still available in the app (no BERT required)

| Path | Notes |
|------|--------|
| Single parser + strategy presets | Sidebar: `docling_pubchem`, `pdfplumber_no_gate`, `no_pubchem_gate`, etc. |
| Pure CAS BERT | Docling + DistilBERT on table cells — needs trained `models/cas_bert` |
| Dual parser | A+B merge + name validation |
| Typed CAS / name | Skip SDS; direct hazard assessment |
| PubChem gate on/off | Strict vs show all checksum-valid CAS |

---

## Appendix: Cursor prompt — cost-free extraction experiments

Use the following in a **new Cursor chat** to implement MarkItDown, OCR, caching, and comparison tests **without Azure Document Intelligence**.

*(Paste everything below the line into Cursor.)*

---

**Context:** I'm working on the `quick-hazard-assessment-app` (v1.4) and need to improve CAS extraction from SDS PDFs while avoiding Azure Document Intelligence costs. The app currently uses Docling + DistilBERT (slow, sometimes misses CAS) and has several fallback parsers.

**Your Task:** Help me implement and test alternative extraction pipelines that are cost-effective and robust. Focus on the following:

### 1. MarkItDown Integration (Primary Path)

Add MarkItDown as a new parsing strategy. Implementation requirements:

- Install `markitdown[pdf]` with optional `[xlsx]` for Excel SDS support
- Create a new parser class `MarkItDownParser` that:
  - Converts PDF to Markdown using MarkItDown's Python API
  - Extracts tables from Markdown (use regex or a lightweight table parser like `pandas.read_csv` on pipe-separated tables)
  - Applies existing `cas_bert` model to table cells OR uses regex ensemble
  - Returns list of CAS numbers with confidence scores
- Add this as a new preset option in the sidebar (e.g., "markitdown_hybrid")
- Cache MarkItDown output to disk to avoid reprocessing same PDF

### 2. Free OCR Alternatives to Azure Document Intelligence

Instead of Azure Document Intelligence, implement these free OCR options:

**Option A: Tesseract OCR (Local)**  
- Use `pytesseract` + `pdf2image` for scanned PDFs  
- Pipeline: PDF → images → Tesseract (`--psm 6` or `11`) → regex CAS → optional Camelot/Tabula on OCR text  

**Option B: MarkItDown OCR Plugin (LLM-based)**  
- `markitdown-ocr` with **Ollama** (LLaVA) or OpenAI-compatible local server  

**Option C: EasyOCR**  
- Local deep-learning OCR; slower, often better on chemical PDFs  

### 3. Testing Framework

Create `test_extraction_pipelines.py` that on each PDF runs:

1. Baseline: Docling + DistilBERT  
2. Single parser (pdfplumber) + regex  
3. MarkItDown + regex ensemble  
4. MarkItDown + BERT on cells  
5. OCR (Tesseract) + regex  
6. OCR (EasyOCR) + regex  
7. Hybrid: MarkItDown first, if 0 CAS → OCR fallback  

**Metrics:** time, raw CAS count, checksum-valid count, PubChem-verified count; output CSV + Markdown summary.

### 4. Caching

- SHA256 of first 1MB of PDF as cache key  
- `cache/{hash}/markitdown.md`, `tables.json`, `ocr_text.json`, `cas_results.json`  
- Optional TTL / version invalidation  

### 5. Success Criteria

- At least one new pipeline matches or improves CAS recall on the test set  
- Target &lt; 30 s/PDF on CPU where possible (document trade-offs)  
- No paid document APIs  

### 6. Questions to answer after implementation

1. Best on table-heavy SDS?  
2. Best on scanned SDS?  
3. Speed vs accuracy trade-offs?  
4. MarkItDown + regex vs Docling + BERT?  
5. EasyOCR vs Tesseract?  
6. Cache hit rate?  

---

*(End of appendix prompt.)*
