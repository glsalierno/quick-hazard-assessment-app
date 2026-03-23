# Hugging Face Transformers CAS Extraction Prompt for Cursor

Implement a **CAS extraction system using only Hugging Face Transformers** for local LLM inference—no external APIs (Ollama, LM Studio, etc.). Use this prompt when building or refactoring SDS CAS extraction with a pure Hugging Face stack.

---

## 1. Architecture Overview

- **Single stack**: Hugging Face only (`transformers`, `torch`, optional `bitsandbytes`).
- **Class**: `HFCASExtractor` — model, tokenizer, pipeline; `model_loaded`; `device` from `_get_optimal_device()` (cuda → mps → cpu).
- **CAS validation**: regex `\b(\d{1,7})-(\d{2})-(\d)\b`; validate format and checksum.
- **Suppress noise**: `transformers_logging.set_verbosity_error()`; `warnings.filterwarnings("ignore")`.

---

## 2. Model Loading (Progressive Options)

- **`load_model(model_id, quantization)`**
  - **Quantization**: "4bit" (CUDA): `BitsAndBytesConfig(load_in_4bit=True, bnb_4bit_quant_type="nf4", bnb_4bit_compute_dtype=torch.float16, bnb_4bit_use_double_quant=True)`. "8bit": `load_in_8bit=True`. CPU/MPS: no quantization.
  - **Tokenizer**: `AutoTokenizer.from_pretrained(model_id, trust_remote_code=True, padding_side="left")`. If no `pad_token`, set `pad_token = eos_token`.
  - **Model**: `AutoModelForCausalLM.from_pretrained(model_id, quantization_config=..., device_map="auto" for cuda else None, torch_dtype=float16 on cuda else float32, trust_remote_code=True, low_cpu_mem_usage=True)`. If not cuda, `.to(device)`.
  - **Pipeline**: `pipeline("text-generation", model=..., tokenizer=..., device=0 or -1, max_new_tokens=500, temperature=0.1, do_sample=False)`.
- **Recommended models** (return dict with name, size_gb, ram_gb, description, quantization, hf_id):
  - **low_memory**: `HuggingFaceTB/SmolLM2-1.7B-Instruct` (~1.7GB, 4GB RAM).
  - **medium_memory**: `microsoft/Phi-3-mini-4k-instruct` (~2.4GB, 6GB RAM).
  - **balanced**: `Qwen/Qwen2.5-7B-Instruct` (7GB; use 4bit on 10GB).
  - **high_accuracy**: `mistralai/Mistral-7B-Instruct-v0.3` (7GB; 4bit).
- **`recommend_model_for_hardware()`**: Use `psutil` for RAM; `torch.cuda` for GPU memory; return one of the above by tier.

---

## 3. PDF Processing

- **`extract_text_from_pdf(pdf_file)`**: Use **pdfplumber**; iterate pages; `extract_text()` and `extract_tables()`; preserve structure with `[PAGE N]`, `[TABLES]` / `[END TABLES]`; table rows as `" | ".join(cells)`. Return `(text, tables)`. Reset `pdf_file.seek(0)`.
- **`extract_section_3(text)`**: Regex patterns for Section 3 (e.g. `Section\s*3...`, `3\.\s*Composition...`, `COMPOSITION...`, `Information on Ingredients...`); return section text up to next section or end.

---

## 4. Rule-Based CAS Extraction (Primary)

- **`extract_cas_rules(text)`**
  - Run on **Section 3 only** (from `extract_section_3`).
  - Pattern 1: Component name + CAS, e.g. `([A-Za-z0-9\s\-/]+?)\s*\(?\s*CAS(?:\s*No\.?)?:?\s*(\d{1,7}-\d{2}-\d)\s*\)?` → `clean_cas`, `validate_cas`; append `{cas, component, source: 'section_3_pattern1', confidence: 'high', method: 'rule_based'}`.
  - Pattern 2: Table rows (e.g. cells split by `|`); in each cell look for `\d{1,7}-\d{2}-\d`; validate; append with `source: 'section_3_table'`.
  - Return list of result dicts.

---

## 5. LLM Verification (Secondary)

- **`verify_with_llm(text, rule_results)`**: If `not model_loaded`, return `rule_results`.
- **Context**: `extract_section_3(text)[:1000]`.
- **Prompt**: Instruct to extract CAS and chemical names (and concentration if present); return **only** a JSON array, e.g. `[{"cas": "...", "chemical": "...", "concentration": "..."}]`.
- **Inference**: `pipeline(prompt, max_new_tokens=500, temperature=0.1, do_sample=False, return_full_text=False)`; parse JSON with `re.search(r'\[.*\]', response, re.DOTALL)`.
- **Merge**: For each LLM item with valid CAS, if not already in `rule_results`, append `{cas, component: chemical, source: 'llm_verification', confidence: 'medium', method: 'llm_enhanced', concentration}`.
- On exception: log warning and return `rule_results`.

---

## 6. Prompt Engineering

- **Phi-style**: Use `<|user|>` / `<|assistant|>` and short instruction + "Return ONLY valid JSON" + truncated section (e.g. 800 chars).
- **Qwen/Mistral**: Instruction-style with "Task:", rules, example output, then section text (e.g. 1000 chars) and "Output JSON:".
- **`create_optimized_prompt(section_text)`**: If `"phi" in model_name.lower()` use Phi prompt, else use instruct prompt.

---

## 7. Complete Extraction Pipeline

- **`extract_cas_from_pdf(pdf_file, use_llm=True)`**
  1. `extract_text_from_pdf(pdf_file)` → text, tables.
  2. `extract_cas_rules(text)` → rule_results; set `results['details']`, `results['method_used'] = 'rule_based'`.
  3. If `use_llm` and `model_loaded`: `verify_with_llm(text, rule_results)`; if more results than rule_results, set `results['details']`, `results['llm_used'] = True`, `results['method_used'] = 'llm_enhanced'`.
  4. Deduplicate by CAS; set `results['cas_numbers']` and unique `results['details']`.
  5. Add `timestamp`; return results dict.

---

## 8. Caching

- **`@st.cache_resource`** for loading model (use a `_self`-prefixed argument for the extractor so Streamlit doesn’t hash it).
- **`@st.cache_data`** or session-state keyed by hash of PDF bytes to avoid re-extracting same file.

---

## 9. User Interface

- **Model Manager**
  - Show device (cuda/mps/cpu), GPU memory (if cuda), system RAM (psutil).
  - Show **recommended model** from `recommend_model_for_hardware()`; "Load Recommended" button.
  - **Manual selection**: Dropdown (SmolLM2-1.7B, Phi-3-mini, Qwen2.5-7B, Mistral-7B, Custom); if Custom, text input for HF model ID. Radio: quantization none/4bit/8bit. "Load Model" button.
  - Status: "Active Model: {model_name}", device; "Unload Model" button.
- **Main interface**
  - Sidebar: model status; "Use LLM verification" checkbox (default = model_loaded); session stats (e.g. PDFs processed).
  - File uploader for PDF; "Extract CAS Numbers" button.
  - On run: progress/spinner; then **display_results**: metrics (count, method, LLM used); list of CAS with component name and confidence (high/medium/low with color); expander per item with full JSON.
  - "Assess" options: single CAS, all components, or select one from dropdown; set session state for assessment mode and selected CAS/list.
  - If no CAS found: "Manual Entry" text input + validate + "Assess Manual CAS".

---

## 10. Helper Functions

- **`clean_cas(cas_str)`**: Strip; regex extract `\d{1,7}-\d{2}-\d`; return normalized string or None.
- **`validate_cas(cas)`**: Format `^\d{1,7}-\d{2}-\d$`; checksum: main part (digits before last hyphen), check digit = last digit; `sum(digit * position from right) % 10 == check`.

---

## 11. Main Entry Point

- **`main()`**: `st.set_page_config(title="Local SDS CAS Extractor", icon="🔬", layout="wide")`; custom CSS for main-header / sub-header; header "Local Chemical Intelligence Platform" and "Powered by Hugging Face Transformers - 100% Local Processing".
- Init `st.session_state.extractor = HFCASExtractor()` if not set; show model manager on first run; call `render_main_interface()`; footer with privacy note and session time.

---

## 12. Requirements

Create **`requirements_hf.txt`** (or append to existing):

```txt
streamlit>=1.28.0
pandas>=2.0.0
pdfplumber>=0.10.0
PyPDF2>=3.0.0
transformers>=4.35.0
torch>=2.1.0
accelerate>=0.25.0
bitsandbytes>=0.41.0
sentencepiece>=0.1.99
psutil>=5.9.0
beautifulsoup4>=4.12.0
```

---

## Model Recommendations Summary

| Hardware   | Model              | HF ID                                      | RAM   | Notes           |
|-----------|--------------------|--------------------------------------------|-------|-----------------|
| 4–6GB RAM | SmolLM2-1.7B       | HuggingFaceTB/SmolLM2-1.7B-Instruct       | 1.7GB | Good for CAS    |
| 6–8GB RAM | Phi-3-mini-4k      | microsoft/Phi-3-mini-4k-instruct          | 2.4GB | Great balance   |
| 8–12GB    | Qwen2.5-7B (4bit)  | Qwen/Qwen2.5-7B-Instruct                  | ~3GB  | Best accuracy   |
| 12GB+     | Mistral-7B (4bit)  | mistralai/Mistral-7B-Instruct-v0.3        | ~4GB  | Very accurate   |

---

## First-Time Setup

```bash
pip install -r requirements_hf.txt
streamlit run app.py
# Open Model Manager → Load recommended model → Upload SDS PDF → Extract CAS
```

**Design principle**: 100% Hugging Face—no Ollama/LM Studio. Rule-based runs first; LLM is optional enhancement. All processing local; no data leaves the machine.
