# Revised Local-Only LLM CAS Extraction Prompt for Cursor

Implement a **local-only** LLM-based CAS extraction system that runs entirely offline with no API dependencies. Use this prompt when adding or refactoring SDS CAS extraction with local LLMs (Ollama, LM Studio, llama.cpp, HuggingFace).

---

## Architecture Overview

Create a modular system with multiple fallback options:

- **Rule-based first** (Section 3 table/text, pattern matching) — always works, no LLM.
- **Local LLM** for ambiguous cases (Ollama → LM Studio → llama.cpp → HuggingFace).
- **Progressive fallback**: rule-based → LLM verification → final validation.

**Core class concept:**

```python
class LocalCASExtractor:
    """Extract CAS numbers from SDS PDFs using ONLY local LLMs. Zero API calls."""
    extraction_methods = [
        method_section_3_table,   # Rule-based first
        method_section_3_text,
        method_local_llm,        # Local LLM for ambiguous cases
        method_fallback_regex
    ]
    # Lazy-load: llm, embedding_model, llm_available
    # initialize_local_llm() tries: Ollama → LM Studio → llama.cpp → HuggingFace
```

---

## 1. Ollama Integration (Primary)

- **Check**: `GET http://localhost:11434/api/tags` (timeout 2s).
- **Pull model if missing**: `POST http://localhost:11434/api/pull` with `{"name": model_name}`.
- **Query**: `POST http://localhost:11434/api/generate` with `prompt`, `stream: false`, `temperature: 0.1`, `max_tokens: 500`.
- Model names: e.g. `phi3:mini`, `qwen2:1.5b`, `qwen2:7b`.

---

## 2. LM Studio Integration (Alternative)

- **Check**: `GET http://localhost:1234/v1/models` (default port 1234).
- **Query**: `POST http://localhost:1234/v1/chat/completions` with OpenAI-compatible payload: `messages`, `temperature`, `max_tokens`.

---

## 3. llama.cpp Integration (Direct)

- Use `llama_cpp.Llama(model_path=..., n_ctx=4096, n_threads=4, n_gpu_layers=1)`.
- Look for GGUF models in `models/` (e.g. `Phi-3.1-mini-128k-instruct-Q4_K_M.gguf`).
- Optional: `@st.cache_resource` for model loading.

---

## 4. HuggingFace Transformers (Universal Fallback)

- `AutoModelForCausalLM` + `AutoTokenizer` (e.g. `microsoft/phi-2`).
- `torch.float16` if CUDA else `torch.float32`; `device_map="auto"`.
- Generate with `max_new_tokens=500`, `temperature=0.1`, strip prompt from response.

---

## 5. Recommended Models (Local CAS Extraction)

| Tier        | Model                         | Size   | Backend        |
|------------|-------------------------------|--------|----------------|
| Tiny       | qwen2:0.5b, phi-1.5           | ~0.8–1.5GB | Ollama/transformers |
| Small      | phi3:mini, qwen2:1.5b, gemma:2b | ~2–2.4GB | Ollama/llama.cpp |
| Medium     | mistral:7b, llama3:8b, qwen2:7b | ~4–5GB | Ollama/llama.cpp |

**Recommendation:** phi3:mini (2.4GB) for best balance; qwen2:1.5b for low-resource.

---

## 6. Prompt Engineering for Local LLMs

- **CAS extraction prompt**: Instruct to extract CAS numbers in format `[digits]-[2 digits]-[1 digit]`. Request **only** a JSON array, e.g. `[{"cas": "124-09-4", "chemical": "...", "section": "3"}]`. Limit text snippet (e.g. 1500 chars).
- **Validation prompt**: Input extracted JSON; ask to verify format (NN-NN-N), match names to CAS, remove invalid entries; return cleaned JSON.
- Use **low temperature** (0.1) and limited `max_tokens` (e.g. 500) for consistent extraction.

---

## 7. RAG-Enhanced Extraction

- **Chunk** PDF text (e.g. 1000 chars).
- **Score** chunks by keywords: `cas`, `cas no`, `registry number`, `einecs`, `inventory`, `component`.
- Take **top 3** relevant chunks; build context and run **one** LLM call on that context to reduce tokens and improve accuracy.

---

## 8. Setup Wizard (User-Facing)

- **Tabs**: Auto-Detect | Ollama | LM Studio | Manual Setup.
- **Auto-Detect**: Check Ollama (11434) and LM Studio (1234); show “Use Auto-Detected Service” if either is running.
- **Ollama**: Show install command; model select (e.g. phi3:mini, qwen2:1.5b); “Pull Model” button.
- **LM Studio**: Short steps (download, load model, Start Server, port 1234); “Test Connection” button.
- **Manual**: GGUF paths for llama.cpp; pip instructions for transformers.

---

## 9. Progressive Fallback Strategy

1. **Rule-based** extraction (existing regex/Section 3 logic).
2. **needs_llm_verification(results)**: true if no results, or only one result, or any low-confidence result.
3. If true and LLM available: run **extract_cas_with_llm**; merge with rule-based (prefer rule-based for high confidence); set LLM-derived entries to `confidence: medium`.
4. **Final validation**: validate CAS format and check digit; return `{ cas_numbers, details, method_used: 'hybrid_local_llm', llm_available }`.

---

## 10. Performance and UI

- **Cache** llama.cpp model with `@st.cache_resource` when using direct backend.
- **Batch**: Pre-filter relevant sections per PDF; then run LLM per context (Ollama one-by-one is fine).
- **UI**: Status card (LLM Ready / Not Configured, backend, model); “Setup LLM” if not configured; file upload; progress (rule-based → LLM verification → complete); display results and `method_used`; note “Local LLM used” vs “Rule-based only”.

---

## 11. Requirements and Docs

- **requirements_local.txt** (optional): Keep core (streamlit, pandas, PyPDF/pdfplumber, requests). Ollama needs only `requests`. Comment optional: `llama-cpp-python`, `transformers`, `torch`, `openai` (for LM Studio compatibility).
- **User documentation**: Quick start (Ollama 5-min; LM Studio steps); hardware table (1–3B: 4–8GB RAM; 7–8B: 8–16GB); model table (phi3:mini, qwen2:1.5b, mistral:7b, llama3:8b) with accuracy/speed; **Privacy** section: no API calls, no upload, no tracking, offline-capable, good for proprietary/confidential SDS.

---

## Implementation Priority

1. **Phase 1**: Rule-based extraction (already in app; ensure it always runs first).
2. **Phase 2**: Ollama integration (check + generate; reuse existing Ollama usage where present).
3. **Phase 3**: LM Studio fallback (OpenAI-compatible client to localhost:1234).
4. **Phase 4**: llama.cpp direct (optional; GGUF path config).
5. **Phase 5**: Setup wizard + in-app documentation.

---

## Advantages: Local vs Cloud

| Aspect       | Cloud API   | Local LLM   |
|-------------|-------------|-------------|
| Cost        | Per token   | Free        |
| Privacy     | Data leaves | Stays local |
| Internet    | Required    | Optional    |
| Latency     | Network     | Local       |
| Rate limits | Yes         | No          |
| Customization | Limited   | Full        |

Use this prompt when implementing or refactoring the SDS CAS extraction pipeline to keep it **local-only** and **offline-first**.
