# Cursor prompt: Explore v1.4 with embedded low-dimensional LLM (no API key)

**Use this document to start a new Cursor conversation** to design and implement v1.4 of the Quick Hazard Assessment app, adding a **low-dimensional, no-API, embedded LLM** for better UX (explain endpoints, summarize text/CPDB/SDS).

---

## Context

- **Repo:** `glsalierno/quick-hazard-assessment-app` (branch `v1.3` has the current work).
- **App:** Streamlit; runs on Streamlit Cloud (`quick-hazard-assessment-v1-3.streamlit.app`) and locally.
- **v1.3 already has:** CPDB (Carcinogenic Potency Database) SQLite + client + UI with decoded labels; rule-based CPDB summary; optional **OpenAI gpt-4o-mini** summarization when `OPENAI_API_KEY` is set (ecotoxicity excerpts, CPDB one-sentence summary).
- **Goal for v1.4:** Add a **local, embedded, small LLM** that works **without any API key** (e.g. when the user runs the app locally with Ollama or a Python-bound small model), to:
  - Explain toxicity/CPDB endpoints in plain language.
  - Summarize text-only excerpts and CPDB results.
  - Optionally support **SDS (Safety Data Sheet)**: user pastes SDS text or we link SDS by CAS; use the embedded LLM to summarize or explain sections.

**Constraint:** On Streamlit Cloud there is no GPU and limited RAM, so an in-process LLM is not feasible there. The embedded path is for **local runs only**. The app should detect “local LLM available” (e.g. Ollama) and use it when present; otherwise keep using the optional OpenAI path when the key is set.

---

## What to explore and compare

1. **Ollama**
   - User installs Ollama, runs e.g. `ollama pull smollm:1.7b` or `gemma2:2b`. App calls `http://localhost:11434` (or configurable `OLLAMA_HOST`).
   - Pros: No API key, good model choice, easy for users. Cons: Requires Ollama installed and running.

2. **Qwen (or similar) in Python**
   - e.g. **Qwen2-0.5B** or **SmolLM** via **transformers** + **llama-cpp-python** or **ollama Python client**.
   - Compare: **ollama Python client** (`pip install ollama`) vs **raw HTTP** to Ollama vs **llama-cpp-python** loading a quantized GGUF (no Ollama server).
   - Clarify: “Qwen” here means either (a) using Qwen as the model **via Ollama**, or (b) loading Qwen/other small model in-process with Python (heavier, but no separate server).

3. **Recommendation**
   - Propose one “v1.4 default” path: e.g. **Ollama** when `OLLAMA_HOST` is set (or detected), with a single small model id (e.g. `smollm:1.7b` or `gemma2:2b`) for summarization and explanation.
   - Document in README or in-app: “For local, no-API summarization and SDS explanation, install Ollama and set OLLAMA_HOST (optional).”

---

## Tasks for the new Cursor session

1. **Explore** the codebase (especially `utils/summary_utils.py`, `app.py` CPDB and ecotoxicity sections) to see where summarization and “explain endpoint” are used.
2. **Design** a small module (e.g. `utils/local_llm.py` or `utils/ollama_client.py`) that:
   - Tries to use **Ollama** (or a chosen in-process option) when configured.
   - Exposes something like `summarize(text, max_tokens=200)` and optionally `explain_endpoint(endpoint_name_or_phrase)`.
   - Fails gracefully when Ollama is not available (no crash; app continues without local LLM).
3. **Compare** in a short design note or code comments:
   - **Ollama** (server) vs **ollama Python package** (client to that server) vs **llama-cpp-python** (load model in-process, no server). Trade-offs: setup, RAM, latency, portability.
4. **Integrate** with the app:
   - When a local LLM is available, show “Summarize with local AI” (or similar) and use it instead of (or in addition to) the existing “Summarize with AI” (OpenAI) when no API key is set.
   - Keep existing OpenAI path when `OPENAI_API_KEY` is set.
5. **Optional:** Add a minimal **SDS** section: input for “Paste SDS text” and a button “Summarize / explain with local AI” that calls the embedded LLM. SDS lookup by CAS (links to PubChem/EPA/ECHA) can be a separate small task.

---

## Repo layout (relevant to v1.4)

- `app.py` – main Streamlit app; CPDB block, ecotoxicity text, summary_utils usage.
- `utils/summary_utils.py` – `summarize_cpdb_experiments`, `format_cpdb_summary`, `summarize_text_with_llm`, `summarize_cpdb_with_llm` (OpenAI).
- `utils/carcinogenic_potency_client.py` – CPDB SQLite client (optional import in app).
- `config.py` – REPO_ROOT, DATA_DIR, etc.
- `requirements.txt` – currently includes `openai>=1.0.0` for optional cloud summarization.

---

## Success criteria

- A new Cursor can read this file and the repo and implement v1.4 with:
  - A clear **Ollama vs Qwen vs python-ollama** comparison and a chosen default.
  - A **local, no-API, embedded LLM** path (Ollama or in-process) that improves UX for endpoint explanation and summarization when running locally.
  - No breaking changes to the existing v1.3 Cloud deployment (embedded path is local-only and optional).
