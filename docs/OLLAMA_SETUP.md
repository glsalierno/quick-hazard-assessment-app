# Local LLM setup (Ollama) — Qwen & Gemma

The v1.4 app can use a **local LLM** (no API key) for SDS extraction and summarization when you run it on your own machine. The models run via [Ollama](https://ollama.com); nothing is pushed to GitHub except these instructions and app config.

---

## What gets pushed to GitHub vs what stays local

| In the repo (pushed to GitHub) | On your machine only (not in repo) |
|--------------------------------|-------------------------------------|
| App code that calls Ollama when available | Ollama installer + server |
| `config.py` / env: `OLLAMA_HOST`, `OLLAMA_MODEL` | Model files (Qwen, Gemma) — downloaded by Ollama |
| This doc + README section | Running `ollama serve` and `ollama pull` |

So: **installation of Ollama and the models is local.** Pushing to GitHub only adds the *instructions* and *configuration* so that anyone who clones the repo knows how to install and use Qwen/Gemma locally.

---

## 1. Install Ollama (one-time, local)

1. **Download and install**
   - Windows: [ollama.com/download](https://ollama.com/download) → run the installer.
   - After install, Ollama usually runs in the background and serves at `http://localhost:11434`.

2. **Confirm it’s running**
   - Open a terminal and run:
     ```bash
     ollama list
     ```
   - If you see a list (or “no models”), the server is running.

---

## 2. Pull Qwen and Gemma (local, not in Git)

In a terminal (same machine where the app will run):

```bash
# Small and fast (good for CPU-only, e.g. Core i7)
ollama pull qwen2:0.5b

# Slightly larger, often better quality (still CPU-friendly)
ollama pull gemma2:2b
```

These commands download the model files to Ollama’s local cache (e.g. under your user profile). **Do not commit that cache to Git** — it’s large and machine-specific.

---

## 3. Optional: configure the app

The app can use environment variables so you don’t hardcode values:

| Variable | Default | Purpose |
|----------|---------|---------|
| `OLLAMA_HOST` | `http://localhost:11434` | Where Ollama is running |
| `OLLAMA_MODEL` | `qwen2:0.5b` (or see `config.py`) | Which model to use for SDS/summarization |

**Windows (PowerShell, current session):**
```powershell
$env:OLLAMA_HOST = "http://localhost:11434"
$env:OLLAMA_MODEL = "qwen2:0.5b"
streamlit run app.py
```

**Windows (permanent):** Set in System Properties → Environment variables, or in your venv activation script.

**Linux/macOS:**
```bash
export OLLAMA_HOST=http://localhost:11434
export OLLAMA_MODEL=qwen2:0.5b
streamlit run app.py
```

If you don’t set these, the app uses the defaults in `config.py` (when we add them). Cloud deployments (e.g. Streamlit Community Cloud) don’t run Ollama, so the app will simply not use a local LLM there.

---

## 4. Pushing this setup to GitHub

After you add the v1.4 code and this doc:

1. **Commit and push** the repo as usual:
   ```bash
   git add docs/OLLAMA_SETUP.md config.py app.py utils/ ...
   git commit -m "v1.4: SDS PDF comparison, optional Ollama (Qwen/Gemma) setup"
   git push origin main
   ```
2. **Do not** add to Git:
   - Ollama’s data directory
   - Any `ollama pull` model files
   - `.env` if it contains machine-specific paths

Result: everyone who clones the repo gets the *instructions* and *config*; they install Ollama and run `ollama pull qwen2:0.5b` (and optionally `gemma2:2b`) on their own machine.

---

## Summary

- **Install Qwen and Gemma:** install Ollama, then run `ollama pull qwen2:0.5b` and `ollama pull gemma2:2b` locally.
- **Push to GitHub:** push the app code, `config.py`, and this doc. The installation of Ollama and the models is documented in the repo but performed locally by each user.
