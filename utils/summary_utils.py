"""
Rule-based summarization and optional mini-LLM summarization for hazard data.
- CPDB: summarize experiments by species, route, author opinion, TD50 range (no LLM).
- Text excerpts / CPDB one-liner: local Ollama (preferred) or OpenAI gpt-4o-mini when API key is set.
Low-dimensional: short prompts, limited tokens, no commercial API required when Ollama is deployed locally.
"""

from __future__ import annotations

from typing import Any

try:
    import requests
except ImportError:
    requests = None


def summarize_cpdb_experiments(experiments: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Rule-based summary of CPDB experiments (no LLM).
    Returns counts by species, route, author opinion, and TD50 range.
    """
    if not experiments:
        return {"n": 0, "by_species": {}, "by_route": {}, "by_opinion": {}, "td50_range": None}

    by_species: dict[str, int] = {}
    by_route: dict[str, int] = {}
    by_opinion: dict[str, int] = {}
    td50_values: list[float] = []

    for e in experiments:
        sp = (e.get("species_name") or e.get("species") or "—").strip() or "—"
        by_species[sp] = by_species.get(sp, 0) + 1
        rt = (e.get("route_name") or e.get("route") or "—").strip() or "—"
        by_route[rt] = by_route.get(rt, 0) + 1
        op = (e.get("opinion_label") or e.get("opinion") or "—").strip() or "—"
        # Normalize to short key for grouping
        if "Positive" in op:
            op_key = "Positive"
        elif "Negative" in op:
            op_key = "Negative"
        elif "Equivocal" in op:
            op_key = "Equivocal"
        else:
            op_key = op
        by_opinion[op_key] = by_opinion.get(op_key, 0) + 1
        try:
            td = e.get("td50")
            if td is not None and str(td).strip() and str(td).upper() not in ("1E31", "N.S.S.", ""):
                v = float(str(td).replace(",", "."))
                if v < 1e30:  # skip sentinel
                    td50_values.append(v)
        except (ValueError, TypeError):
            pass

    td50_range = None
    if td50_values:
        td50_range = (min(td50_values), max(td50_values))

    return {
        "n": len(experiments),
        "by_species": dict(sorted(by_species.items(), key=lambda x: -x[1])),
        "by_route": dict(sorted(by_route.items(), key=lambda x: -x[1])),
        "by_opinion": dict(sorted(by_opinion.items(), key=lambda x: -x[1])),
        "td50_range": td50_range,
    }


def format_cpdb_summary(summary: dict[str, Any]) -> str:
    """Turn summary dict into a short readable paragraph."""
    if summary["n"] == 0:
        return "No experiments."
    parts = [f"{summary['n']} experiment(s)"]
    if summary["by_species"]:
        sp_parts = [f"{k} ({v})" for k, v in list(summary["by_species"].items())[:5]]
        parts.append("species: " + ", ".join(sp_parts))
    if summary["by_route"]:
        rt_parts = [f"{k} ({v})" for k, v in list(summary["by_route"].items())[:5]]
        parts.append("routes: " + ", ".join(rt_parts))
    if summary["by_opinion"]:
        op_parts = [f"{k} ({v})" for k, v in summary["by_opinion"].items()]
        parts.append("author opinion: " + ", ".join(op_parts))
    if summary.get("td50_range"):
        lo, hi = summary["td50_range"]
        parts.append(f"TD50 range: {lo:.2f}–{hi:.2f} mg/kg/day")
    return ". ".join(parts) + "."


def summarize_cpdb_with_llm(summary_paragraph: str, api_key: str | None) -> str | None:
    """
    Optional: turn CPDB rule-based summary into one short sentence (e.g. for at-a-glance).
    Uses gpt-4o-mini. Returns None if no key or error.
    """
    if not api_key or not (summary_paragraph or "").strip():
        return None
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        r = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You reply with exactly one short sentence summarizing the carcinogenicity data. Mention species, positive/negative, and TD50 range if present. Be factual."},
                {"role": "user", "content": summary_paragraph},
            ],
            max_tokens=100,
        )
        if r.choices and r.choices[0].message and r.choices[0].message.content:
            return r.choices[0].message.content.strip()
    except Exception:
        pass
    return None


def summarize_text_with_llm(text: str, api_key: str | None, max_chars: int = 3000) -> str | None:
    """
    Optional: summarize long text using OpenAI gpt-4o-mini (cheap, fast).
    Returns None if no key, or on error. Requires: pip install openai
    """
    if not api_key or not (text or "").strip():
        return None
    text = (text or "").strip()
    if len(text) > max_chars:
        text = text[:max_chars] + "..."
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        r = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You summarize toxicity or hazard text in 1–3 short sentences. Be factual and keep numbers/units."},
                {"role": "user", "content": text},
            ],
            max_tokens=200,
        )
        if r.choices and r.choices[0].message and r.choices[0].message.content:
            return r.choices[0].message.content.strip()
    except Exception:
        pass
    return None


def _ollama_generate(
    prompt: str,
    host: str = "http://localhost:11434",
    model: str = "qwen2:0.5b",
    max_tokens: int = 150,
    timeout: int = 60,
) -> str | None:
    """Single prompt completion via Ollama /api/generate. Returns stripped response or None."""
    if not requests:
        return None
    url = host.rstrip("/") + "/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"num_predict": max_tokens},
    }
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        if r.status_code != 200:
            return None
        data = r.json()
        out = (data.get("response") or "").strip()
        return out if out else None
    except Exception:
        return None


def summarize_text_with_ollama(
    text: str,
    host: str = "http://localhost:11434",
    model: str = "qwen2:0.5b",
    max_chars: int = 3000,
    timeout: int = 60,
) -> str | None:
    """
    Summarize toxicity/hazard text using local Ollama. Low-dimensional: short prompt, limited tokens.
    Returns 1–3 sentences. No API key required. Returns None if Ollama unreachable or error.
    """
    if not (text or "").strip():
        return None
    text = (text or "").strip()
    if len(text) > max_chars:
        text = text[:max_chars] + "..."
    prompt = (
        "Summarize this toxicity or hazard text in 1 to 3 short sentences. Be factual and keep numbers and units. Reply with only the summary.\n\n"
        + text
    )
    return _ollama_generate(prompt, host=host, model=model, max_tokens=200, timeout=timeout)


def summarize_cpdb_with_ollama(
    summary_paragraph: str,
    host: str = "http://localhost:11434",
    model: str = "qwen2:0.5b",
    timeout: int = 60,
) -> str | None:
    """
    One-sentence summary of CPDB data using local Ollama. Low-dimensional output.
    Returns None if Ollama unreachable or error. No API key required.
    """
    if not (summary_paragraph or "").strip():
        return None
    prompt = (
        "Reply with exactly one short sentence summarizing this carcinogenicity data. Mention species, positive/negative, and TD50 range if present. Be factual. Only output the sentence.\n\n"
        + summary_paragraph.strip()
    )
    return _ollama_generate(prompt, host=host, model=model, max_tokens=120, timeout=timeout)


def is_ollama_available(host: str = "http://localhost:11434", timeout: int = 2) -> bool:
    """Check if Ollama is reachable (for local LLM summarization)."""
    if not requests:
        return False
    try:
        r = requests.get(host.rstrip("/") + "/api/tags", timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False
