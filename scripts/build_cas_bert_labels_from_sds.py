#!/usr/bin/env python3
"""
Build train_cas.json for DistilBERT CAS token classifier from SDS PDFs.

Two modes:
  1) --mode regex     Fast, no API: find checksum-valid CAS in each line; label spans.
  2) --mode llm       Use a high-end LLM (OpenAI or Ollama) to propose {text, cas_substring}
                      from PDF text, then filter by checksum.

Expected by scripts/train_cas_bert.py: JSON list of {"text": str, "cas_substring": str}.
Empty cas_substring = negative example (non-CAS tokens).

Examples:
  # Regex-only baseline (no API keys)
  python scripts/build_cas_bert_labels_from_sds.py --folder "../sds examples" --limit 30 --mode regex

  # Ollama (local LLM, e.g. llama3.1 or qwen2.5)
  set OLLAMA_HOST=http://localhost:11434
  set OLLAMA_LABEL_MODEL=qwen2.5:7b
  python scripts/build_cas_bert_labels_from_sds.py --folder "../sds examples" --limit 20 --mode llm --backend ollama

  # OpenAI (paid)
  set OPENAI_API_KEY=...
  python scripts/build_cas_bert_labels_from_sds.py --mode llm --backend openai --model gpt-4o-mini
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Optional

# Repo root on path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _extract_pdf_text(pdf_path: Path) -> str:
    from utils import sds_pdf_utils

    data = pdf_path.read_bytes()
    t = sds_pdf_utils.extract_text_from_pdf_bytes(data)
    return sds_pdf_utils.normalize_whitespace(t) or ""


def _cas_pattern_line(line: str) -> list[str]:
    """Find CAS-like tokens in a line (regex)."""
    return re.findall(r"\b(\d{1,7}-\d{2}-\d)\b", line)


def _checksum_ok(cas: str) -> bool:
    from utils import cas_validator

    norm = cas_validator.normalize_cas_input(cas) or cas
    ok, _ = cas_validator.validate_cas_relaxed(norm)
    return bool(ok)


def labels_regex_from_text(text: str, max_negatives: int = 200) -> list[dict[str, str]]:
    """
    Build training rows: positive lines containing valid CAS; negative lines without.
    """
    from utils import cas_validator

    positives: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for line in text.splitlines():
        line = line.strip()
        if len(line) < 3 or len(line) > 500:
            continue
        for raw in _cas_pattern_line(line):
            norm = cas_validator.normalize_cas_input(raw) or raw
            if not _checksum_ok(norm):
                continue
            # cas_substring as it appears in line (prefer raw if present else norm)
            sub = raw if raw in line else norm
            key = (line[:300], sub)
            if key in seen:
                continue
            seen.add(key)
            positives.append({"text": line[:512], "cas_substring": sub})

    negatives: list[dict[str, str]] = []
    for line in text.splitlines():
        line = line.strip()
        if len(line) < 15 or len(line) > 400:
            continue
        if re.search(r"\d{1,7}-\d{2}-\d", line):
            continue
        if not re.search(r"[A-Za-z]{3,}", line):
            continue
        negatives.append({"text": line[:512], "cas_substring": ""})
        if len(negatives) >= max_negatives:
            break

    return positives + negatives


def _llm_ollama(messages: list[dict[str, str]], model: str) -> str:
    import urllib.request

    host = (os.environ.get("OLLAMA_HOST") or "http://localhost:11434").rstrip("/")
    url = f"{host}/api/chat"
    body = json.dumps(
        {"model": model, "messages": messages, "stream": False, "options": {"temperature": 0.1}}
    ).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=600) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return (data.get("message") or {}).get("content") or ""


def _llm_openai(messages: list[dict[str, str]], model: str) -> str:
    try:
        from openai import OpenAI
    except ImportError as e:
        raise RuntimeError("pip install openai") from e
    client = OpenAI()
    r = client.chat.completions.create(model=model, messages=messages, temperature=0.1)
    return (r.choices[0].message.content or "").strip()


def _parse_json_array(raw: str) -> list[dict[str, Any]]:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    data = json.loads(raw)
    if isinstance(data, dict) and "items" in data:
        data = data["items"]
    if not isinstance(data, list):
        return []
    return data


def labels_llm_from_text(text: str, backend: str, model: str) -> list[dict[str, str]]:
    """Ask LLM for JSON array of {text, cas_substring}."""
    excerpt = text[:14000]
    system = (
        "You extract training data for a CAS Registry token classifier.\n"
        'Return ONLY a JSON array of objects: {"text": string, "cas_substring": string}.\n'
        '"text" is ONE short line or table cell exactly as context would appear (max 400 chars).\n'
        '"cas_substring" is the exact CAS number as written (format: digits-digits-digit), or "" if none.\n'
        "Include only real CAS Registry numbers. Skip trade secrets without numbers.\n"
        "If the excerpt has no CAS, return [].\n"
        "No markdown, no commentary — JSON only."
    )
    user = f"SDS text excerpt:\n\n{excerpt}"
    messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    if backend == "ollama":
        content = _llm_ollama(messages, model=model)
    elif backend == "openai":
        content = _llm_openai(messages, model=model)
    else:
        raise ValueError(backend)

    rows = _parse_json_array(content)
    out: list[dict[str, str]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        t = str(r.get("text", "")).strip()
        cas = str(r.get("cas_substring", "")).strip()
        if len(t) < 2:
            continue
        t = t[:512]
        if cas and not _checksum_ok(cas):
            continue
        if cas and cas not in t and t.find(cas.replace(" ", "")) < 0:
            # allow minor whitespace
            pass
        out.append({"text": t, "cas_substring": cas})
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Build CAS BERT training JSON from SDS PDFs")
    ap.add_argument(
        "--folder",
        type=Path,
        default=_ROOT.parent / "sds examples",
        help="Folder containing SDS PDFs",
    )
    ap.add_argument("--limit", type=int, default=25, help="Max PDFs to process")
    ap.add_argument(
        "--out",
        type=Path,
        default=_ROOT / "data" / "train_cas_from_sds.json",
        help="Output JSON path",
    )
    ap.add_argument("--mode", choices=("regex", "llm"), default="regex", help="Labeling strategy")
    ap.add_argument("--backend", choices=("ollama", "openai"), default="ollama", help="LLM backend for --mode llm")
    ap.add_argument(
        "--model",
        type=str,
        default="",
        help="Model id (default: OLLAMA_LABEL_MODEL or qwen2.5:7b / gpt-4o-mini)",
    )
    ap.add_argument("--merge-base", type=Path, default=None, help="Optional JSON to merge (e.g. data/train_cas.json)")
    args = ap.parse_args()

    folder = args.folder.resolve()
    if not folder.is_dir():
        print("Folder not found:", folder)
        return 1

    pdfs = sorted(folder.glob("*.pdf")) + sorted(folder.glob("*.PDF"))
    if args.limit:
        pdfs = pdfs[: args.limit]
    if not pdfs:
        print("No PDFs in", folder)
        return 1

    model = args.model or (
        os.environ.get("OLLAMA_LABEL_MODEL")
        or os.environ.get("OPENAI_LABEL_MODEL")
        or ("gpt-4o-mini" if args.backend == "openai" else "qwen2.5:7b")
    )

    all_rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for i, pdf in enumerate(pdfs, 1):
        print(f"[{i}/{len(pdfs)}] {pdf.name} ...", flush=True)
        try:
            text = _extract_pdf_text(pdf)
        except Exception as e:
            print(f"  skip (read error): {e}", flush=True)
            continue
        if len(text) < 50:
            print("  skip (too little text)", flush=True)
            continue

        if args.mode == "regex":
            rows = labels_regex_from_text(text)
        else:
            try:
                rows = labels_llm_from_text(text, args.backend, model)
                time.sleep(0.5)
            except Exception as e:
                print(f"  LLM error, fallback regex: {e}", flush=True)
                rows = labels_regex_from_text(text)

        for r in rows:
            key = (r["text"][:200], r.get("cas_substring", ""))
            if key in seen:
                continue
            seen.add(key)
            all_rows.append(r)
        print(f"  +{len(rows)} rows (total unique {len(all_rows)})", flush=True)

    if args.merge_base and args.merge_base.is_file():
        with open(args.merge_base, encoding="utf-8") as f:
            base = json.load(f)
        if isinstance(base, list):
            for r in base:
                if isinstance(r, dict) and "text" in r:
                    k = (str(r["text"])[:200], str(r.get("cas_substring", "")))
                    if k not in seen:
                        seen.add(k)
                        all_rows.append(
                            {"text": str(r["text"]), "cas_substring": str(r.get("cas_substring", ""))}
                        )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(all_rows, f, indent=2, ensure_ascii=False)

    n_pos = sum(1 for r in all_rows if (r.get("cas_substring") or "").strip())
    print(f"\nWrote {len(all_rows)} rows ({n_pos} positive) -> {args.out}")
    print("Next: python scripts/train_cas_bert.py --data", args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
