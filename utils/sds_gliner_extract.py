"""
Optional **GLiNER2** structured extraction on MarkItDown markdown (v1.5 experimental).

Install: ``pip install gliner2`` (see ``requirements-gliner2.txt``). If the package or model
is missing, pipelines fall back to **regex-only** CAS / H-code extraction on the same markdown.

Env:

- ``HAZQUERY_USE_GLINER2`` — ``0`` / ``false`` disables the model even when installed (regex only).
- ``HAZQUERY_GLINER2_MODEL`` — Hugging Face model id (default ``fastino/gliner2-base-v1``).
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

from utils import cas_validator

logger = logging.getLogger(__name__)

H_CODE_RE = re.compile(r"\bH\d{3}(?:\([^)]+\))?\b", re.I)

# Schema aligned with GLiNER2 ``extract_json`` examples (fastino/gliner2-base-v1).
SDS_GLINER_SCHEMA: dict[str, Any] = {
    "chemicals": [
        {
            "cas_number": "str::Chemical Abstracts Service (CAS) Registry Number like 50-00-0",
            "chemical_name": "str::Ingredient or substance name if stated",
            "ghs_h_code": "str::GHS hazard statement code such as H301 or H314",
        }
    ]
}


def extract_h_codes_regex(text: str) -> list[str]:
    """Deterministic GHS H-code scan (whole-word H + three digits, optional parenthetical)."""
    if not text or not str(text).strip():
        return []
    found = H_CODE_RE.findall(text)
    out: list[str] = []
    seen: set[str] = set()
    for raw in found:
        u = raw.upper()
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def gliner2_is_installed() -> bool:
    try:
        import gliner2  # noqa: F401

        return True
    except ImportError:
        return False


def gliner2_runtime_enabled() -> bool:
    """When False, skip model inference (regex-only stage-2)."""
    if os.getenv("HAZQUERY_USE_GLINER2", "1").strip().lower() in ("0", "false", "no", "off"):
        return False
    try:
        import streamlit as st

        if st.session_state.get("sds_run_gliner2") is False:
            return False
    except Exception:
        pass
    return True


def _canonical_cas_str(raw: str | None) -> str | None:
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None
    relaxed, _ = cas_validator.validate_cas_relaxed(s)
    if not relaxed:
        return None
    ok, canonical = cas_validator.validate_cas(relaxed)
    return canonical if ok else None


def _cas_from_gliner_obj(obj: Any) -> list[str]:
    if not isinstance(obj, dict):
        return []
    keys = ("cas_number", "CAS", "cas", "registry_number", "cas_no", "cas_rn")
    out: list[str] = []
    for k in keys:
        v = obj.get(k)
        if v is None:
            continue
        if isinstance(v, (list, tuple)):
            for item in v:
                c = _canonical_cas_str(str(item).strip()) if item is not None else None
                if c:
                    out.append(c)
        else:
            c = _canonical_cas_str(str(v).strip())
            if c:
                out.append(c)
    return out


def _flatten_gliner_cas(data: Any) -> list[str]:
    """Walk common ``extract_json`` shapes for CAS strings."""
    found: list[str] = []
    if data is None:
        return found
    if isinstance(data, dict):
        if "chemicals" in data and isinstance(data["chemicals"], list):
            for chem in data["chemicals"]:
                found.extend(_cas_from_gliner_obj(chem))
        else:
            found.extend(_cas_from_gliner_obj(data))
    elif isinstance(data, list):
        for item in data:
            found.extend(_flatten_gliner_cas(item))
    return found


_gliner_model: Any = None
_gliner_model_id: str | None = None


def reset_gliner_model_cache() -> None:
    """Test hook / memory relief — clears lazy-loaded model."""
    global _gliner_model, _gliner_model_id
    _gliner_model = None
    _gliner_model_id = None


def _get_gliner_model() -> Any:
    global _gliner_model, _gliner_model_id
    mid = (os.getenv("HAZQUERY_GLINER2_MODEL") or "fastino/gliner2-base-v1").strip()
    if _gliner_model is not None and _gliner_model_id == mid:
        return _gliner_model
    from gliner2 import GLiNER2

    logger.info("Loading GLiNER2 model %s (first call may download weights)", mid)
    _gliner_model = GLiNER2.from_pretrained(mid)
    _gliner_model_id = mid
    return _gliner_model


def extract_sds_fields_gliner2(markdown: str) -> dict[str, Any]:
    """
    Run GLiNER2 ``extract_json`` on markdown. Returns dict with
    ``cas_numbers``, ``raw``, ``error`` (optional), ``wall_time_sec``.
    """
    import time

    t0 = time.perf_counter()
    out: dict[str, Any] = {
        "cas_numbers": [],
        "raw": None,
        "error": None,
        "wall_time_sec": 0.0,
    }
    text = (markdown or "").strip()
    if len(text) < 40:
        out["error"] = "markdown_too_short"
        out["wall_time_sec"] = time.perf_counter() - t0
        return out
    # Cap input length for CPU / memory (SDS markdown is usually < 200k; keep head + tail heuristic)
    max_chars = int(os.getenv("HAZQUERY_GLINER2_MAX_CHARS", "120000") or "120000")
    if len(text) > max_chars:
        head = max_chars * 3 // 4
        tail = max_chars - head
        text = text[:head] + "\n\n[... truncated ...]\n\n" + text[-tail:]

    try:
        model = _get_gliner_model()
        if not hasattr(model, "extract_json"):
            out["error"] = "extract_json_not_available"
            out["wall_time_sec"] = time.perf_counter() - t0
            return out
        raw = model.extract_json(text, SDS_GLINER_SCHEMA)
        out["raw"] = raw
        cas_seen: set[str] = set()
        cas_list: list[str] = []
        for c in _flatten_gliner_cas(raw):
            if c not in cas_seen:
                cas_seen.add(c)
                cas_list.append(c)
        out["cas_numbers"] = cas_list
    except Exception as exc:
        logger.warning("GLiNER2 extraction failed: %s", exc)
        out["error"] = str(exc)
    out["wall_time_sec"] = time.perf_counter() - t0
    return out
