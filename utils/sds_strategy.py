"""
SDS extraction strategy: runtime overrides for testing combinations without restart.

Session-state keys in sds_strategy_override take precedence over config env vars.
Used by sds_parser, robust_cas_extractor, input_handler when present.
"""

from __future__ import annotations

import os
from contextvars import ContextVar
from typing import Any, Optional

# Temporary overrides for a single parse (e.g. pdfplumber-only preset) without mutating session.
_STRATEGY_CTX: ContextVar[Optional[dict[str, Any]]] = ContextVar("sds_strategy_ctx", default=None)


class strategy_context:
    """Use as ``with strategy_context({...}):`` so ``get()`` sees extra keys for one call stack."""

    def __init__(self, mapping: dict[str, Any]) -> None:
        self.mapping = mapping
        self._token: Any = None

    def __enter__(self) -> "strategy_context":
        cur = _STRATEGY_CTX.get()
        merged: dict[str, Any] = dict(cur) if cur else {}
        merged.update(self.mapping)
        self._token = _STRATEGY_CTX.set(merged)
        return self

    def __exit__(self, *args: Any) -> None:
        if self._token is not None:
            _STRATEGY_CTX.reset(self._token)


def _session_overrides() -> dict[str, Any]:
    """Session-state overrides for extraction (set by sidebar tester)."""
    try:
        import streamlit as st
        return st.session_state.get("sds_strategy_override") or {}
    except Exception:
        return {}


def get(key: str, default: Any = None) -> Any:
    """Effective config value: context > session override > env > config default."""
    ctx = _STRATEGY_CTX.get()
    if ctx is not None and key in ctx:
        return ctx[key]
    overrides = _session_overrides()
    if key in overrides:
        return overrides[key]

    env_map = {
        "USE_DOCLING": ("USE_DOCLING", lambda v: (v or "1").strip().lower() in ("1", "true", "yes", "on")),
        "USE_OCR": ("USE_OCR", lambda v: (v or "").strip().lower() in ("1", "true", "yes", "on")),
        "USE_ROBUST_CAS_EXTRACTOR": ("USE_ROBUST_CAS_EXTRACTOR", lambda v: (v or "1").strip().lower() in ("1", "true", "yes", "on")),
        "USE_PUBCHEM_CAS_VALIDATION": ("USE_PUBCHEM_CAS_VALIDATION", lambda v: (v or "1").strip().lower() in ("1", "true", "yes", "on")),
        "SHOW_ONLY_PUBCHEM_VERIFIED": ("SHOW_ONLY_PUBCHEM_VERIFIED", lambda v: (v or "1").strip().lower() in ("1", "true", "yes", "on")),
        "USE_RECONSTRUCTOR_AS_FALLBACK_ONLY": ("USE_RECONSTRUCTOR_AS_FALLBACK_ONLY", lambda v: (v or "1").strip().lower() in ("1", "true", "yes", "on")),
        "RECONSTRUCTOR_USE_CONTEXT_FILTER": ("RECONSTRUCTOR_USE_CONTEXT_FILTER", lambda v: (v or "1").strip().lower() in ("1", "true", "yes", "on")),
        "RECONSTRUCTOR_MAX_GAP": ("RECONSTRUCTOR_MAX_GAP", lambda v: int(v or "15")),
        "MIN_CAS_CONFIDENCE": ("MIN_CAS_CONFIDENCE", lambda v: float(v or "0.0")),
    }
    if key in env_map:
        env_name, parser = env_map[key]
        return parser(os.environ.get(env_name))

    # Fall back to config module
    try:
        import config as cfg
        return getattr(cfg, key, default)
    except ImportError:
        return default


# Presets for common strategy combos
PRESETS = {
    "strict": {
        "label": "Strict (no invalid, no made-up)",
        "USE_DOCLING": True,
        "USE_OCR": False,
        "USE_ROBUST_CAS_EXTRACTOR": True,
        "USE_PUBCHEM_CAS_VALIDATION": True,
        "SHOW_ONLY_PUBCHEM_VERIFIED": True,
        "USE_RECONSTRUCTOR_AS_FALLBACK_ONLY": True,
        "RECONSTRUCTOR_USE_CONTEXT_FILTER": True,
        "RECONSTRUCTOR_MAX_GAP": 10,
    },
    "max_coverage": {
        "label": "Max coverage (more CAS, may include unverified)",
        "USE_DOCLING": True,
        "USE_OCR": True,
        "USE_ROBUST_CAS_EXTRACTOR": True,
        "USE_PUBCHEM_CAS_VALIDATION": True,
        "SHOW_ONLY_PUBCHEM_VERIFIED": False,
        "USE_RECONSTRUCTOR_AS_FALLBACK_ONLY": False,
        "RECONSTRUCTOR_USE_CONTEXT_FILTER": False,
        "RECONSTRUCTOR_MAX_GAP": 25,
    },
    "reconstructor_first": {
        "label": "Reconstructor first (digits → CAS)",
        "USE_DOCLING": False,
        "USE_OCR": False,
        "USE_ROBUST_CAS_EXTRACTOR": True,
        "USE_PUBCHEM_CAS_VALIDATION": True,
        "SHOW_ONLY_PUBCHEM_VERIFIED": True,
        "USE_RECONSTRUCTOR_AS_FALLBACK_ONLY": False,
        "RECONSTRUCTOR_USE_CONTEXT_FILTER": False,
        "RECONSTRUCTOR_MAX_GAP": 25,
    },
    "docling_pubchem": {
        "label": "Docling + PubChem gate (recommended)",
        "USE_DOCLING": True,
        "USE_OCR": False,
        "USE_ROBUST_CAS_EXTRACTOR": True,
        "USE_PUBCHEM_CAS_VALIDATION": True,
        "SHOW_ONLY_PUBCHEM_VERIFIED": True,
        "USE_RECONSTRUCTOR_AS_FALLBACK_ONLY": True,
        "RECONSTRUCTOR_USE_CONTEXT_FILTER": True,
        "RECONSTRUCTOR_MAX_GAP": 10,
    },
    "docling_only": {
        "label": "Docling tables only",
        "USE_DOCLING": True,
        "USE_OCR": False,
        "USE_ROBUST_CAS_EXTRACTOR": True,
        "USE_PUBCHEM_CAS_VALIDATION": True,
        "SHOW_ONLY_PUBCHEM_VERIFIED": True,
        "USE_RECONSTRUCTOR_AS_FALLBACK_ONLY": True,
        "RECONSTRUCTOR_USE_CONTEXT_FILTER": True,
        "RECONSTRUCTOR_MAX_GAP": 15,
    },
    "pdfplumber_only": {
        "label": "pdfplumber only (no Docling)",
        "USE_DOCLING": False,
        "USE_OCR": False,
        "USE_ROBUST_CAS_EXTRACTOR": True,
        "USE_PUBCHEM_CAS_VALIDATION": True,
        "SHOW_ONLY_PUBCHEM_VERIFIED": True,
        "USE_RECONSTRUCTOR_AS_FALLBACK_ONLY": True,
        "RECONSTRUCTOR_USE_CONTEXT_FILTER": True,
        "RECONSTRUCTOR_MAX_GAP": 10,
    },
    "pdfplumber_no_gate": {
        "label": "pdfplumber only, no PubChem gate",
        "USE_DOCLING": False,
        "USE_OCR": False,
        "USE_ROBUST_CAS_EXTRACTOR": True,
        "USE_PUBCHEM_CAS_VALIDATION": True,
        "SHOW_ONLY_PUBCHEM_VERIFIED": False,
        "USE_RECONSTRUCTOR_AS_FALLBACK_ONLY": True,
        "RECONSTRUCTOR_USE_CONTEXT_FILTER": True,
        "RECONSTRUCTOR_MAX_GAP": 10,
    },
    "pure_bert_no_gate": {
        "label": "Pure CAS BERT (Docling+DistilBERT), no PubChem gate",
        "USE_DOCLING": True,
        "USE_OCR": False,
        "USE_ROBUST_CAS_EXTRACTOR": True,
        "USE_PUBCHEM_CAS_VALIDATION": True,
        "SHOW_ONLY_PUBCHEM_VERIFIED": False,
        "USE_RECONSTRUCTOR_AS_FALLBACK_ONLY": True,
        "RECONSTRUCTOR_USE_CONTEXT_FILTER": True,
        "RECONSTRUCTOR_MAX_GAP": 10,
    },
    "no_pubchem_gate": {
        "label": "No PubChem gate (show all checksum-valid)",
        "USE_DOCLING": True,
        "USE_OCR": False,
        "USE_ROBUST_CAS_EXTRACTOR": True,
        "USE_PUBCHEM_CAS_VALIDATION": True,
        "SHOW_ONLY_PUBCHEM_VERIFIED": False,
        "USE_RECONSTRUCTOR_AS_FALLBACK_ONLY": True,
        "RECONSTRUCTOR_USE_CONTEXT_FILTER": True,
        "RECONSTRUCTOR_MAX_GAP": 10,
    },
}
