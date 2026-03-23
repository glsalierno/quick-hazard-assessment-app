"""
Shared CAS extraction from plain text: regex + checksum validation.

Used by MarkItDown, OCR, and benchmark pipelines (no Docling dependency).
"""

from __future__ import annotations

import re
from typing import Iterable

from utils import cas_validator

# CAS Registry pattern (same family as build_cas_bert_labels_from_sds)
_CAS_LINE_RE = re.compile(r"\b(\d{1,7}-\d{2}-\d)\b")


def find_checksum_valid_cas_in_text(text: str) -> list[str]:
    """
    Return unique CAS numbers in order of first appearance; only checksum-valid.
    """
    if not text or not str(text).strip():
        return []
    seen: set[str] = set()
    out: list[str] = []
    for m in _CAS_LINE_RE.finditer(text):
        raw = m.group(1)
        norm = cas_validator.normalize_cas_input(raw) or raw
        ok, _ = cas_validator.validate_cas_relaxed(norm)
        if not ok or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def count_raw_cas_matches(text: str) -> int:
    """Count regex CAS-like tokens (may include invalid checksum)."""
    if not text:
        return 0
    return len(_CAS_LINE_RE.findall(text))
