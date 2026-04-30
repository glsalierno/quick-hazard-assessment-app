"""
Shared CAS extraction from plain text: regex + checksum validation.

SDS sources use many labels for the CAS Registry Number: ``CAS``, ``C.A.S.``,
``CAS-No.``, ``CAS #``, ``CAS Number``, ``RN``, ``Registry Number``,
``Index No.`` / ``INDEX NO.``, etc. We normalize common spellings, then scan
with a prefix-aware pattern plus a bare ``N-N-N`` pattern.

Used by MarkItDown, OCR, and benchmark pipelines (no Docling dependency).
"""

from __future__ import annotations

import re

from utils import cas_validator

# Bare CAS (hyphen-separated registry format; first segment 1–9 digits per CAS RN rules)
_CAS_LINE_RE = re.compile(r"\b([1-9]\d{0,8}-\d{2}-\d)\b")

# Optional SDS label before the registry number (capture group 1 = dashed CAS).
# VERBOSE: avoid raw ``#`` (starts a comment) — use ``[#]`` for hash.
_CAS_PREFIX_SCAN_RE = re.compile(
    r"""
    (?ix)
    (?:
        (?:
            C(?:\.|\s)?A(?:\.|\s)?S(?:\.|\s)?
                (?:-?\s*(?:No\.?|Number|Nr\.?|N°|[#]))?
          | CAS-No\.?
        )\s*[:\-]?\s*
      | \bCAS\s+Number\s*[:\-]?\s*
      | \bRN\b\s*[:\-]?\s*
      | \bRegistry\s+Number\s*[:\-]?\s*
      | \bCAS\s+Registry(?:\s+Number)?\s*[:\-]?\s*
      | \bIndex\s+No\.?\s*[:\-]?\s*
      | \bINDEX\s+NO\.?\s*[:\-]?\s*
    )?
    \b([1-9]\d{0,8}-\d{2}-\d)\b
    """,
    re.VERBOSE,
)

# Normalize common label variants so legacy ``CAS``-anchored patterns match more PDFs.
_CAS_LABEL_NORMALIZE: list[tuple[str, str]] = [
    (r"\bINDEX\s+NO\.?\b", "CAS:"),
    (r"\bIndex\s+No\.?\b", "CAS:"),
    (r"\bC\.?\s*A\.?\s*S\.?(?:-?\s*No\.?)?\b", "CAS"),
    (r"\bCAS-No\.?\b", "CAS:"),
    (r"\bCAS\s*[#]\b", "CAS:"),
    (r"\bCAS\s+Number\b", "CAS:"),
    (r"\bRegistry\s+Number\b", "CAS:"),
    (r"\bRN\b\s*[:\-]?\s*", "CAS: "),
]


def normalize_cas_labels_for_extraction(text: str) -> str:
    """
    Replace common SDS spellings of the CAS label with a canonical ``CAS:`` / ``CAS`` token.

    Does not alter EC / EINECS lines (those are not CAS registry numbers).
    """
    if not text or not str(text).strip():
        return ""
    t = str(text)
    for pat, repl in _CAS_LABEL_NORMALIZE:
        t = re.sub(pat, repl, t, flags=re.IGNORECASE)
    return t


def _append_canonical_from_raw(raw: str, seen: set[str], out: list[str]) -> None:
    if not raw:
        return
    norm = cas_validator.normalize_cas_input(raw) or raw.strip()
    relaxed, _ = cas_validator.validate_cas_relaxed(norm)
    if not relaxed:
        return
    ok, canonical = cas_validator.validate_cas(relaxed)
    if not ok or canonical in seen:
        return
    seen.add(canonical)
    out.append(canonical)


def find_checksum_valid_cas_in_text(text: str) -> list[str]:
    """
    Return unique CAS numbers in order of first appearance; only checksum-valid.

    Scans **original** and **label-normalized** text with a prefix-aware pattern
    and a bare ``N-N-N`` pattern so values after ``C.A.S.``, ``CAS-No.``, ``RN``,
    ``Registry Number``, ``Index No.``, etc. are found even when spacing differs.
    """
    if not text or not str(text).strip():
        return []
    seen: set[str] = set()
    out: list[str] = []

    def scan(sub: str) -> None:
        if not sub:
            return
        for m in _CAS_PREFIX_SCAN_RE.finditer(sub):
            _append_canonical_from_raw(m.group(1), seen, out)
        for m in _CAS_LINE_RE.finditer(sub):
            _append_canonical_from_raw(m.group(1), seen, out)

    s = str(text)
    scan(s)
    normalized = normalize_cas_labels_for_extraction(s)
    if normalized != s:
        scan(normalized)
    return out


def count_raw_cas_matches(text: str) -> int:
    """Count distinct CAS-shaped tokens after label normalization (may include invalid checksum)."""
    if not text:
        return 0
    t = normalize_cas_labels_for_extraction(str(text))
    raw: set[str] = set()
    for m in _CAS_PREFIX_SCAN_RE.finditer(t):
        raw.add(m.group(1))
    for m in _CAS_LINE_RE.finditer(t):
        raw.add(m.group(1))
    return len(raw)
