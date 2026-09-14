"""
CAS number reconstruction from corrupted PDF text.

Addresses digit loss during PDF extraction: numbers split across lines,
Unicode hyphen variants, spaces inside CAS, OCR digit substitutions.

Runs BEFORE regex extraction to fix CAS before other stages.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from utils import cas_validator

# Unicode dash variants to normalize
_HYPHEN_VARIANTS = [
    "\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2015",
    "\u2212", "\uFF0D",
]


def _normalize_text(text: str) -> str:
    """
    Normalize text before CAS extraction.
    - Unicode dash variants → ASCII hyphen
    - Zero-width spaces removed
    - Spaces around hyphens inside digit contexts collapsed
    """
    if not text:
        return ""
    for h in _HYPHEN_VARIANTS:
        text = text.replace(h, "-")
    text = re.sub(r"[\u200B-\u200D\uFEFF]", "", text)
    # Collapse spaces inside CAS-like: "75 - 45 - 6" -> "75-45-6"
    text = re.sub(r"(\d)\s+-\s+(\d)", r"\1-\2", text)
    text = re.sub(r"(\d)\s+(\d{2})\s+-\s+(\d)\b", r"\1-\2-\3", text)
    # Collapse spaces between digits: "7 5 - 4 5 - 6" -> "75-45-6"
    text = re.sub(r"(\d)\s+(\d)", r"\1\2", text)
    return text


def _looks_like_date(cas: str) -> bool:
    """
    Reject CAS that clearly look like dates to avoid false positives:
    - 0-XX-X (e.g. 0-31-4)
    - 19xx/20xx-XX-X (years)
    - Only when first part is exactly 2 digits (01-12): treat as month-day if second is 01-31
    """
    if not cas or "-" not in cas:
        return False
    parts = cas.split("-")
    if len(parts) != 3:
        return False
    p1, p2 = parts[0], parts[1]
    if p1 == "0":
        return True
    if len(p1) == 4 and p1.startswith(("19", "20")):
        try:
            if 1900 <= int(p1) <= 2099:
                return True
        except ValueError:
            pass
    # Month-day: only when first is 01-12 (2 digits) and second 01-31 (2 digits)
    if len(p1) == 2 and len(p2) == 2:
        try:
            m, d = int(p1), int(p2)
            if 1 <= m <= 12 and 1 <= d <= 31:
                return True
        except ValueError:
            pass
    return False


def _validate_cas_checksum(cas: str) -> bool:
    """Validate CAS check digit using cas_validator."""
    if not cas or "-" not in cas:
        return False
    parts = cas.split("-")
    if len(parts) != 3:
        return False
    norm = cas_validator.normalize_to_cas_format(parts[0], parts[1], parts[2])
    if not norm:
        return False
    _, ok = cas_validator.validate_cas_relaxed(norm)
    return ok


def _try_ocr_correction(digits: str) -> List[str]:
    """
    For scanned PDFs: try common OCR substitutions in digit-only context.
    Returns list of variants (original first, then corrections).
    Does NOT apply globally - only to strings that are already digit-like.
    """
    if not digits or not all(c in "0123456789OolS5" for c in digits):
        return [digits] if digits.isdigit() else []
    variants = [digits]
    # Only substitute in strings that look like they could be CAS parts
    for old, new in [("O", "0"), ("o", "0"), ("l", "1"), ("S", "5"), ("s", "5")]:
        if old in digits:
            v = digits.replace(old, new)
            if v.isdigit() and v not in variants:
                variants.append(v)
    return variants


@dataclass
class ReconstructResult:
    """Result of CAS reconstruction with optional debug info."""
    cas_list: List[str] = field(default_factory=list)
    digit_sequences: List[Dict[str, Any]] = field(default_factory=list)
    candidates: List[str] = field(default_factory=list)
    raw_snippet: str = ""
    normalized_snippet: str = ""


class CASReconstructor:
    """
    Reconstructs CAS numbers from corrupted PDF text.
    Runs BEFORE regex extraction to fix digit loss.
    """

    def __init__(
        self,
        max_gap: int = 15,
        try_ocr_corrections: bool = False,
        use_context_filter: bool = False,
    ):
        self.max_gap = max_gap
        self.try_ocr_corrections = try_ocr_corrections
        self.use_context_filter = use_context_filter

    def _extract_all_digit_sequences(self, text: str) -> List[Dict[str, Any]]:
        """Extract every contiguous digit sequence with position context."""
        sequences: List[Dict[str, Any]] = []
        pattern = re.compile(r"\b(\d{1,7})\b")
        for m in pattern.finditer(text):
            sequences.append({
                "digits": m.group(1),
                "start": m.start(),
                "end": m.end(),
                "length": len(m.group(1)),
            })
        return sequences

    def _has_cas_context(self, text: str, start: int, end: int, window: int = 80) -> bool:
        """True if text within ±window chars contains CAS-like keywords."""
        if not text:
            return False
        lo = max(0, start - window)
        hi = min(len(text), end + window)
        snippet = text[lo:hi].lower()
        return any(kw in snippet for kw in ("cas", "no.", "number", "registry", "composition", "ingredient"))

    def _assemble_cas_triples(
        self, sequences: List[Dict[str, Any]], text: str = ""
    ) -> List[str]:
        """
        Assemble three consecutive digit sequences into CAS candidates.
        CAS format: [1-7 digits]-[2 digits]-[1 digit]
        Rejects date-like patterns. Prefers candidates with CAS-like context when text provided.
        """
        candidates: List[str] = []
        seen: set[str] = set()

        for i in range(len(sequences) - 2):
            p1 = sequences[i]["digits"]
            p2 = sequences[i + 1]["digits"]
            p3 = sequences[i + 2]["digits"]

            if len(p2) != 2 or len(p3) != 1 or not (1 <= len(p1) <= 7):
                continue

            gap1 = sequences[i + 1]["start"] - sequences[i]["end"]
            gap2 = sequences[i + 2]["start"] - sequences[i + 1]["end"]
            if gap1 > self.max_gap or gap2 > self.max_gap:
                continue

            candidate = f"{p1}-{p2}-{p3}"
            if candidate in seen:
                continue
            if _looks_like_date(candidate):
                continue
            if self.use_context_filter and text and not self._has_cas_context(text, sequences[i]["start"], sequences[i + 2]["end"]):
                continue
            seen.add(candidate)
            candidates.append(candidate)
        return candidates

    def reconstruct_from_text(self, raw_text: str) -> List[str]:
        """
        Main method: find digit sequences, assemble triples, validate checksum.
        Returns only CAS that pass validation.
        """
        if not raw_text or len(raw_text) < 10:
            return []

        normalized = _normalize_text(raw_text)
        sequences = self._extract_all_digit_sequences(normalized)
        candidates = self._assemble_cas_triples(sequences, normalized)

        valid: List[str] = []
        seen: set[str] = set()

        for cand in candidates:
            if cand in seen:
                continue
            if _validate_cas_checksum(cand):
                valid.append(cand)
                seen.add(cand)
                continue
            if self.try_ocr_corrections:
                parts = cand.split("-")
                for v1 in _try_ocr_correction(parts[0]):
                    for v2 in _try_ocr_correction(parts[1]):
                        for v3 in _try_ocr_correction(parts[2]):
                            if v1 and v2 and v3 and len(v2) == 2 and len(v3) == 1:
                                alt = f"{v1}-{v2}-{v3}"
                                if alt not in seen and _validate_cas_checksum(alt):
                                    valid.append(alt)
                                    seen.add(alt)
                                    break
                        else:
                            continue
                        break

        return valid

    def reconstruct_with_debug(self, raw_text: str) -> Dict[str, Any]:
        """
        Debug version: returns intermediate steps for diagnostics.
        """
        normalized = _normalize_text(raw_text)
        sequences = self._extract_all_digit_sequences(normalized)
        candidates = self._assemble_cas_triples(sequences, normalized)

        valid: List[str] = []
        for c in candidates:
            if _validate_cas_checksum(c):
                valid.append(c)

        return {
            "raw_text_snippet": raw_text[:500] if raw_text else "",
            "normalized_snippet": normalized[:500] if normalized else "",
            "digit_sequences": sequences[:50],
            "candidates": candidates[:30],
            "valid_cas": valid,
        }
