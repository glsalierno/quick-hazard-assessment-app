"""
Robust multi-stage CAS extractor for SDS PDFs.

Handles adversarial formatting: spaces around hyphens, Unicode dashes, split digits,
complex tables. Memory-aware: Docling and OCR are optional toggles for Streamlit Cloud.

Pipeline:
  1. pdfplumber – fast text and table extraction (always on)
  2. Docling – optional, for complex table structure (disabled by default)
  3. OCR – optional fallback for scanned PDFs (pytesseract + pdf2image)
  4. Fragment reconstruction – reassembles CAS from separated digit sequences
  5. Context extraction – chemical names and concentrations near CAS
"""

from __future__ import annotations

import logging
import re
import unicodedata
from io import BytesIO
from typing import Any, Optional

from utils import cas_validator
from utils.cas_reconstructor import CASReconstructor
from utils.sds_models import CASExtraction

logger = logging.getLogger(__name__)


def _get_reconstructor() -> CASReconstructor:
    """Build reconstructor from config or strategy override."""
    try:
        from utils.sds_strategy import get as strategy_get

        max_gap = strategy_get("RECONSTRUCTOR_MAX_GAP", 15)
        use_context = strategy_get("RECONSTRUCTOR_USE_CONTEXT_FILTER", True)
    except Exception:
        max_gap = 15
        use_context = False
    return CASReconstructor(
        max_gap=max_gap,
        try_ocr_corrections=False,
        use_context_filter=use_context,
    )


def _is_cas_debug() -> bool:
    """Lazy check for CAS debug mode (avoids importing Streamlit when not needed)."""
    try:
        from utils.sds_debug import is_cas_debug_enabled

        return is_cas_debug_enabled()
    except Exception:
        return False


def _cas_debug_log(stage: str, match: str = "", cas: str = "", validated: Optional[str] = None, context: str = "") -> None:
    """Log CAS extraction detail to Streamlit debug console when cas_debug is on."""
    if not _is_cas_debug():
        return
    logger.info("CAS debug [%s]: match=%r cas=%r validated=%s", stage, match, cas, validated)
    try:
        from datetime import datetime

        import streamlit as st

        from utils.sds_debug import make_json_safe

        entry = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "stage": f"cas_extractor.{stage}",
            "data": make_json_safe({"match": match, "cas": cas, "validated": validated, "context": (context[:200] if context else "")}),
            "metadata": {},
        }
        logs = st.session_state.setdefault("sds_debug_logs", [])
        logs.append(entry)
        if len(logs) > 80:
            del logs[: len(logs) - 80]
    except Exception:
        pass

# --- Adversarial CAS patterns ---
# Spaces around hyphens, Unicode dash variants (en-dash U+2013, em-dash U+2014, minus U+2212, full-width U+FF0D)
_CAS_DASH_CHARS = r"[\-\u2010-\u2015\u2212\uFF0D\s]*"
# Standard: 1-7 digits, hyphen/dash, 2 digits, hyphen/dash, 1 digit
_CAS_FLEXIBLE_RE = re.compile(
    rf"\b(\d{{1,7}})\s*[\-\u2010-\u2015\u2212\uFF0D]\s*(\d{{2}})\s*[\-\u2010-\u2015\u2212\uFF0D]\s*(\d)\b",
    re.IGNORECASE,
)
# Fragment reconstruction: three digit sequences that could form CAS (e.g. "75 45 6" or "75.45.6")
_CAS_FRAGMENT_RE = re.compile(
    r"\b(\d{1,7})\s*[.\s\u2010-\u2015\-]\s*(\d{2})\s*[.\s\u2010-\u2015\-]\s*(\d)\b",
)
# After "CAS" label (same line) – strip prefix
_CAS_PREFIX_RE = re.compile(
    r"(?:CAS\s*(?:No\.?|Number|#|Registry\s*No\.?)?\s*[:\-]?\s*)",
    re.IGNORECASE,
)
# Concentration: ranges like ">=30 - <60%", simple "30%"
_CONC_RANGE_RE = re.compile(
    r"(?:([<>]=?)\s*)?(\d+(?:\.\d+)?)\s*[-–]\s*(?:([<>]=?)\s*)?(\d+(?:\.\d+)?)\s*[%％]?",
    re.IGNORECASE,
)
_CONC_SIMPLE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*[%％]", re.IGNORECASE)


def _clean_text_for_cas(text: str) -> str:
    """
    Aggressive Unicode cleaning for adversarial PDFs.
    - NFKC normalization (compatibility chars)
    - Remove zero-width spaces (U+200B, U+200C, U+200D, U+FEFF)
    - Normalize all dash variants to ASCII hyphen
    """
    if not text:
        return ""
    t = unicodedata.normalize("NFKC", text)
    t = re.sub(r"[\u200B-\u200D\uFEFF]", "", t)  # zero-width spaces
    t = (
        t.replace("\u2010", "-")
        .replace("\u2011", "-")
        .replace("\u2012", "-")
        .replace("\u2013", "-")  # en-dash
        .replace("\u2014", "-")  # em-dash
        .replace("\u2015", "-")
        .replace("\u2212", "-")  # minus sign
        .replace("\uFF0D", "-")  # full-width hyphen
    )
    # Collapse spaces between digits: "7 5 - 4 5 - 6" -> "75-45-6"
    t = re.sub(r"(\d)\s+(\d)", r"\1\2", t)
    return t


def _normalize_cas_text(text: str) -> str:
    """Alias for backward compat; delegates to _clean_text_for_cas."""
    return _clean_text_for_cas(text)


def _extract_concentration(text: str) -> Optional[str]:
    """Extract concentration string (ranges or simple %)."""
    if not text or not text.strip():
        return None
    t = text.strip()
    m = _CONC_RANGE_RE.search(t)
    if m:
        lo, lv, ho, hv = m.group(1) or "", m.group(2), m.group(3) or "", m.group(4)
        left = f"{lo} {lv}".strip() if lo else lv
        right = f"{ho} {hv}".strip() if ho else hv
        return f"{left} - {right}%"
    m = _CONC_SIMPLE_RE.search(t)
    if m:
        return f"{m.group(1)}%"
    return t[:80] if len(t) > 80 else t


def _extract_context(text: str, pos: int, window: int = 120) -> tuple[str, Optional[str], Optional[str]]:
    """
    Extract surrounding context around a CAS match. Returns (raw_context, chemical_name, concentration).
    Chemical name: text with letters before CAS. Concentration: % pattern after CAS.
    """
    start = max(0, pos - window)
    end = min(len(text), pos + window)
    raw = text[start:end]
    before = text[max(0, pos - 150) : pos]
    after = text[pos : min(len(text), pos + 150)]
    name = None
    conc = None
    # Name: last run of non-digit text (letters) before the CAS
    name_match = re.search(r"([A-Za-z][A-Za-z0-9\s,\-\.\'\u2019\u2013]+?)(?=\s*\d{1,7}[\-\s])", before)
    if name_match:
        cand = name_match.group(1).strip()
        if len(cand) > 2 and not re.match(r"^[\d\s.%<>,\-–]+$", cand):
            name = cand[:120]
    conc_cand = _extract_concentration(after)
    if conc_cand:
        conc = conc_cand
    return raw, name, conc


def _validate_and_normalize_cas(first: str, second: str, check: str) -> Optional[str]:
    """
    Validate CAS and return normalized string or None.
    Prefers format-valid CAS; validate_cas_relaxed may alter check digit.
    """
    norm = cas_validator.normalize_to_cas_format(first.strip(), second.strip(), check.strip())
    if not norm:
        return None
    try:
        result, check_ok = cas_validator.validate_cas_relaxed(norm)
        # If checksum validation altered the CAS, prefer original if format is valid
        if result and result != norm and not check_ok:
            if cas_validator.is_valid_cas_format(norm):
                return norm
        return result or norm
    except Exception:
        return norm if cas_validator.is_valid_cas_format(norm) else None


def _reconstruct_from_digit_sequences(text: str) -> list[tuple[str, int]]:
    """
    Fragment reconstruction: find three digit sequences that could form a CAS.
    Scans whole text for \\b(\\d{1,7})\\b; when three consecutive matches have
    lengths (1-7, 2, 1), try to form CAS. Returns list of (cas, start_pos).
    """
    text_clean = _clean_text_for_cas(text)
    matches = list(re.finditer(r"\b(\d{1,7})\b", text_clean))
    results: list[tuple[str, int]] = []
    seen: set[str] = set()

    for i in range(len(matches) - 2):
        a, b, c = matches[i].group(1), matches[i + 1].group(1), matches[i + 2].group(1)
        if len(b) != 2 or len(c) != 1 or not (1 <= len(a) <= 7):
            continue
        # Require close proximity: gap between first and third match < 25 chars
        gap = matches[i + 2].start() - matches[i].end()
        if gap > 25:
            continue
        candidate = f"{a}-{b}-{c}"
        if candidate in seen:
            continue
        norm = _validate_and_normalize_cas(a, b, c)
        if norm:
            seen.add(norm)
            results.append((norm, matches[i].start()))
    return results


def _extract_cas_from_text(text: str, source_page: Optional[int] = None) -> list[CASExtraction]:
    """
    Stage 4: Extract CAS from text using adversarial patterns + fragment reconstruction.
    Returns list of CASExtraction with context.
    """
    if not text:
        return []
    _cas_debug_log("input_text", context=text[:2000] if text else "")
    text_norm = _clean_text_for_cas(text)
    results: list[CASExtraction] = []
    seen: set[str] = set()

    # Flexible pattern (spaces, Unicode dashes)
    for m in _CAS_FLEXIBLE_RE.finditer(text_norm):
        first, second, check = m.groups()
        raw_match = m.group(0)
        cas = _validate_and_normalize_cas(first, second, check)
        _cas_debug_log("flexible", match=raw_match, cas=f"{first}-{second}-{check}", validated=cas)
        if cas and cas not in seen:
            seen.add(cas)
            raw_ctx, name, conc = _extract_context(text, m.start())
            results.append(
                CASExtraction(
                    cas=cas,
                    chemical_name=name,
                    concentration=conc,
                    section=3,
                    method="robust_flexible",
                    confidence=0.92,
                    context=raw_ctx[:300] if raw_ctx else None,
                    validated=True,
                )
            )

    # Fragment pattern (dots/spaces as separators)
    for m in _CAS_FRAGMENT_RE.finditer(text_norm):
        first, second, check = m.groups()
        cas = _validate_and_normalize_cas(first, second, check)
        _cas_debug_log("fragment", match=m.group(0), cas=f"{first}-{second}-{check}", validated=cas)
        if cas and cas not in seen:
            seen.add(cas)
            raw_ctx, name, conc = _extract_context(text, m.start())
            results.append(
                CASExtraction(
                    cas=cas,
                    chemical_name=name,
                    concentration=conc,
                    section=3,
                    method="robust_fragment",
                    confidence=0.88,
                    context=raw_ctx[:300] if raw_ctx else None,
                    validated=True,
                )
            )

    # Digit-sequence reconstruction (e.g. "75" "45" "6" split across lines)
    for cas, pos in _reconstruct_from_digit_sequences(text):
        _cas_debug_log("digit_seq", match="", cas=cas, validated=cas)
        if cas not in seen:
            seen.add(cas)
            raw_ctx, name, conc = _extract_context(text, pos)
            results.append(
                CASExtraction(
                    cas=cas,
                    chemical_name=name,
                    concentration=conc,
                    section=3,
                    method="robust_digit_seq",
                    confidence=0.85,
                    context=raw_ctx[:300] if raw_ctx else None,
                    validated=True,
                )
            )

    return results


def _pdfplumber_extract(pdf_bytes: bytes) -> tuple[str, list[list[list[str]]]]:
    """Stage 1: pdfplumber text and table extraction. Returns (full_text, list of tables)."""
    try:
        import pdfplumber
    except ImportError:
        logger.debug("pdfplumber not installed")
        return "", []

    text_parts: list[str] = []
    tables_all: list[list[list[str]]] = []

    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
                page_tables = page.extract_tables() or []
                for tbl in page_tables:
                    if tbl and len(tbl) >= 2:
                        tables_all.append(tbl)
    except Exception as e:
        logger.warning("pdfplumber extract failed: %s", e)
        return "", []

    full_text = "\n".join(text_parts)
    return full_text, tables_all


def _tables_to_cas_extractions(tables: list[list[list[str]]]) -> list[CASExtraction]:
    """Parse tables for CAS in rows; preserve row relationships for name/conc."""
    results: list[CASExtraction] = []
    seen: set[str] = set()

    for table in tables:
        if not table or len(table) < 2:
            continue
        header_row = [str(c).lower() for c in table[0]]
        joined = " ".join(header_row)
        cas_col = name_col = conc_col = None
        for i, h in enumerate(header_row):
            if any(x in h for x in ("cas", "cas-no", "registry")):
                cas_col = i
            elif any(x in h for x in ("chemical", "component", "ingredient", "substance", "name")):
                name_col = i
            elif any(x in h for x in ("wt", "weight", "concentration", "%", "percent")):
                conc_col = i

        # Fallback: if no CAS header, find column with digits + hyphen pattern
        if cas_col is None and len(table) > 1:
            max_cols = max(len(row) for row in table if row)
            for col_idx in range(max_cols):
                cells = [str(row[col_idx] or "") for row in table[1:7] if col_idx < len(row)]
                sample = " ".join(cells)
                if re.search(r"\d{1,7}[\-\u2010-\u2015]\d{2}[\-\u2010-\u2015]\d", sample):
                    cas_col = col_idx
                    break
        if cas_col is None:
            continue

        for row in table[1:]:
            if len(row) <= cas_col:
                continue
            cell = str(row[cas_col] or "").strip()
            m = _CAS_FLEXIBLE_RE.search(cell) or _CAS_FRAGMENT_RE.search(_normalize_cas_text(cell))
            if not m:
                continue
            first, second, check = m.groups()
            cas = _validate_and_normalize_cas(first, second, check)
            _cas_debug_log("table", match=cell, cas=f"{first}-{second}-{check}", validated=cas)
            if not cas or cas in seen:
                continue
            seen.add(cas)

            name = None
            if name_col is not None and name_col < len(row):
                name = str(row[name_col] or "").strip() or None
            conc = None
            if conc_col is not None and conc_col < len(row):
                conc = _extract_concentration(str(row[conc_col] or "")) or str(row[conc_col]).strip() or None

            results.append(
                CASExtraction(
                    cas=cas,
                    chemical_name=name,
                    concentration=conc,
                    section=3,
                    method="robust_table",
                    confidence=0.95,
                    context=" | ".join(str(c) for c in row)[:280],
                    validated=True,
                )
            )
    return results


def _docling_extract(pdf_bytes: bytes) -> list[CASExtraction]:
    """Stage 2 (optional): Docling for complex table structure. Returns list of CASExtraction."""
    try:
        from config import USE_DOCLING
        if not USE_DOCLING:
            return []
    except ImportError:
        return []

    try:
        from utils.docling_sds_parser import is_docling_available, is_docling_disabled
        if is_docling_disabled() or not is_docling_available():
            return []
    except ImportError:
        return []

    try:
        from utils.docling_sds_parser import extract_composition_from_pdf
        comps = extract_composition_from_pdf(pdf_bytes, use_cache=True, low_memory=True)
        if not comps:
            return []
        return [
            CASExtraction(
                cas=c.cas,
                chemical_name=c.chemical_name,
                concentration=c.concentration,
                section=3,
                method="robust_docling",
                confidence=0.97,
                context=c.context,
                validated=True,
            )
            for c in comps
        ]
    except Exception as e:
        logger.debug("Docling extract failed: %s", e)
        return []


def _ocr_extract(pdf_bytes: bytes) -> str:
    """Stage 3 (optional): OCR for scanned PDFs."""
    try:
        from utils import sds_pdf_utils
        return sds_pdf_utils.extract_text_from_pdf_bytes(
            pdf_bytes,
            use_ocr_if_needed=True,
            min_text_length=50,
        )
    except ImportError:
        return ""


class RobustCASExtractor:
    """
    Multi-stage CAS extractor for adversarial SDS PDFs.
    Memory-aware: Docling and OCR are optional toggles.
    """

    def __init__(self, use_docling: bool = False, use_ocr: bool = False) -> None:
        self.use_docling = use_docling
        self.use_ocr = use_ocr

    def extract(self, pdf_bytes: bytes) -> list[CASExtraction]:
        """
        Run the full pipeline. Returns deduplicated CASExtraction list, sorted by confidence.
        """
        if not pdf_bytes or len(pdf_bytes) < 50:
            return []

        store: dict[str, CASExtraction] = {}
        seen: set[str] = set()

        def _put(item: CASExtraction) -> None:
            if item.cas not in seen:
                seen.add(item.cas)
                store[item.cas] = item
            else:
                # Prefer higher confidence / richer data
                existing = store[item.cas]
                if item.confidence > existing.confidence or (
                    (item.chemical_name or item.concentration)
                    and not (existing.chemical_name or existing.concentration)
                ):
                    merged = CASExtraction(
                        cas=item.cas,
                        chemical_name=item.chemical_name or existing.chemical_name,
                        concentration=item.concentration or existing.concentration,
                        section=item.section or existing.section,
                        method=item.method,
                        confidence=max(item.confidence, existing.confidence),
                        context=item.context or existing.context,
                        validated=item.validated or existing.validated,
                        warnings=list(set(existing.warnings + item.warnings)),
                    )
                    store[item.cas] = merged

        # Stage 0 + 1: pdfplumber extraction, tables + text first (or reconstructor if fallback-only)
        try:
            full_text, tables = _pdfplumber_extract(pdf_bytes)
            use_fallback_only = False
            try:
                from utils.sds_strategy import get as strategy_get

                use_fallback_only = strategy_get("USE_RECONSTRUCTOR_AS_FALLBACK_ONLY", True)
            except Exception:
                pass

            _recon = _get_reconstructor()

            if use_fallback_only:
                for item in _tables_to_cas_extractions(tables):
                    _put(item)
                for item in _extract_cas_from_text(full_text):
                    _put(item)
                if not store and full_text:
                    recon_cas = _recon.reconstruct_from_text(full_text)
                    if _is_cas_debug():
                        dbg = _recon.reconstruct_with_debug(full_text)
                        _cas_debug_log("reconstructor", cas=",".join(recon_cas), context=str(dbg)[:400])
                    for cas in recon_cas:
                        _put(
                            CASExtraction(
                                cas=cas,
                                section=3,
                                method="reconstructor",
                                confidence=0.95,
                                validated=True,
                            )
                        )
            else:
                if full_text:
                    recon_cas = _recon.reconstruct_from_text(full_text)
                    if _is_cas_debug():
                        dbg = _recon.reconstruct_with_debug(full_text)
                        _cas_debug_log("reconstructor", cas=",".join(recon_cas), context=str(dbg)[:400])
                    for cas in recon_cas:
                        _put(
                            CASExtraction(
                                cas=cas,
                                section=3,
                                method="reconstructor",
                                confidence=0.95,
                                validated=True,
                            )
                        )
                for item in _tables_to_cas_extractions(tables):
                    _put(item)
                for item in _extract_cas_from_text(full_text):
                    _put(item)
        except Exception as e:
            logger.warning("pdfplumber stage failed: %s", e)

        # Stage 2: Docling (optional)
        if self.use_docling:
            try:
                docling_items = _docling_extract(pdf_bytes)
                for item in docling_items:
                    _put(item)
            except Exception as e:
                logger.debug("Docling stage failed: %s", e)

        # Stage 3: OCR (optional) if still little text
        if self.use_ocr and not store:
            try:
                ocr_text = _ocr_extract(pdf_bytes)
                if ocr_text and len(ocr_text) > 100:
                    for item in _extract_cas_from_text(ocr_text):
                        _put(item)
            except Exception as e:
                logger.debug("OCR stage failed: %s", e)

        # Stage 4: Fragment reconstruction on combined text (fallback if pdfplumber gave text but no CAS)
        if not store:
            try:
                full_text, _ = _pdfplumber_extract(pdf_bytes)
                if not full_text or len(full_text) < 50:
                    full_text = _ocr_extract(pdf_bytes) if self.use_ocr else ""
                if full_text:
                    for item in _extract_cas_from_text(full_text):
                        _put(item)
            except Exception as e:
                logger.debug("Fragment stage failed: %s", e)

        out = list(store.values())
        out.sort(key=lambda x: (-x.confidence, x.cas))
        return out


def get_robust_extractor(use_docling: bool = False, use_ocr: bool = False) -> RobustCASExtractor:
    """Factory for RobustCASExtractor. Cache with @st.cache_resource in callers if needed."""
    return RobustCASExtractor(use_docling=use_docling, use_ocr=use_ocr)


# Alias for diagnostic code: get_cas_extractor -> get_robust_extractor
get_cas_extractor = get_robust_extractor
