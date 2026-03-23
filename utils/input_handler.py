"""
Unified chemical input: typed CAS/name or SDS PDF -> normalized identifiers for assessment.

**Typed CAS / name (main form in ``app.py``)** uses ``ChemicalAssessmentService.assess`` only.
That path never calls ``_score_cas_confidence``.

**SDS PDF upload** uses MarkItDown / hybrid pipelines, then ``_score_cas_confidence`` for
checksum + optional PubChem filtering. Gate knobs (``SHOW_ONLY_PUBCHEM_VERIFIED``,
``MIN_CAS_CONFIDENCE``) are read from ``config`` only — not ``sds_strategy`` session
presets — so sidebar “strategy” testers for legacy parsers do not strip SDS rows or
affect typed-CAS assessment.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple

import streamlit as st

from utils import cas_validator


def _validate_checksum(cas: str) -> bool:
    """Return True if CAS passes checksum validation."""
    if not cas or not str(cas).strip():
        return False
    _, check_ok = cas_validator.validate_cas_relaxed(str(cas).strip())
    return check_ok


def _norm_cas_key(cas: str) -> str:
    """Normalize CAS for dict lookup (hyphens, spacing) so rows match scored cas_list."""
    s = (cas or "").strip()
    if not s:
        return ""
    return cas_validator.normalize_cas_input(s) or s


def _rows_by_norm_cas(rows: List[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Map normalized CAS → row (last wins)."""
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        c = (r.get("cas") or "").strip()
        if not c:
            continue
        k = _norm_cas_key(c)
        if k:
            out[k] = r
    return out


_METHOD_WEIGHTS: dict[str, float] = {
    "docling_table": 0.2,
    "pipe_table_parsing": 0.15,
    "fragment_reconstruction": 0.1,
    "regex_pattern": 0.05,
    "docling_distilbert": 0.15,
    "table": 0.15,
    "ocr_fallback": -0.1,
    "section1_anchor": 0.15,
    "markitdown_regex": 0.12,
    "markitdown_bert": 0.14,
    "markitdown_hybrid_primary": 0.13,
    "ocr_tesseract_regex": 0.08,
    "ocr_easyocr_regex": 0.09,
    "ocr_hybrid_fallback": 0.06,
}


def _calculate_confidence(row: dict[str, Any], pubchem_result: dict[str, Any]) -> float:
    """
    Compute confidence score (0–1) from checksum, PubChem, method, context,
    name-CAS validation (Phase 2), and multi-section evidence (Phase 3).
    """
    cas = (row.get("cas") or "").strip()
    if not cas:
        return 0.0

    confidence = 0.5  # Neutral baseline

    # Checksum: +0.3 if pass, -0.2 if fail
    if _validate_checksum(cas):
        confidence += 0.3
    else:
        confidence -= 0.2

    # PubChem: use boost from validator (0.2 if found, 0.0 if not, 0.0 if unknown)
    confidence += pubchem_result.get("confidence_boost", 0.0)

    # Phase 2: Name-CAS cross-check via PubChem (+0.15 when SDS name matches PubChem synonyms)
    if row.get("name_validated"):
        confidence += 0.15

    # Phase 3: Multi-section evidence – CAS in both Section 1 and Section 3 (+0.1)
    sections = row.get("sections") or []
    if 1 in sections and 3 in sections:
        confidence += 0.1

    # Method quality
    method = row.get("method") or ""
    confidence += _METHOD_WEIGHTS.get(method, 0.0)

    # Has chemical name (+0.1)
    if (row.get("chemical_name") or "").strip():
        confidence += 0.1

    # Has concentration (+0.05)
    if (row.get("concentration") or "").strip():
        confidence += 0.05

    return max(0.1, min(1.0, confidence))


# Methods that extract CAS from composition tables/sections (Section 1, 3) - high trust
_COMPOSITION_METHODS: frozenset[str] = frozenset({
    "section1_anchor", "composition_section3", "html_table_parsing", "pipe_table_parsing",
    "delimiter_table_parsing", "line_composition_parsing", "orphan_cas_line", "table_parsing",
    "docling", "docling_table", "docling_distilbert",
    "markitdown_regex", "markitdown_bert", "markitdown_hybrid_primary",
    "ocr_tesseract_regex", "ocr_easyocr_regex", "ocr_hybrid_fallback",
})
# Methods that infer CAS from digit sequences / full-text scan - high false positive risk
_LOW_TRUST_METHODS: frozenset[str] = frozenset({
    "reconstructor", "robust_fragment", "fragment_reconstruction",
})


def _filter_madeup_cas(cas_list: List[str], rows: List[dict[str, Any]]) -> Tuple[List[str], List[dict[str, Any]]]:
    """
    When composition-sourced CAS exist, exclude CAS from low-trust methods (fragment
    reconstruction, reconstructor) which often produce false positives from random
    digit sequences (dates, IDs, page numbers). Keep low-trust CAS only when they
    are the sole source (no composition CAS found).
    """
    if not rows or not cas_list:
        return cas_list, rows
    has_composition = any(
        (r.get("method") or "") in _COMPOSITION_METHODS for r in rows
    )
    if not has_composition:
        return cas_list, rows
    # Drop rows whose method is low-trust when we have composition CAS
    row_by_cas = _rows_by_norm_cas(rows)
    filtered_rows: List[dict[str, Any]] = []
    filtered_cas: List[str] = []
    for cas in cas_list:
        r = row_by_cas.get(_norm_cas_key(cas), {})
        method = (r.get("method") or "").strip()
        if method in _LOW_TRUST_METHODS:
            continue  # Exclude fragment/reconstructor when composition CAS exist
        filtered_rows.append(r)
        filtered_cas.append(cas)
    return filtered_cas, filtered_rows


def _score_cas_confidence(
    cas_list: List[str], rows: List[dict[str, Any]]
) -> Tuple[List[str], List[dict[str, Any]]]:
    """
    Score CAS with PubChem validation. Never show invalid or made-up CAS:
    - Hard filter: drop any CAS that fails checksum (no exceptions)
    - Filter: exclude fragment/reconstructor CAS when composition CAS exist
    - When gate on: hide only untrusted extractions that PubChem rejects (see gate below)
    """
    # Use ``config`` for gate knobs — not ``sds_strategy`` session presets (those target the
    # legacy unified parser; a "strict" preset was forcing SHOW_ONLY_PUBCHEM_VERIFIED and
    # clearing all MarkItDown SDS rows).
    import config as _cfg

    MIN_CAS_CONFIDENCE = float(getattr(_cfg, "MIN_CAS_CONFIDENCE", 0.0))
    SHOW_ONLY_PUBCHEM_VERIFIED = bool(getattr(_cfg, "SHOW_ONLY_PUBCHEM_VERIFIED", False))
    USE_PUBCHEM_CAS_VALIDATION = bool(getattr(_cfg, "USE_PUBCHEM_CAS_VALIDATION", True))
    from utils.pubchem_validator import get_pubchem_validator

    # Filter made-up CAS: when composition sources found CAS, drop fragment/reconstructor-only
    cas_list, rows = _filter_madeup_cas(cas_list, rows)

    # Normalize CAS so row lookup matches MarkItDown output (hyphen variants)
    _seen_norm: set[str] = set()
    _deduped: List[str] = []
    for c in cas_list:
        k = _norm_cas_key(c)
        if not k or k in _seen_norm:
            continue
        _seen_norm.add(k)
        _deduped.append(k)
    cas_list = _deduped

    # Hard filter: NEVER show CAS that fails checksum — avoid invalid/made-up CAS
    valid_cas_list: List[str] = []
    valid_rows: List[dict[str, Any]] = []
    row_by_cas = _rows_by_norm_cas(rows)
    for cas in cas_list:
        if not _validate_checksum(cas):
            continue
        valid_cas_list.append(cas)
        if cas in row_by_cas:
            valid_rows.append(dict(row_by_cas[cas]))

    validator = get_pubchem_validator()
    row_by_cas = _rows_by_norm_cas(valid_rows)
    # Fallback: single extraction method for the batch if a row lost its method key
    _batch_method = ""
    if rows:
        _batch_method = str((rows[0] or {}).get("method") or "").strip()

    enriched: List[dict[str, Any]] = []
    for cas in valid_cas_list:
        r = dict(row_by_cas.get(cas, {}))
        r["cas"] = cas
        if not (r.get("method") or "").strip() and _batch_method:
            r["method"] = _batch_method

        if USE_PUBCHEM_CAS_VALIDATION:
            check = validator.validate(cas)
            ex = check.get("exists")
            # Tri-state: None = lookup failed (network); do not treat like "not in PubChem"
            if ex is True:
                r["pubchem_verified"] = True
            elif ex is None:
                r["pubchem_verified"] = None
            else:
                r["pubchem_verified"] = False
            r["pubchem_status"] = "verified" if ex is True else ("unknown" if ex is None else "not_found")
            if check.get("name") and not r.get("chemical_name"):
                r["chemical_name"] = check["name"]
            r["_pubchem_result"] = check
        else:
            r["pubchem_verified"] = None
            r["pubchem_status"] = "skipped"
            r["_pubchem_result"] = {"confidence_boost": 0.0}

        # Phase 2: Name-CAS cross-check via PubChem when SDS chemical name is available
        chem_name = (r.get("chemical_name") or "").strip()
        if chem_name:
            try:
                from utils.sds_dual_parser import _validate_cas_via_name
                name_match, _ = _validate_cas_via_name(cas, chem_name)
                r["name_validated"] = name_match
            except Exception:
                r["name_validated"] = False
        else:
            r["name_validated"] = None

        r["confidence"] = _calculate_confidence(r, r.get("_pubchem_result", {}))
        r.pop("_pubchem_result", None)

        # When gate on: hide CAS only if PubChem says "not found" *and* extraction is low-trust.
        # Keep verified, unknown (API failure), and checksum-valid MarkItDown/OCR/composition rows.
        if SHOW_ONLY_PUBCHEM_VERIFIED and USE_PUBCHEM_CAS_VALIDATION:
            pv = r["pubchem_verified"]
            if pv is False:
                method = (r.get("method") or "").strip() or _batch_method
                if method not in _COMPOSITION_METHODS or not _validate_checksum(cas):
                    continue
        enriched.append(r)

    enriched.sort(key=lambda x: float(x.get("confidence", 0)), reverse=True)
    min_conf = float(MIN_CAS_CONFIDENCE) if isinstance(MIN_CAS_CONFIDENCE, (int, float)) else 0.0
    filtered = [r for r in enriched if float(r.get("confidence", 0)) >= min_conf]

    out_cas = [r["cas"] for r in filtered]
    return out_cas, filtered


@dataclass
class ChemicalInput:
    """Result of parsing user input before database assessment."""

    input_type: str  # "cas" | "name" | "sds_single" | "sds_multi"
    primary: str  # string passed to existing pipeline (CAS or name)
    cas_numbers: list[str] = field(default_factory=list)
    source_label: Optional[str] = None
    extraction_rows: list[dict[str, Any]] = field(default_factory=list)
    # Set when MarkItDown/hybrid pipeline raises (see metrics["error"] in alternative_extraction)
    extraction_error: Optional[str] = None

    def has_multiple_cas(self) -> bool:
        return len(self.cas_numbers) > 1


class UnifiedInputHandler:
    """Routes text or uploaded PDF to a ChemicalInput."""

    def __init__(self) -> None:
        pass

    def process_text(self, text: str) -> Optional[ChemicalInput]:
        """Typed CAS or chemical name (same rules as main form)."""
        if not text or not str(text).strip():
            return None
        raw = str(text).strip()
        norm = cas_validator.normalize_cas_input(raw)
        if not norm:
            return None
        if cas_validator.is_valid_cas_format(norm):
            return ChemicalInput(
                input_type="cas",
                primary=norm,
                cas_numbers=[norm],
                source_label="typed",
            )
        return ChemicalInput(
            input_type="name",
            primary=norm,
            cas_numbers=[],
            source_label="typed_name",
        )

    def process_sds_pdf(self, uploaded_file: Any) -> Optional[ChemicalInput]:
        """
        Extract CAS from SDS PDF via **MarkItDown + regex** or **Hybrid** (MarkItDown → OCR).
        See ``docs/SDS_EXTRACTION_PIPELINES.md``. uploaded_file: Streamlit UploadedFile.
        """
        if uploaded_file is None:
            return None
        pdf_bytes = uploaded_file.getvalue()
        if not pdf_bytes:
            return None
        from utils.sds_debug import sds_debug_log

        sds_debug_log(
            "input_handler.process_sds_pdf",
            {"filename": getattr(uploaded_file, "name", None), "bytes": len(pdf_bytes)},
        )

        try:
            from utils.alternative_extraction import get_extraction_pipeline_mode

            mode = get_extraction_pipeline_mode()
        except Exception:
            mode = "hybrid_md_ocr"

        return self._process_sds_pdf_alternative(uploaded_file, pdf_bytes, mode)

    def _process_sds_pdf_alternative(self, uploaded_file: Any, pdf_bytes: bytes, mode: str) -> ChemicalInput:
        """MarkItDown / OCR / hybrid pipelines with shared scoring."""
        from utils.alternative_extraction import run_pipeline_with_metrics
        from utils.sds_debug import sds_debug_log

        rows, metrics = run_pipeline_with_metrics(pdf_bytes, mode)
        sds_debug_log(
            "input_handler.alternative",
            {"mode": mode, "metrics": metrics, "n_rows": len(rows)},
        )
        cas_list: list[str] = []
        seen: set[str] = set()
        best: tuple[float, str] = (-1.0, "")
        for r in rows:
            c = (r.get("cas") or "").strip()
            if not c or c in seen:
                continue
            seen.add(c)
            cas_list.append(c)
            conf = float(r.get("confidence") or 0.0)
            if conf > best[0]:
                best = (conf, c)

        raw_count_before_gate = len(cas_list)
        cas_list, rows = _score_cas_confidence(cas_list, rows)
        primary = best[1] if best[1] and best[1] in cas_list else (cas_list[0] if cas_list else "")
        in_type = "sds_multi" if len(cas_list) > 1 else "sds_single"
        label = getattr(uploaded_file, "name", None) or "SDS.pdf"
        err = metrics.get("error")
        filtered_note: Optional[str] = None
        if not err and raw_count_before_gate > 0 and not cas_list:
            filtered_note = (
                f"Extractor found {raw_count_before_gate} checksum-valid CAS, but all were removed by "
                "filters (PubChem gate if SHOW_ONLY_PUBCHEM_VERIFIED=1, or MIN_CAS_CONFIDENCE too high). "
                "Defaults: SHOW_ONLY_PUBCHEM_VERIFIED=0, MIN_CAS_CONFIDENCE=0."
            )
        return ChemicalInput(
            input_type=in_type if cas_list else "sds_single",
            primary=primary,
            cas_numbers=cas_list,
            source_label=label,
            extraction_rows=rows,
            extraction_error=str(err) if err else filtered_note,
        )


@st.cache_resource
def get_input_handler() -> UnifiedInputHandler:
    return UnifiedInputHandler()
