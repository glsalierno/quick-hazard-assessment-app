"""
Unified chemical input: typed CAS/name or SDS PDF -> normalized identifiers for assessment.
"""

from __future__ import annotations

import os
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


_METHOD_WEIGHTS: dict[str, float] = {
    "docling_table": 0.2,
    "pipe_table_parsing": 0.15,
    "fragment_reconstruction": 0.1,
    "regex_pattern": 0.05,
    "docling_distilbert": 0.15,
    "table": 0.15,
    "ocr_fallback": -0.1,
}


def _calculate_confidence(row: dict[str, Any], pubchem_result: dict[str, Any]) -> float:
    """
    Compute confidence score (0–1) from checksum, PubChem, method, and context.
    DSSTox cross-check disabled; validation relies on PubChem only.
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


def _score_cas_confidence(
    cas_list: List[str], rows: List[dict[str, Any]]
) -> Tuple[List[str], List[dict[str, Any]]]:
    """
    Score CAS with PubChem-only validation. Invalid CAS (not found in PubChem) are hidden.
    Sort by confidence (high first). DSSTox cross-check disabled.
    """
    from config import MIN_CAS_CONFIDENCE, SHOW_ONLY_PUBCHEM_VERIFIED, USE_PUBCHEM_CAS_VALIDATION
    from utils.pubchem_validator import get_pubchem_validator

    validator = get_pubchem_validator()
    row_by_cas = {r.get("cas", ""): r for r in rows}

    enriched: List[dict[str, Any]] = []
    for cas in cas_list:
        r = dict(row_by_cas.get(cas, {}))
        r["cas"] = cas

        if USE_PUBCHEM_CAS_VALIDATION:
            check = validator.validate(cas)
            r["pubchem_verified"] = check["exists"] is True
            r["pubchem_status"] = "verified" if check["exists"] is True else ("unknown" if check["exists"] is None else "not_found")
            if check.get("name") and not r.get("chemical_name"):
                r["chemical_name"] = check["name"]
            r["_pubchem_result"] = check
        else:
            r["pubchem_verified"] = None
            r["pubchem_status"] = "skipped"
            r["_pubchem_result"] = {"confidence_boost": 0.0}

        r["confidence"] = _calculate_confidence(r, r.get("_pubchem_result", {}))
        r.pop("_pubchem_result", None)

        # Filter invalid: only show PubChem-verified CAS when gate is on
        if SHOW_ONLY_PUBCHEM_VERIFIED and USE_PUBCHEM_CAS_VALIDATION and r["pubchem_verified"] is not True:
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

    def has_multiple_cas(self) -> bool:
        return len(self.cas_numbers) > 1


def _use_pure_cas_bert_pipeline() -> bool:
    """Session toggle (Streamlit) or HAZQUERY_PURE_CAS_BERT=1."""
    if os.getenv("HAZQUERY_PURE_CAS_BERT", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    try:
        return bool(st.session_state.get("use_pure_cas_bert"))
    except Exception:
        return False


def _use_dual_parser_crossref() -> bool:
    """Session toggle for dual parser + DB cross-reference."""
    if os.getenv("HAZQUERY_DUAL_PARSER", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    try:
        return bool(st.session_state.get("use_dual_parser_crossref"))
    except Exception:
        return False


class UnifiedInputHandler:
    """Routes text or uploaded PDF to a ChemicalInput."""

    def __init__(self) -> None:
        from utils.sds_parser import get_sds_parser

        self._sds_parser = get_sds_parser()

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
        Extract CAS list from SDS using unified parser.
        uploaded_file: Streamlit UploadedFile with .name and .getvalue().
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
        if _use_pure_cas_bert_pipeline():
            return self._process_sds_pdf_pure_bert(uploaded_file, pdf_bytes)
        if _use_dual_parser_crossref():
            return self._process_sds_pdf_dual_parser(uploaded_file, pdf_bytes)

        return self._process_sds_pdf_single(uploaded_file, pdf_bytes)

    def _process_sds_pdf_single(self, uploaded_file: Any, pdf_bytes: bytes) -> ChemicalInput:
        """Single parser (A only) - default path."""
        parse_result = self._sds_parser.parse_pdf(pdf_bytes)
        if not parse_result or not parse_result.cas_numbers:
            return ChemicalInput(
                input_type="sds_single",
                primary="",
                cas_numbers=[],
                source_label=getattr(uploaded_file, "name", None) or "SDS.pdf",
                extraction_rows=[],
            )

        rows: list[dict[str, Any]] = []
        cas_list: list[str] = []
        seen: set[str] = set()
        best: tuple[float, str] = (-1.0, "")
        for ext in parse_result.cas_numbers:
            c = (ext.cas or "").strip()
            if not c or c in seen:
                continue
            seen.add(c)
            cas_list.append(c)
            conf = float(ext.confidence) if ext.confidence is not None else 0.0
            if conf > best[0]:
                best = (conf, c)
            rows.append(
                {
                    "cas": c,
                    "chemical_name": ext.chemical_name or "",
                    "concentration": ext.concentration or "",
                    "section": ext.section,
                    "method": ext.method,
                    "confidence": ext.confidence,
                    "validated": ext.validated,
                    "context": ext.context or "",
                    "warnings": ", ".join(ext.warnings) if ext.warnings else "",
                }
            )

        cas_list, rows = _score_cas_confidence(cas_list, rows)
        primary = best[1] if best[1] and best[1] in cas_list else (cas_list[0] if cas_list else "")
        in_type = "sds_multi" if len(cas_list) > 1 else "sds_single"
        return ChemicalInput(
            input_type=in_type,
            primary=primary,
            cas_numbers=cas_list,
            source_label=getattr(uploaded_file, "name", None) or "SDS.pdf",
            extraction_rows=rows,
        )

    def _process_sds_pdf_pure_bert(self, uploaded_file: Any, pdf_bytes: bytes) -> Optional[ChemicalInput]:
        """Docling + DistilBERT only (see utils/cas_extractor.py)."""
        from utils.cas_extractor import get_pure_cas_extractor, is_pure_cas_bert_available
        from utils.sds_debug import sds_debug_log

        sds_debug_log(
            "input_handler.pure_bert",
            {"status": is_pure_cas_bert_available(), "filename": getattr(uploaded_file, "name", None)},
        )
        extractor = get_pure_cas_extractor()
        bert_rows = extractor.extract(pdf_bytes)
        sds_debug_log(
            "input_handler.pure_bert_result",
            {"n_rows": len(bert_rows), "cas_list": [r.cas for r in bert_rows]},
        )
        rows: list[dict[str, Any]] = []
        cas_list: list[str] = []
        seen: set[str] = set()
        best: tuple[float, str] = (-1.0, "")
        for r in bert_rows:
            c = (r.cas or "").strip()
            if not c or c in seen:
                continue
            seen.add(c)
            cas_list.append(c)
            conf = float(r.confidence)
            if conf > best[0]:
                best = (conf, c)
            rows.append(
                {
                    "cas": c,
                    "chemical_name": r.chemical_name or "",
                    "concentration": r.concentration or "",
                    "section": r.source_page,
                    "method": "docling_distilbert",
                    "confidence": r.confidence,
                    "validated": True,
                    "context": "",
                    "warnings": "",
                }
            )
        cas_list, rows = _score_cas_confidence(cas_list, rows)
        label = getattr(uploaded_file, "name", None) or "SDS.pdf"
        primary = best[1] if best[1] and best[1] in cas_list else (cas_list[0] if cas_list else "")
        in_type = "sds_multi" if len(cas_list) > 1 else "sds_single"
        return ChemicalInput(
            input_type=in_type if cas_list else "sds_single",
            primary=primary,
            cas_numbers=cas_list,
            source_label=label,
            extraction_rows=rows,
        )

    def _process_sds_pdf_dual_parser(self, uploaded_file: Any, pdf_bytes: bytes) -> Optional[ChemicalInput]:
        """Run both parsers (A + B), merge, cross-reference with DB and name validation."""
        try:
            from utils.sds_dual_parser import merge_and_cross_reference
        except ImportError:
            return None

        from utils.sds_debug import sds_debug_log

        sds_debug_log(
            "input_handler.dual_parser",
            {"filename": getattr(uploaded_file, "name", None), "bytes": len(pdf_bytes)},
        )
        try:
            cas_list, rows = merge_and_cross_reference(pdf_bytes, use_name_validation=True)
        except Exception as e:
            sds_debug_log("input_handler.dual_parser_error", {"error": str(e)})
            return self._process_sds_pdf_single(uploaded_file, pdf_bytes)
        sds_debug_log(
            "input_handler.dual_parser_result",
            {"n_cas": len(cas_list), "recognized": sum(1 for r in rows if r.get("recognized"))},
        )

        cas_list, rows = _score_cas_confidence(cas_list, rows)

        if not cas_list:
            return ChemicalInput(
                input_type="sds_single",
                primary="",
                cas_numbers=[],
                source_label=getattr(uploaded_file, "name", None) or "SDS.pdf",
                extraction_rows=[],
            )

        primary = cas_list[0]
        in_type = "sds_multi" if len(cas_list) > 1 else "sds_single"
        return ChemicalInput(
            input_type=in_type,
            primary=primary,
            cas_numbers=cas_list,
            source_label=getattr(uploaded_file, "name", None) or "SDS.pdf",
            extraction_rows=rows,
        )


@st.cache_resource
def get_input_handler() -> UnifiedInputHandler:
    return UnifiedInputHandler()
