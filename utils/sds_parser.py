"""
Unified SDS Parser public interface.
"""

from __future__ import annotations

import traceback
from typing import Optional

import streamlit as st

from utils import cas_validator, sds_pdf_utils
from utils.sds_environment import EnvironmentDetector
from utils.sds_models import CASExtraction, SDSParseResult
from utils.sds_parser_engine import SDSParserEngine


def _merge_docling_cas_extractions(result: SDSParseResult, pdf_bytes: bytes) -> None:
    """Prepend / enrich composition rows from Docling table export when available."""
    from utils import docling_sds_parser
    from utils.sds_debug import cas_rows_brief, sds_debug_log

    n_before = len(result.cas_numbers)
    extra = docling_sds_parser.extract_composition_from_pdf(pdf_bytes)
    if not extra:
        sds_debug_log("merge.docling", {"n_docling": 0, "n_before": n_before, "skipped": True})
        return

    def _key(c: str) -> str:
        return cas_validator.normalize_cas_input(c) or c

    doc_by: dict[str, CASExtraction] = {_key(x.cas): x for x in extra}
    for old in result.cas_numbers:
        k = _key(old.cas)
        if k in doc_by:
            d = doc_by[k]
            if not d.chemical_name and old.chemical_name:
                d.chemical_name = old.chemical_name
            if not d.concentration and old.concentration:
                d.concentration = old.concentration
            d.confidence = max(float(d.confidence or 0), float(old.confidence or 0))
            d.validated = bool(d.validated or old.validated)

    merged = list(doc_by.values())
    seen = set(doc_by.keys())
    for old in result.cas_numbers:
        k = _key(old.cas)
        if k not in seen:
            merged.append(old)
            seen.add(k)
    result.cas_numbers = merged
    result.methods_used = sorted(set([*result.methods_used, "docling"]))
    sds_debug_log(
        "merge.docling",
        {
            "n_docling_rows": len(extra),
            "n_before": n_before,
            "n_after": len(merged),
            "docling_sample": cas_rows_brief(extra),
        },
    )


class SDSParser:
    def __init__(self) -> None:
        self.engine = SDSParserEngine()
        self.env = EnvironmentDetector.detect()

    def parse_pdf(self, pdf_bytes: bytes) -> Optional[SDSParseResult]:
        from utils.sds_debug import sds_debug_log

        try:
            sds_debug_log("parse_pdf.start", {"pdf_bytes": len(pdf_bytes or b"")})
            text = sds_pdf_utils.extract_text_from_pdf_bytes(pdf_bytes)
            text = sds_pdf_utils.normalize_whitespace(text)
            sds_debug_log(
                "parse_pdf.text_extracted",
                {"text_len": len(text or ""), "head": (text or "")[:800]},
            )
            if not (text or "").strip():
                sds_debug_log("parse_pdf.abort", {"reason": "no_text"})
                return None
            result = self.engine.parse(text)
            if pdf_bytes:
                _merge_docling_cas_extractions(result, pdf_bytes)
            return result
        except Exception as e:
            sds_debug_log(
                "parse_pdf.exception",
                {"error": str(e), "traceback": traceback.format_exc()},
            )
            st.error(f"Parsing error: {e}")
            return None

    def get_capability_message(self) -> str:
        base = EnvironmentDetector.get_capability_message()
        try:
            from utils import docling_sds_parser

            if docling_sds_parser.is_docling_available():
                return base + "\n\n- ✅ **Docling** enabled (PDF composition tables; first run may download models)."
            return base + f"\n\n- ℹ️ **Docling:** {docling_sds_parser.docling_status_message()}"
        except Exception:
            return base


@st.cache_resource
def get_sds_parser() -> SDSParser:
    return SDSParser()
