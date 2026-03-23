"""
SDS CAS extraction for Streamlit: **MarkItDown + regex** and **Hybrid** (MarkItDown → OCR fallback).

Older parsers were removed from the product surface; see ``docs/SDS_EXTRACTION_PIPELINES.md``.
"""

from __future__ import annotations

import os
import time
from typing import Any, Optional

from utils.cache_manager import ExtractionCacheManager, default_cache_manager, pdf_fingerprint
from utils import cas_text_extract

# --- Supported pipeline IDs (sidebar / env) ---
PIPELINE_MARKITDOWN_FAST = "markitdown_fast"
PIPELINE_HYBRID_MD_OCR = "hybrid_md_ocr"

SUPPORTED_SDS_PIPELINES: frozenset[str] = frozenset({
    PIPELINE_MARKITDOWN_FAST,
    PIPELINE_HYBRID_MD_OCR,
})

# Legacy env/session/bookmark values → map onto a supported pipeline
_LEGACY_SDS_PIPELINE_MAP: dict[str, str] = {
    "default": PIPELINE_HYBRID_MD_OCR,
    "markitdown_bert": PIPELINE_MARKITDOWN_FAST,
    "ocr_tesseract": PIPELINE_HYBRID_MD_OCR,
    "ocr_easyocr": PIPELINE_HYBRID_MD_OCR,
    "docling_bert": PIPELINE_HYBRID_MD_OCR,
    "pdfplumber_regex": PIPELINE_HYBRID_MD_OCR,
}

PIPELINE_LABELS: dict[str, str] = {
    PIPELINE_HYBRID_MD_OCR: "Hybrid (recommended): MarkItDown + regex → OCR if no CAS",
    PIPELINE_MARKITDOWN_FAST: "MarkItDown + regex only (fast; best on text/table PDFs)",
}

PIPELINE_SIDEBAR_ORDER: list[str] = [
    PIPELINE_HYBRID_MD_OCR,
    PIPELINE_MARKITDOWN_FAST,
]


def normalize_sds_pipeline_mode(mode: str) -> str:
    """Map legacy pipeline names onto ``SUPPORTED_SDS_PIPELINES``; unknown → hybrid."""
    m = (mode or "").strip()
    if m in SUPPORTED_SDS_PIPELINES:
        return m
    return _LEGACY_SDS_PIPELINE_MAP.get(m, PIPELINE_HYBRID_MD_OCR)


def get_extraction_pipeline_mode() -> str:
    """
    Active SDS pipeline: env ``HAZQUERY_EXTRACTION_PIPELINE``, then Streamlit session,
    then ``HAZQUERY_DEFAULT_SDS_PIPELINE`` / ``config.DEFAULT_SDS_EXTRACTION_PIPELINE``.
    Always returns a supported id (``markitdown_fast`` or ``hybrid_md_ocr``).
    """
    raw = ""
    env = (os.environ.get("HAZQUERY_EXTRACTION_PIPELINE") or "").strip()
    if env:
        raw = env
    else:
        try:
            import streamlit as st

            v = st.session_state.get("sds_extraction_pipeline")
            if v:
                raw = str(v)
        except Exception:
            pass
    if not raw:
        raw = (os.environ.get("HAZQUERY_DEFAULT_SDS_PIPELINE") or "").strip()
    if not raw:
        try:
            import config

            raw = str(getattr(config, "DEFAULT_SDS_EXTRACTION_PIPELINE", "") or "").strip()
        except Exception:
            pass
    if not raw:
        raw = PIPELINE_HYBRID_MD_OCR
    return normalize_sds_pipeline_mode(raw)


def get_cache_behavior() -> str:
    """use | force | clear_once"""
    env = (os.environ.get("HAZQUERY_EXTRACTION_CACHE") or "").strip().lower()
    if env in ("use", "force", "clear_once"):
        return env
    try:
        import streamlit as st

        v = st.session_state.get("pdf_cache_behavior")
        if v in ("use", "force", "clear_once"):
            return str(v)
    except Exception:
        pass
    return "use"


def get_ocr_engine_choice() -> str:
    """tesseract | easyocr | llm_vision (reserved)."""
    try:
        import streamlit as st

        v = st.session_state.get("sds_ocr_engine")
        if v:
            return str(v)
    except Exception:
        pass
    return os.environ.get("HAZQUERY_OCR_ENGINE", "tesseract").strip() or "tesseract"


def get_tesseract_psm() -> int:
    try:
        import streamlit as st

        v = st.session_state.get("sds_tesseract_psm")
        if v is not None:
            return int(v)
    except Exception:
        pass
    return int(os.environ.get("HAZQUERY_TESSERACT_PSM", "6") or "6")


def _cache_for_run(behavior: str) -> tuple[ExtractionCacheManager, bool]:
    mgr = default_cache_manager()
    mgr.ensure_root()
    if behavior == "clear_once":
        mgr.clear_all()
        try:
            import streamlit as st

            st.session_state["pdf_cache_behavior"] = "use"
        except Exception:
            pass
    force = behavior == "force"
    return mgr, force


def extraction_rows_from_cas_list(
    cas_list: list[str],
    *,
    method: str,
    default_confidence: float = 0.82,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cas in cas_list:
        rows.append(
            {
                "cas": cas,
                "chemical_name": "",
                "concentration": "",
                "section": None,
                "sections": [],
                "method": method,
                "confidence": default_confidence,
                "validated": True,
                "context": "",
                "warnings": "",
            }
        )
    return rows


def run_markitdown_pipeline(
    pdf_bytes: bytes,
    *,
    use_bert: bool,
    cache: ExtractionCacheManager,
    force_cache: bool,
) -> tuple[list[str], list[dict[str, Any]], str]:
    """Returns (cas_list, detail_rows, markdown_text)."""
    from utils.markitdown_check import require_markitdown

    require_markitdown()
    from parsers.markitdown_parser import MarkItDownParser

    fp = pdf_fingerprint(pdf_bytes)
    bert = None
    if use_bert:
        try:
            from utils.cas_extractor import get_pure_cas_extractor

            bert = get_pure_cas_extractor()
        except Exception:
            bert = None

    parser = MarkItDownParser(use_bert=use_bert, bert_extractor=bert, cache=cache)
    md_text = parser.convert_pdf_to_markdown(pdf_bytes, fp, force=force_cache)
    cas_list, details = parser.extract_cas_from_markdown(md_text)
    seen: set[str] = set()
    out_cas: list[str] = []
    for c in cas_list:
        if c not in seen:
            seen.add(c)
            out_cas.append(c)
    return out_cas, details, md_text


def run_ocr_pipeline(
    pdf_bytes: bytes,
    *,
    engine: str,
    cache: ExtractionCacheManager,
    force_cache: bool,
    psm: int = 6,
    easyocr_reader: Any = None,
    ocr_dpi: Optional[int] = None,
) -> tuple[list[str], str]:
    from parsers import ocr_pipeline

    fp = pdf_fingerprint(pdf_bytes)
    if engine == "easyocr":
        text = ocr_pipeline.ocr_pdf_with_cache(
            pdf_bytes,
            "easyocr",
            fp,
            cache,
            force=force_cache,
            tesseract_psm=psm,
            easyocr_reader=easyocr_reader,
            dpi=ocr_dpi,
        )
        method = "ocr_easyocr_regex"
    else:
        text = ocr_pipeline.ocr_pdf_with_cache(
            pdf_bytes,
            "tesseract",
            fp,
            cache,
            force=force_cache,
            tesseract_psm=psm,
            dpi=ocr_dpi,
        )
        method = "ocr_tesseract_regex"
    cas_list = cas_text_extract.find_checksum_valid_cas_in_text(text)
    return cas_list, method


def run_hybrid_pipeline(
    pdf_bytes: bytes,
    *,
    use_bert: bool,
    cache: ExtractionCacheManager,
    force_cache: bool,
    psm: int,
    ocr_dpi: Optional[int] = None,
    ocr_engine: Optional[str] = None,
) -> tuple[list[str], str]:
    """MarkItDown + regex first; if zero CAS, OCR + regex (engine from sidebar or Tesseract)."""
    cas_md, _, _ = run_markitdown_pipeline(
        pdf_bytes, use_bert=use_bert, cache=cache, force_cache=force_cache
    )
    if cas_md:
        return cas_md, "markitdown_hybrid_primary"
    chosen = (ocr_engine or get_ocr_engine_choice() or "tesseract").strip().lower()
    if chosen in ("llm_vision", ""):
        chosen = "tesseract"
    if chosen not in ("tesseract", "easyocr"):
        chosen = "tesseract"
    cas_ocr, meth = run_ocr_pipeline(
        pdf_bytes,
        engine=chosen,
        cache=cache,
        force_cache=force_cache,
        psm=psm,
        ocr_dpi=ocr_dpi,
    )
    return cas_ocr, "ocr_hybrid_fallback" if cas_ocr else meth


def run_pipeline_with_metrics(
    pdf_bytes: bytes,
    pipeline: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Execute MarkItDown or Hybrid pipeline and return (extraction_rows, metrics).
    """
    pipeline = normalize_sds_pipeline_mode(pipeline)
    behavior = get_cache_behavior()
    cache, force = _cache_for_run(behavior)
    t0 = time.perf_counter()
    metrics: dict[str, Any] = {
        "pipeline": pipeline,
        "cache_behavior": behavior,
        "wall_time_sec": 0.0,
        "raw_cas_like_count": 0,
        "checksum_valid_count": 0,
    }

    cas_list: list[str] = []
    method = "unknown"

    try:
        if pipeline == PIPELINE_MARKITDOWN_FAST:
            cas_list, _, md = run_markitdown_pipeline(
                pdf_bytes, use_bert=False, cache=cache, force_cache=force
            )
            method = "markitdown_regex"
            metrics["markdown_chars"] = len(md or "")
        elif pipeline == PIPELINE_HYBRID_MD_OCR:
            cas_list, method = run_hybrid_pipeline(
                pdf_bytes,
                use_bert=False,
                cache=cache,
                force_cache=force,
                psm=get_tesseract_psm(),
            )
        else:
            # Should not happen after normalize; run hybrid as safe default
            cas_list, method = run_hybrid_pipeline(
                pdf_bytes,
                use_bert=False,
                cache=cache,
                force_cache=force,
                psm=get_tesseract_psm(),
            )
            metrics["pipeline"] = PIPELINE_HYBRID_MD_OCR
    except Exception as e:
        metrics["error"] = str(e)
        metrics["wall_time_sec"] = time.perf_counter() - t0
        return [], metrics

    metrics["checksum_valid_count"] = len(cas_list)
    metrics["raw_cas_like_count"] = len(cas_list)
    metrics["wall_time_sec"] = time.perf_counter() - t0

    rows = extraction_rows_from_cas_list(cas_list, method=method)
    try:
        fp = pdf_fingerprint(pdf_bytes)
        cache.save_json(
            fp,
            "cas_results.json",
            {
                "pipeline": pipeline,
                "cas": cas_list,
                "method": method,
                "metrics": metrics,
            },
        )
    except Exception:
        pass
    return rows, metrics
