"""
SDS CAS extraction for Streamlit: **MarkItDown + regex**, **Hybrid** (MarkItDown → OCR fallback),
and optional **MarkItDown → regex + GLiNER2** (see ``requirements-gliner2.txt``).

See ``docs/SDS_EXTRACTION_PIPELINES.md`` for pipeline details.
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
# v1.5: MarkItDown → Markdown → **regex CAS / H-codes** + optional **GLiNER2** structured extract (local).
PIPELINE_MARKDOWN_GLINER_REGEX = "markdown_gliner_regex"

SUPPORTED_SDS_PIPELINES: frozenset[str] = frozenset({
    PIPELINE_MARKITDOWN_FAST,
    PIPELINE_HYBRID_MD_OCR,
    PIPELINE_MARKDOWN_GLINER_REGEX,
})

# Legacy env/session/bookmark values → map onto a supported pipeline
_LEGACY_SDS_PIPELINE_MAP: dict[str, str] = {
    "default": PIPELINE_HYBRID_MD_OCR,
    "markitdown_bert": PIPELINE_MARKITDOWN_FAST,
    "ocr_tesseract": PIPELINE_HYBRID_MD_OCR,
    "ocr_easyocr": PIPELINE_HYBRID_MD_OCR,
    "docling_bert": PIPELINE_HYBRID_MD_OCR,
    "pdfplumber_regex": PIPELINE_HYBRID_MD_OCR,
    "gliner2": PIPELINE_MARKDOWN_GLINER_REGEX,
    "markdown_gliner": PIPELINE_MARKDOWN_GLINER_REGEX,
}

PIPELINE_LABELS: dict[str, str] = {
    PIPELINE_HYBRID_MD_OCR: "Hybrid (recommended): MarkItDown + regex → OCR if no CAS",
    PIPELINE_MARKITDOWN_FAST: "MarkItDown + regex only (fast; best on text/table PDFs)",
    PIPELINE_MARKDOWN_GLINER_REGEX: "Parse-then-extract: MarkItDown → regex + optional GLiNER2",
}

PIPELINE_SIDEBAR_ORDER: list[str] = [
    PIPELINE_HYBRID_MD_OCR,
    PIPELINE_MARKITDOWN_FAST,
    PIPELINE_MARKDOWN_GLINER_REGEX,
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
    Always returns a supported id (``markitdown_fast``, ``hybrid_md_ocr``, or ``markdown_gliner_regex``).
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


def run_markdown_gliner_regex_pipeline(
    pdf_bytes: bytes,
    *,
    cache: ExtractionCacheManager,
    force_cache: bool,
) -> tuple[list[str], str, dict[str, Any]]:
    """
    Stage 1: MarkItDown → Markdown. Stage 2: regex CAS + H-codes on markdown; optional GLiNER2
    ``extract_json`` merge for CAS (and diagnostics). No OCR in this pipeline.
    """
    from utils import sds_gliner_extract

    extra: dict[str, Any] = {
        "stage2": PIPELINE_MARKDOWN_GLINER_REGEX,
        "gliner2_installed": sds_gliner_extract.gliner2_is_installed(),
        "gliner2_runtime_enabled": sds_gliner_extract.gliner2_runtime_enabled(),
        "gliner2_used": False,
        "regex_cas": [],
        "gliner_cas": [],
        "gliner_wall_time_sec": None,
        "gliner_error": None,
        "gliner_raw_preview": None,
    }
    _, _, md = run_markitdown_pipeline(
        pdf_bytes, use_bert=False, cache=cache, force_cache=force_cache
    )
    md = md or ""
    extra["markdown_chars"] = len(md)
    regex_cas = cas_text_extract.find_checksum_valid_cas_in_text(md)
    h_codes = sds_gliner_extract.extract_h_codes_regex(md)
    p_codes = sds_gliner_extract.extract_p_codes_regex(md)
    props = sds_gliner_extract.extract_properties_regex(md)
    extra["regex_cas"] = list(regex_cas)
    extra["h_codes_regex"] = h_codes[:100]
    extra["h_codes_regex_count"] = len(h_codes)
    extra["p_codes_regex"] = p_codes[:100]
    extra["p_codes_regex_count"] = len(p_codes)
    extra["properties_regex"] = {k: v for k, v in props.items() if not str(k).startswith("_")}
    extra["regex_cas_count"] = len(regex_cas)

    sources: dict[str, str] = {c: "regex" for c in regex_cas}
    gliner_cas: list[str] = []

    if sds_gliner_extract.gliner2_is_installed() and sds_gliner_extract.gliner2_runtime_enabled():
        try:
            gout = sds_gliner_extract.extract_sds_fields_gliner2(md)
            extra["gliner_wall_time_sec"] = gout.get("wall_time_sec")
            extra["gliner_error"] = gout.get("error")
            raw = gout.get("raw")
            if raw is not None:
                try:
                    import json

                    raw_s = json.dumps(raw, ensure_ascii=False, default=str)
                    extra["gliner_raw_preview"] = raw_s[:4000] + ("…" if len(raw_s) > 4000 else "")
                except Exception:
                    extra["gliner_raw_preview"] = str(type(raw).__name__)
            gliner_cas = list(gout.get("cas_numbers") or [])
            extra["gliner2_used"] = True
        except Exception as exc:
            extra["gliner_error"] = str(exc)
            extra["gliner2_used"] = False
    extra["gliner_cas"] = list(gliner_cas)
    extra["gliner_cas_count"] = len(gliner_cas)

    merged: list[str] = []
    seen: set[str] = set()
    for c in regex_cas:
        if c not in seen:
            seen.add(c)
            merged.append(c)
    for c in gliner_cas:
        if c not in seen:
            sources[c] = "gliner"
            seen.add(c)
            merged.append(c)
        else:
            sources[c] = "both"
    extra["merged_cas_count"] = len(merged)
    extra["cas_source_by_cas"] = {k: sources.get(k, "?") for k in merged}
    return merged, "markitdown_gliner_regex", extra


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
        elif pipeline == PIPELINE_MARKDOWN_GLINER_REGEX:
            cas_list, method, gmetrics = run_markdown_gliner_regex_pipeline(
                pdf_bytes, cache=cache, force_cache=force
            )
            metrics.update(gmetrics)
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

    if pipeline == PIPELINE_MARKDOWN_GLINER_REGEX and cas_list:
        src_map = metrics.get("cas_source_by_cas") or {}
        rows = []
        for cas in cas_list:
            rows.append(
                {
                    "cas": cas,
                    "chemical_name": "",
                    "concentration": "",
                    "section": None,
                    "sections": [],
                    "method": method,
                    "confidence": 0.85 if src_map.get(cas) in ("both", "regex") else 0.78,
                    "validated": True,
                    "context": "",
                    "warnings": "",
                    "cas_source": src_map.get(cas, ""),
                }
            )
    else:
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
