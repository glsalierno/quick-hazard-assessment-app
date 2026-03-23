"""
Optional SDS composition extraction using IBM Docling (local PDF + table structure).

Falls back silently when Docling is not installed or ``HAZQUERY_DISABLE_DOCLING`` is set.
"""

from __future__ import annotations

import logging
import os
import re
from io import BytesIO
from typing import Any, Optional

from utils import cas_validator
from utils.sds_models import CASExtraction

logger = logging.getLogger(__name__)

# --- Optional Docling (lazy import to avoid cv2/opencv at startup on Streamlit Cloud) ---
_DOCLING_IMPORT_ERROR: Optional[str] = None
_DOCLING_CACHE: Optional[dict[str, Any]] = None


def _try_import_docling() -> dict[str, Any]:
    """Lazy import docling. Returns dict with keys or None values. Only called when needed."""
    global _DOCLING_CACHE, _DOCLING_IMPORT_ERROR
    if _DOCLING_CACHE is not None:
        return _DOCLING_CACHE
    try:
        from docling.datamodel.base_models import DocumentStream, InputFormat
        from docling.document_converter import DocumentConverter, PdfFormatOption
        from docling.datamodel.pipeline_options import PdfPipelineOptions

        try:
            from docling.datamodel.pipeline_options import TableFormerMode
            _HAS_TABLE_FORMER_MODE = True
        except ImportError:
            TableFormerMode = None  # type: ignore[misc, assignment]
            _HAS_TABLE_FORMER_MODE = False

        _DOCLING_IMPORT_ERROR = None
        _DOCLING_CACHE = {
            "DocumentConverter": DocumentConverter,
            "DocumentStream": DocumentStream,
            "InputFormat": InputFormat,
            "PdfFormatOption": PdfFormatOption,
            "PdfPipelineOptions": PdfPipelineOptions,
            "TableFormerMode": TableFormerMode,
            "_HAS_TABLE_FORMER_MODE": _HAS_TABLE_FORMER_MODE,
        }
        return _DOCLING_CACHE
    except ImportError as e:
        _DOCLING_IMPORT_ERROR = str(e)
        _DOCLING_CACHE = {
            "DocumentConverter": None,
            "DocumentStream": None,
            "InputFormat": None,
            "PdfFormatOption": None,
            "PdfPipelineOptions": None,
            "TableFormerMode": None,
            "_HAS_TABLE_FORMER_MODE": False,
        }
        return _DOCLING_CACHE


def is_docling_available() -> bool:
    if is_docling_disabled():
        return False
    d = _try_import_docling()
    return d["DocumentConverter"] is not None


def is_docling_disabled() -> bool:
    return os.getenv("HAZQUERY_DISABLE_DOCLING", "").strip().lower() in ("1", "true", "yes", "on")


def docling_status_message() -> str:
    if is_docling_disabled():
        return "Docling disabled via HAZQUERY_DISABLE_DOCLING."
    d = _try_import_docling()
    if _DOCLING_IMPORT_ERROR:
        return f"Docling not installed ({_DOCLING_IMPORT_ERROR[:120]}…)."
    if d["DocumentConverter"] is None:
        return "Docling not installed."
    return "Docling available for PDF table extraction."


def __getattr__(name: str) -> Any:
    """Lazy export so ``from utils.docling_sds_parser import DocumentStream`` works for cas_extractor."""
    if name == "DocumentStream":
        return _try_import_docling().get("DocumentStream")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _validate_cas_checksum(cas: str) -> bool:
    if not cas or not re.match(r"^\d{1,7}-\d{2}-\d$", cas):
        return False
    try:
        a, b, c = cas.split("-")
        main = a + b
        check = int(c)
        total = 0
        for i, d in enumerate(reversed(main), 1):
            total += int(d) * i
        return (total % 10) == check
    except Exception:
        return False


_CAS_RE = re.compile(r"\b(\d{1,7})-(\d{2})-(\d)\b")


def _clean_cas(text: str) -> Optional[str]:
    if not text:
        return None
    m = _CAS_RE.search(str(text))
    if not m:
        return None
    cas = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return cas_validator.normalize_cas_input(cas) or cas


def _extract_concentration(text: str) -> Optional[str]:
    if not text:
        return None
    t = str(text).strip()
    range_pct = re.search(
        r"(?:([<>]=?)\s*)?(\d+(?:\.\d+)?)\s*[-–]\s*(?:([<>]=?)\s*)?(\d+(?:\.\d+)?)\s*[%％]?",
        t,
        re.IGNORECASE,
    )
    if range_pct:
        lo, lv, ho, hv = range_pct.group(1) or "", range_pct.group(2), range_pct.group(3) or "", range_pct.group(4)
        left = f"{lo} {lv}".strip() if lo else lv
        right = f"{ho} {hv}".strip() if ho else hv
        return f"{left} - {right}%"
    sm = re.search(r"(\d+(?:\.\d+)?)\s*[%％]", t)
    if sm:
        return f"{sm.group(1)}%"
    return t if t else None


def _table_composition_score(headers_lower: list[str]) -> int:
    joined = " ".join(headers_lower)
    score = 0
    if "cas" in joined or "registry" in joined:
        score += 3
    if any(x in joined for x in ("chemical", "component", "ingredient", "substance", "name")):
        score += 2
    if any(x in joined for x in ("%", "percent", "wt", "weight", "concentration")):
        score += 2
    return score


def _dataframe_to_cas_extractions(df: Any, table_index: int) -> list[CASExtraction]:
    """Parse a Docling-exported DataFrame for composition rows."""
    out: list[CASExtraction] = []
    if df is None or getattr(df, "empty", True):
        return out
    try:
        headers = [str(c).strip().lower() for c in df.columns]
    except Exception:
        return out
    if _table_composition_score(headers) < 2 and len(df.columns) < 2:
        return out

    cas_col = name_col = conc_col = None
    for i, h in enumerate(headers):
        if any(t in h for t in ("cas", "cas-no", "cas no", "cas number", "registry")):
            cas_col = i
        elif any(t in h for t in ("chemical", "component", "ingredient", "substance", "name", "product")):
            name_col = i
        elif any(t in h for t in ("wt", "weight", "concentration", "%", "percent", "amount")):
            conc_col = i

    # Heuristic: three unnamed / generic columns often = name, CAS, %
    if cas_col is None and len(df.columns) >= 3:
        cas_col, name_col, conc_col = 1, 0, 2

    for _, row in df.iterrows():
        try:
            cells = [row.iloc[i] if i < len(row) else "" for i in range(len(df.columns))]
        except Exception:
            continue
        cas_raw = str(cells[cas_col]) if cas_col is not None and cas_col < len(cells) else ""
        cas = _clean_cas(cas_raw)
        if not cas:
            for cell in cells:
                cas = _clean_cas(str(cell))
                if cas:
                    break
        if not cas or not _validate_cas_checksum(cas):
            continue
        name = ""
        if name_col is not None and name_col < len(cells):
            name = str(cells[name_col]).strip()
        conc_raw = str(cells[conc_col]) if conc_col is not None and conc_col < len(cells) else ""
        conc = _extract_concentration(conc_raw) or (conc_raw.strip() or None)
        ctx = " | ".join(str(c) for c in cells)[:400]
        out.append(
            CASExtraction(
                cas=cas,
                chemical_name=name or None,
                concentration=conc,
                section=3,
                method="docling_table",
                confidence=0.97,
                context=ctx,
                validated=True,
            )
        )
    return out


def build_docling_converter(*, low_memory: bool = False) -> Optional[Any]:
    """
    Create a DocumentConverter configured for SDS-style tables (no Streamlit).

    ``low_memory=True`` (or env ``HAZQUERY_DOCLING_LOW_MEMORY=1``) disables OCR and uses
    faster table structure to reduce RAM / avoid ``std::bad_alloc`` on large pages.
    """
    d = _try_import_docling()
    DocumentConverter = d["DocumentConverter"]
    PdfPipelineOptions = d["PdfPipelineOptions"]
    PdfFormatOption = d["PdfFormatOption"]
    InputFormat = d["InputFormat"]
    TableFormerMode = d["TableFormerMode"]
    _HAS_TABLE_FORMER_MODE = d["_HAS_TABLE_FORMER_MODE"]
    if not DocumentConverter or not PdfPipelineOptions or not PdfFormatOption or not InputFormat:
        return None

    lm = low_memory or os.getenv("HAZQUERY_DOCLING_LOW_MEMORY", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    try:
        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = not lm
        pipeline_options.do_table_structure = True
        try:
            pipeline_options.table_structure_options.do_cell_matching = True
        except Exception:
            pass
        for attr in ("generate_page_images", "generate_picture_images"):
            if hasattr(pipeline_options, attr):
                try:
                    setattr(pipeline_options, attr, False)
                except Exception:
                    pass
        if _HAS_TABLE_FORMER_MODE and TableFormerMode is not None:
            try:
                fast_mode = getattr(TableFormerMode, "FAST", None)
                if lm and fast_mode is not None:
                    pipeline_options.table_structure_options.mode = fast_mode
                else:
                    pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE
            except Exception:
                pass

        return DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options),
            }
        )
    except Exception as e:
        logger.warning("Docling DocumentConverter init failed: %s", e)
        return None


_batch_low_memory_converter: Optional[Any] = None


def get_batch_low_memory_converter() -> Optional[Any]:
    """Singleton converter for batch/CLI (OCR off, FAST tables) — lower peak RAM."""
    global _batch_low_memory_converter
    if _batch_low_memory_converter is None:
        _batch_low_memory_converter = build_docling_converter(low_memory=True)
    return _batch_low_memory_converter


def reset_batch_low_memory_converter() -> None:
    """Release singleton (e.g. after long batch) to free native heap."""
    global _batch_low_memory_converter
    _batch_low_memory_converter = None


def get_cached_docling_converter() -> Optional[Any]:
    """Streamlit-cached converter (expensive to construct)."""
    try:
        import streamlit as st

        @st.cache_resource(show_spinner=False)
        def _make() -> Optional[Any]:
            return build_docling_converter()

        return _make()
    except Exception:
        return build_docling_converter()


def extract_composition_from_pdf(
    pdf_bytes: bytes,
    *,
    use_cache: bool = True,
    low_memory: bool = False,
    converter: Optional[Any] = None,
) -> list[CASExtraction]:
    """
    Run Docling on PDF bytes and return composition rows (CAS + name + concentration when present).
    """
    from utils.sds_debug import cas_rows_brief, sds_debug_log

    if not pdf_bytes:
        sds_debug_log("docling.skip", {"reason": "empty_bytes"})
        return []
    if is_docling_disabled():
        sds_debug_log("docling.skip", {"reason": "HAZQUERY_DISABLE_DOCLING"})
        return []
    if not is_docling_available():
        sds_debug_log("docling.skip", {"reason": "not_installed", "detail": _DOCLING_IMPORT_ERROR})
        return []

    if converter is not None:
        pass
    elif low_memory:
        converter = get_batch_low_memory_converter()
    elif use_cache:
        converter = get_cached_docling_converter()
    else:
        converter = build_docling_converter()
    DocumentStream = _try_import_docling().get("DocumentStream")
    if converter is None or DocumentStream is None:
        sds_debug_log("docling.skip", {"reason": "converter_init_failed"})
        return []

    buf = BytesIO(pdf_bytes)
    source = DocumentStream(name="sds.pdf", stream=buf)
    try:
        conv_res = converter.convert(source)
    except Exception as e:
        logger.warning("Docling convert failed: %s", e)
        sds_debug_log("docling.error", {"error": str(e)})
        return []

    doc = conv_res.document
    if not getattr(doc, "tables", None):
        sds_debug_log("docling.no_tables", {"message": "document.tables empty or missing"})
        return []

    scored: list[tuple[int, int, Any]] = []
    for table_ix, table in enumerate(doc.tables):
        try:
            try:
                table_df = table.export_to_dataframe(doc=doc)
            except TypeError:
                table_df = table.export_to_dataframe()
        except Exception as e:
            logger.debug("Docling table %s export failed: %s", table_ix, e)
            continue
        try:
            headers = [str(c).strip().lower() for c in table_df.columns]
        except Exception:
            headers = []
        sc = _table_composition_score(headers)
        scored.append((sc, table_ix, table_df))

    scored.sort(key=lambda x: (-x[0], x[1]))
    components: list[CASExtraction] = []
    seen: set[str] = set()

    for sc, _ix, table_df in scored:
        if sc < 2 and len(scored) > 1:
            continue
        for ext in _dataframe_to_cas_extractions(table_df, _ix):
            k = cas_validator.normalize_cas_input(ext.cas) or ext.cas
            if k in seen:
                continue
            seen.add(k)
            components.append(ext)

    if not components:
        for table_ix, table in enumerate(doc.tables):
            try:
                try:
                    table_df = table.export_to_dataframe(doc=doc)
                except TypeError:
                    table_df = table.export_to_dataframe()
            except Exception:
                continue
            for ext in _dataframe_to_cas_extractions(table_df, table_ix):
                k = cas_validator.normalize_cas_input(ext.cas) or ext.cas
                if k in seen:
                    continue
                seen.add(k)
                components.append(ext)

    table_summaries = []
    for table_ix, table in enumerate(doc.tables):
        try:
            try:
                table_df = table.export_to_dataframe(doc=doc)
            except TypeError:
                table_df = table.export_to_dataframe()
            table_summaries.append(
                {
                    "ix": table_ix,
                    "shape": [int(table_df.shape[0]), int(table_df.shape[1])],
                    "columns": [str(c) for c in table_df.columns][:20],
                    "score": _table_composition_score([str(c).strip().lower() for c in table_df.columns]),
                }
            )
        except Exception as ex:
            table_summaries.append({"ix": table_ix, "error": str(ex)})

    sds_debug_log(
        "docling.result",
        {
            "n_tables": len(doc.tables),
            "n_components": len(components),
            "table_summaries": table_summaries[:25],
            "rows": cas_rows_brief(components),
        },
    )
    return components
