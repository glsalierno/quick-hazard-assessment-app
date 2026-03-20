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

# --- Optional Docling (heavy deps: torch, models on first run) ---
_DOCLING_IMPORT_ERROR: Optional[str] = None
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
except ImportError as e:
    DocumentConverter = None  # type: ignore[misc, assignment]
    DocumentStream = None  # type: ignore[misc, assignment]
    InputFormat = None  # type: ignore[misc, assignment]
    PdfFormatOption = None  # type: ignore[misc, assignment]
    PdfPipelineOptions = None  # type: ignore[misc, assignment]
    _DOCLING_IMPORT_ERROR = str(e)


def is_docling_available() -> bool:
    return DocumentConverter is not None and not is_docling_disabled()


def is_docling_disabled() -> bool:
    return os.getenv("HAZQUERY_DISABLE_DOCLING", "").strip().lower() in ("1", "true", "yes", "on")


def docling_status_message() -> str:
    if is_docling_disabled():
        return "Docling disabled via HAZQUERY_DISABLE_DOCLING."
    if _DOCLING_IMPORT_ERROR:
        return f"Docling not installed ({_DOCLING_IMPORT_ERROR[:120]}…)."
    return "Docling available for PDF table extraction."


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


def build_docling_converter() -> Optional[Any]:
    """Create a DocumentConverter configured for SDS-style tables (no Streamlit)."""
    if not is_docling_available():
        return None
    assert PdfPipelineOptions is not None and PdfFormatOption is not None and InputFormat is not None

    try:
        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = True
        pipeline_options.do_table_structure = True
        try:
            pipeline_options.table_structure_options.do_cell_matching = True
        except Exception:
            pass
        if _HAS_TABLE_FORMER_MODE and TableFormerMode is not None:
            try:
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


def extract_composition_from_pdf(pdf_bytes: bytes, *, use_cache: bool = True) -> list[CASExtraction]:
    """
    Run Docling on PDF bytes and return composition rows (CAS + name + concentration when present).
    """
    if not pdf_bytes or is_docling_disabled() or not is_docling_available():
        return []

    converter = get_cached_docling_converter() if use_cache else build_docling_converter()
    if converter is None or DocumentStream is None:
        return []

    buf = BytesIO(pdf_bytes)
    source = DocumentStream(name="sds.pdf", stream=buf)
    try:
        conv_res = converter.convert(source)
    except Exception as e:
        logger.warning("Docling convert failed: %s", e)
        return []

    doc = conv_res.document
    if not getattr(doc, "tables", None):
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

    return components
