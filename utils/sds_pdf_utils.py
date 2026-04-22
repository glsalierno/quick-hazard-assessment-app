"""
Extract text from SDS PDFs: embedded text first, then OCR (Tesseract + EasyOCR fallback).

- Text-based PDFs: pypdf extraction.
- Scanned or image-heavy PDFs: PDF → images (pdf2image) → Tesseract OCR; if a page
  yields little text, EasyOCR is used for that page when available.
- Optional: ocrmypdf to produce searchable PDFs (writes a new file with text layer).
"""

from __future__ import annotations

import logging
import os
import re
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

from pypdf import PdfReader

logger = logging.getLogger(__name__)


def poppler_kwargs_for_pdf2image() -> dict[str, Any]:
    """
    Extra kwargs for ``pdf2image.convert_from_bytes`` when Poppler is not on PATH.

    On Windows, download Poppler from
    https://github.com/oschwartz10612/poppler-windows/releases/ and set **either**:

    - Add ``.../poppler-xx/Library/bin`` to system **PATH**, **or**
    - Set env ``HAZQUERY_POPPLER_PATH`` or ``POPPLER_PATH`` to that **bin** folder
      (the directory that contains ``pdfinfo.exe`` and ``pdftoppm.exe``).
    """
    for key in ("HAZQUERY_POPPLER_PATH", "POPPLER_PATH"):
        raw = (os.environ.get(key) or "").strip().strip('"')
        if not raw:
            continue
        p = Path(raw)
        if p.is_dir():
            return {"poppler_path": str(p)}
        logger.warning("Poppler path from %s is not a directory: %s", key, raw)
    return {}


def ocr_raster_dpi() -> int:
    """
    Raster DPI for pdf2image OCR (lower = faster, less accurate).

    Env ``HAZQUERY_OCR_DPI`` (default 200), clamped to 72–400.
    """
    try:
        v = int((os.environ.get("HAZQUERY_OCR_DPI") or "200").strip())
        return max(72, min(400, v))
    except ValueError:
        return 200


# Minimum total extracted text length (chars) below which we run OCR.
MIN_TEXT_LENGTH_FOR_OCR = 250

# Optional OCR stack (Tesseract + pdf2image, then EasyOCR fallback)
_HAS_PDF2IMAGE = False
_HAS_PYTESSERACT = False
_HAS_OCRMYPDF = False

try:
    from pdf2image import convert_from_bytes
    _HAS_PDF2IMAGE = True
except ImportError:
    convert_from_bytes = None  # type: ignore[misc, assignment]

try:
    import pytesseract
    _HAS_PYTESSERACT = True
except ImportError:
    pytesseract = None  # type: ignore[misc, assignment]

# EasyOCR imports cv2 (opencv) which can fail with LOADER_DIR on Streamlit Cloud.
# Lazy-import only when OCR is actually run to avoid startup crash.
easyocr = None  # type: ignore[misc, assignment]
np = None  # type: ignore[misc, assignment]

try:
    import ocrmypdf
    _HAS_OCRMYPDF = True
except ImportError:
    ocrmypdf = None  # type: ignore[misc, assignment]


def ocr_available() -> bool:
    """True if at least Tesseract + pdf2image are available for OCR."""
    return bool(_HAS_PDF2IMAGE and _HAS_PYTESSERACT)


def _try_import_easyocr() -> tuple[bool, Any, Any]:
    """Lazy import easyocr and numpy. Returns (ok, easyocr_module, np_module)."""
    try:
        import easyocr
        import numpy as np_mod
        return True, easyocr, np_mod
    except Exception:
        return False, None, None


def easyocr_available() -> bool:
    """True if EasyOCR is available for fallback OCR."""
    ok, _, _ = _try_import_easyocr()
    return ok


def extract_text_from_pdf_bytes(
    pdf_bytes: bytes,
    use_ocr_if_needed: bool = True,
    min_text_length: int = MIN_TEXT_LENGTH_FOR_OCR,
) -> str:
    """
    Extract text from a PDF. Tries embedded text first; if too short, runs OCR.

    Args:
        pdf_bytes: Raw PDF bytes.
        use_ocr_if_needed: If True and embedded text length < min_text_length, run OCR.
        min_text_length: Threshold (total chars) below which OCR is used.

    Returns:
        Concatenated text from all pages. Empty string if extraction fails.
    """
    if not pdf_bytes:
        return ""

    text = _extract_embedded_text(pdf_bytes)
    text = (text or "").strip()

    if use_ocr_if_needed and len(text) < min_text_length and ocr_available():
        logger.info("Embedded text short (%s chars), running OCR.", len(text))
        ocr_text = extract_text_via_ocr(pdf_bytes)
        if ocr_text.strip():
            return ocr_text.strip()

    return text


def _extract_embedded_text(pdf_bytes: bytes) -> str:
    """Text from pypdf only (no OCR)."""
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        parts: list[str] = []
        for page in reader.pages:
            try:
                txt = page.extract_text() or ""
            except Exception:
                txt = ""
            parts.append(txt)
        return "\n".join(parts).strip()
    except Exception as e:
        logger.warning("Embedded text extraction failed: %s", e)
        return ""


def extract_text_via_ocr(
    pdf_bytes: bytes,
    use_easyocr_fallback: bool = True,
    dpi: Optional[int] = None,
) -> str:
    """
    Render PDF to images and run OCR (Tesseract; EasyOCR per-page fallback if enabled).

    Requires: pdf2image (and system Poppler), pytesseract (and system Tesseract).
    Optional: EasyOCR for pages where Tesseract returns little text.

    ``dpi`` defaults to :func:`ocr_raster_dpi` (env ``HAZQUERY_OCR_DPI``).
    """
    if not _HAS_PDF2IMAGE or not convert_from_bytes:
        logger.warning("pdf2image not available for OCR.")
        return ""

    eff_dpi = dpi if dpi is not None else ocr_raster_dpi()

    try:
        images = convert_from_bytes(
            pdf_bytes, dpi=eff_dpi, fmt="jpeg", **poppler_kwargs_for_pdf2image()
        )
    except Exception as e:
        logger.warning("pdf2image failed: %s", e)
        return ""

    if not images:
        return ""

    reader_easy: Any = None
    if use_easyocr_fallback:
        ok, easyocr_mod, _ = _try_import_easyocr()
        if ok and easyocr_mod:
            try:
                reader_easy = easyocr_mod.Reader(["en"], gpu=False, verbose=False)
            except Exception as e:
                logger.info("EasyOCR init skipped: %s", e)
                reader_easy = None

    page_texts: list[str] = []
    for i, img in enumerate(images):
        page_str = _ocr_page_tesseract(img)
        if use_easyocr_fallback and reader_easy and len((page_str or "").strip()) < 50:
            easy_str = _ocr_page_easyocr(img, reader_easy)
            if easy_str.strip():
                page_str = easy_str
        page_texts.append(page_str or "")

    return "\n".join(page_texts).strip()


def _ocr_page_tesseract(img) -> str:
    """Run Tesseract on a PIL Image. Returns empty string if unavailable."""
    if not _HAS_PYTESSERACT or pytesseract is None:
        return ""
    try:
        return pytesseract.image_to_string(img, lang="eng")
    except Exception as e:
        logger.debug("Tesseract OCR failed for page: %s", e)
        return ""


def _ocr_page_easyocr(img: Any, reader: Any) -> str:
    """Run EasyOCR on a PIL Image. img is PIL Image; reader is easyocr.Reader."""
    ok, _, np_mod = _try_import_easyocr()
    if not ok or np_mod is None:
        return ""
    try:
        arr = np_mod.array(img)
        results = reader.readtext(arr, paragraph=True)
        return " ".join([item[1] for item in results if len(item) > 1])
    except Exception as e:
        logger.debug("EasyOCR failed for page: %s", e)
        return ""


def normalize_whitespace(text: str) -> str:
    """Collapse repeated whitespace for more stable regex extraction."""
    if not text:
        return ""
    collapsed = " ".join(text.replace("\r", "\n").split())
    # Embedded PDF text often becomes one long line; section parsers expect headers
    # at line boundaries. Re-insert newlines before OSHA/GHS-style "Section N:" blocks.
    restored = re.sub(
        r"(?<=.)\b(Section\s+(?:1[0-6]|[1-9])\s*[:\.]?(?=\s|$))",
        r"\n\1",
        collapsed,
        flags=re.IGNORECASE,
    )
    return restored.strip()


def extract_tables_from_text(text: str) -> list[list[list[str]]]:
    """
    Heuristic extraction of table-like structures from plain text.
    Detects rows by pipe (|) or tab separators; groups consecutive rows with
    the same cell count into tables. Returns list of tables; each table is
    list of rows; each row is list of cell strings.
    """
    if not text or not text.strip():
        return []
    tables: list[list[list[str]]] = []
    current: list[list[str]] = []
    prev_len = -1
    for line in text.replace("\r", "\n").split("\n"):
        line = line.strip()
        if not line:
            if current:
                tables.append(current)
                current = []
            prev_len = -1
            continue
        cells: list[str]
        if "|" in line:
            cells = [c.strip() for c in line.split("|")]
        elif "\t" in line:
            cells = [c.strip() for c in line.split("\t")]
        else:
            if current:
                tables.append(current)
                current = []
            prev_len = -1
            continue
        if len(cells) < 2:
            continue
        if prev_len != -1 and len(cells) != prev_len:
            if current:
                tables.append(current)
                current = []
        current.append(cells)
        prev_len = len(cells)
    if current:
        tables.append(current)
    return tables


def make_searchable_pdf(
    input_path: str,
    output_path: str,
    language: str = "eng",
) -> bool:
    """
    Add a text layer to a PDF using ocrmypdf (Tesseract). Produces a searchable PDF.

    Requires: ocrmypdf and system Tesseract. Use after installing:
      pip install ocrmypdf
    And ensure Tesseract is on PATH.

    Returns:
        True if successful, False otherwise.
    """
    if not _HAS_OCRMYPDF or ocrmypdf is None:
        logger.warning("ocrmypdf not installed. pip install ocrmypdf")
        return False
    if not os.path.isfile(input_path):
        logger.warning("Input file not found: %s", input_path)
        return False
    try:
        # Legacy API: input, output, then keyword args (language, mode='skip', etc.)
        ocrmypdf.ocr(input_path, output_path, language=language)
        return True
    except Exception as e:
        logger.warning("ocrmypdf failed: %s", e)
        return False
