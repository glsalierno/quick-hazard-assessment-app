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
from io import BytesIO
from typing import Optional

from pypdf import PdfReader

logger = logging.getLogger(__name__)

# Minimum total extracted text length (chars) below which we run OCR.
MIN_TEXT_LENGTH_FOR_OCR = 250

# Optional OCR stack (Tesseract + pdf2image, then EasyOCR fallback)
_HAS_PDF2IMAGE = False
_HAS_PYTESSERACT = False
_HAS_EASYOCR = False
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

try:
    import easyocr
    import numpy as np
    _HAS_EASYOCR = True
except ImportError:
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


def easyocr_available() -> bool:
    """True if EasyOCR is available for fallback OCR."""
    return bool(_HAS_EASYOCR)


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
    dpi: int = 200,
    lang: str = "eng",
) -> str:
    """
    Render PDF to images and run OCR (Tesseract; EasyOCR per-page fallback if enabled).

    Requires: pdf2image (and system Poppler), pytesseract (and system Tesseract).
    Optional: EasyOCR for pages where Tesseract returns little text.
    """
    if not _HAS_PDF2IMAGE or not convert_from_bytes:
        logger.warning("pdf2image not available for OCR.")
        return ""

    try:
        images = convert_from_bytes(pdf_bytes, dpi=dpi, fmt="jpeg")
    except Exception as e:
        logger.warning("pdf2image failed: %s", e)
        return ""

    if not images:
        return ""

    reader_easy: Optional["easyocr.Reader"] = None
    if use_easyocr_fallback and _HAS_EASYOCR and np is not None:
        try:
            reader_easy = easyocr.Reader(["en"], gpu=False, verbose=False)
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


def _ocr_page_easyocr(img, reader: "easyocr.Reader") -> str:
    """Run EasyOCR on a PIL Image. img is PIL Image; reader is easyocr.Reader."""
    if not _HAS_EASYOCR or np is None:
        return ""
    try:
        arr = np.array(img)
        results = reader.readtext(arr, paragraph=True)
        return " ".join([item[1] for item in results if len(item) > 1])
    except Exception as e:
        logger.debug("EasyOCR failed for page: %s", e)
        return ""


def normalize_whitespace(text: str) -> str:
    """Collapse repeated whitespace for more stable regex extraction."""
    if not text:
        return ""
    return " ".join(text.replace("\r", "\n").split())


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
