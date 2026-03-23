"""
OCR pipelines: Tesseract (configurable PSM) and EasyOCR-only, for scanned PDFs.

Requires: pdf2image + Poppler, pytesseract + Tesseract binary, optional easyocr.
"""

from __future__ import annotations

import logging
from typing import Any, Literal, Optional

logger = logging.getLogger(__name__)

OcrBackend = Literal["tesseract", "easyocr"]


def _has_pdf2image() -> bool:
    try:
        from pdf2image import convert_from_bytes  # noqa: F401

        return True
    except ImportError:
        return False


def ocr_pdf_tesseract(
    pdf_bytes: bytes,
    *,
    dpi: Optional[int] = None,
    psm: int = 6,
    oem: int = 3,
) -> str:
    """
    Rasterize PDF and run Tesseract with --psm (default 6: uniform block of text).

    psm 11: sparse text (try for some chemical SDS layouts).
    """
    if not _has_pdf2image():
        logger.warning("pdf2image not installed")
        return ""
    try:
        from pdf2image import convert_from_bytes
        import pytesseract
    except ImportError as e:
        logger.warning("OCR deps missing: %s", e)
        return ""

    try:
        from utils.sds_pdf_utils import ocr_raster_dpi, poppler_kwargs_for_pdf2image

        rdpi = dpi if dpi is not None else ocr_raster_dpi()
        images = convert_from_bytes(
            pdf_bytes, dpi=rdpi, fmt="jpeg", **poppler_kwargs_for_pdf2image()
        )
    except Exception as e:
        logger.warning("pdf2image failed: %s", e)
        return ""

    if not images:
        return ""

    config = f"--oem {oem} --psm {psm}"
    parts: list[str] = []
    for img in images:
        try:
            txt = pytesseract.image_to_string(img, lang="eng", config=config)
            parts.append(txt or "")
        except Exception as e:
            logger.debug("Tesseract page failed: %s", e)
            parts.append("")
    return "\n".join(parts).strip()


def ocr_pdf_easyocr_only(
    pdf_bytes: bytes,
    *,
    dpi: Optional[int] = None,
    gpu: bool = False,
    easyocr_reader: Optional[Any] = None,
) -> str:
    """Rasterize PDF and run EasyOCR on each page (no Tesseract).

    Pass a pre-built ``easyocr.Reader`` via ``easyocr_reader`` to avoid reloading
    weights on every PDF (batch scripts).
    """
    if not _has_pdf2image():
        return ""
    try:
        from pdf2image import convert_from_bytes
        import easyocr
        import numpy as np
    except ImportError as e:
        logger.warning("EasyOCR/pdf2image missing: %s", e)
        return ""

    try:
        from utils.sds_pdf_utils import ocr_raster_dpi, poppler_kwargs_for_pdf2image

        rdpi = dpi if dpi is not None else ocr_raster_dpi()
        images = convert_from_bytes(
            pdf_bytes, dpi=rdpi, fmt="jpeg", **poppler_kwargs_for_pdf2image()
        )
    except Exception as e:
        logger.warning("pdf2image failed: %s", e)
        return ""

    if not images:
        return ""

    reader = easyocr_reader
    if reader is None:
        try:
            reader = easyocr.Reader(["en"], gpu=gpu, verbose=False)
        except Exception as e:
            logger.warning("EasyOCR init failed: %s", e)
            return ""

    page_texts: list[str] = []
    for img in images:
        try:
            arr = np.array(img)
            results = reader.readtext(arr, paragraph=True)
            if isinstance(results, list) and results and isinstance(results[0], (list, tuple)):
                page_texts.append(" ".join(str(item[1]) for item in results if len(item) > 1))
            else:
                page_texts.append("")
        except Exception as e:
            logger.debug("EasyOCR page failed: %s", e)
            page_texts.append("")
    return "\n".join(page_texts).strip()


def ocr_pdf_with_cache(
    pdf_bytes: bytes,
    backend: OcrBackend,
    fingerprint: str,
    cache: Any,
    *,
    force: bool = False,
    tesseract_psm: int = 6,
    easyocr_reader: Optional[Any] = None,
    dpi: Optional[int] = None,
) -> str:
    """Load ocr_text.json from cache when possible."""
    name = "ocr_text_tesseract.json" if backend == "tesseract" else "ocr_text_easyocr.json"
    if cache is not None and not force:
        data = cache.load_json(fingerprint, name)
        if isinstance(data, dict) and isinstance(data.get("text"), str):
            return data["text"]

    if backend == "tesseract":
        text = ocr_pdf_tesseract(pdf_bytes, psm=tesseract_psm, dpi=dpi)
    else:
        text = ocr_pdf_easyocr_only(pdf_bytes, easyocr_reader=easyocr_reader, dpi=dpi)

    if cache is not None:
        cache.save_json(
            fingerprint,
            name,
            {"text": text, "backend": backend, "psm": tesseract_psm, "dpi": dpi},
        )
    return text


def ocr_tesseract_then_easyocr_fallback(
    pdf_bytes: bytes,
    *,
    dpi: Optional[int] = None,
    psm: int = 6,
    min_chars_per_page: int = 50,
) -> tuple[str, str]:
    """
    Run Tesseract; for pages with little text, try EasyOCR (similar to sds_pdf_utils.extract_text_via_ocr).
    Returns (combined_text, method_used) where method_used is 'tesseract' or 'tesseract+easyocr_fallback'.
    """
    try:
        from utils import sds_pdf_utils
    except ImportError:
        return "", "none"

    base = sds_pdf_utils.extract_text_via_ocr(pdf_bytes, use_easyocr_fallback=True, dpi=dpi)
    if not base.strip():
        return "", "none"
    return base, "tesseract+easyocr_fallback"
