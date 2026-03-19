"""
Quick test of SDS readers: OCR availability and extraction on example PDFs.
Run from repo root: python scripts/test_sds_readers.py [--limit N]
"""

from __future__ import annotations

import argparse
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)

import config
from utils import sds_pdf_utils, sds_regex_extractor


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="Max PDFs to test (0 = all)")
    args = parser.parse_args()

    sds_dir = os.path.abspath(config.SDS_EXAMPLES_DIR)
    if not os.path.isdir(sds_dir):
        print(f"SDS examples folder not found: {sds_dir}")
        sys.exit(1)

    pdfs = sorted([f for f in os.listdir(sds_dir) if f.lower().endswith(".pdf")])
    if args.limit:
        pdfs = pdfs[: args.limit]

    print("=== SDS reader test ===\n")
    print("OCR available (pdf2image + tesseract):", sds_pdf_utils.ocr_available())
    print("EasyOCR available:", sds_pdf_utils.easyocr_available())
    print(f"Testing {len(pdfs)} PDF(s) from: {sds_dir}\n")

    for i, name in enumerate(pdfs, 1):
        path = os.path.join(sds_dir, name)
        with open(path, "rb") as f:
            raw = f.read()
        text = sds_pdf_utils.extract_text_from_pdf_bytes(raw)
        text = sds_pdf_utils.normalize_whitespace(text)
        result = sds_regex_extractor.extract_sds_fields_from_text(text)

        cas = result.get("cas_numbers") or []
        ghs = result.get("ghs") or {}
        quant = result.get("quantitative") or {}

        n_h = len(ghs.get("h_codes") or [])
        n_p = len(ghs.get("p_codes") or [])
        sig = ghs.get("signal_word") or "-"
        fp = len(quant.get("flash_point") or [])
        vp = len(quant.get("vapor_pressure") or [])
        aq = len(quant.get("aquatic_toxicity") or [])

        print(f"[{i}] {name}")
        print(f"    Text len: {len(text)}  |  CAS: {cas or '(none)'}  |  H: {n_h}  P: {n_p}  Signal: {sig}")
        if fp or vp or aq:
            print(f"    Quantitative: flash_pt={fp}  vapor_press={vp}  aquatic={aq}")
        print()

    print("Done.")


if __name__ == "__main__":
    main()
