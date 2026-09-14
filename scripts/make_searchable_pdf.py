"""
Produce a searchable PDF by adding a text layer with Tesseract (via ocrmypdf).

Usage (from repo root):
  python scripts/make_searchable_pdf.py input.pdf [output.pdf]

If output is omitted, writes to input_searchable.pdf.
Requires: pip install ocrmypdf, and Tesseract on PATH.
"""

from __future__ import annotations

import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

from utils.sds_pdf_utils import make_searchable_pdf

def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/make_searchable_pdf.py input.pdf [output.pdf]")
        sys.exit(1)
    input_path = os.path.abspath(sys.argv[1])
    if len(sys.argv) >= 3:
        output_path = os.path.abspath(sys.argv[2])
    else:
        base, ext = os.path.splitext(input_path)
        output_path = base + "_searchable" + (ext or ".pdf")

    if not os.path.isfile(input_path):
        print(f"File not found: {input_path}")
        sys.exit(1)

    if make_searchable_pdf(input_path, output_path, language="eng"):
        print(f"Wrote searchable PDF: {output_path}")
    else:
        print("Failed. Ensure ocrmypdf and Tesseract are installed (see docs/OCR_SETUP.md).")
        sys.exit(1)

if __name__ == "__main__":
    main()
