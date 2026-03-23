#!/usr/bin/env python3
"""
Run Pure CAS BERT (Docling + DistilBERT) on a single PDF. Unbuffered output.

Usage (from quick-hazard-assessment-app):
  python -u scripts/test_bert_one_pdf.py "c:/path/to/file.pdf"
  python -u scripts/test_bert_one_pdf.py "../sds examples/402516.pdf"
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    os.environ.setdefault("PYTHONUNBUFFERED", "1")

    if len(sys.argv) < 2:
        print("Usage: python -u scripts/test_bert_one_pdf.py <path-to.pdf>", flush=True)
        return 2

    pdf = Path(sys.argv[1]).resolve()
    if not pdf.is_file():
        print("File not found:", pdf, flush=True)
        return 1

    print("PDF:", pdf, "bytes:", pdf.stat().st_size, flush=True)

    from utils.cas_extractor import MemoryOptimizedPureCASExtractor, is_pure_cas_bert_available

    print("is_pure_cas_bert_available:", is_pure_cas_bert_available(), flush=True)

    ex = MemoryOptimizedPureCASExtractor(use_streamlit_converter_cache=False)
    data = pdf.read_bytes()
    print("Docling + BERT extract (first run can take several minutes on CPU)...", flush=True)
    t0 = time.perf_counter()
    try:
        results = ex.extract(data)
    except Exception as e:
        print("ERROR:", repr(e), flush=True)
        return 1
    elapsed = time.perf_counter() - t0
    print(f"Done in {elapsed:.1f}s — n_cas={len(results)}", flush=True)
    for r in results:
        print(f"  {r.cas}  conf={r.confidence:.3f}  page={r.source_page}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
