#!/usr/bin/env python3
"""
Batch-test PureCASExtractor (Docling + DistilBERT) on a folder of SDS PDFs.

Usage (from repo root quick-hazard-assessment-app):
  python scripts/test_pure_cas_sds_folder.py --folder "../sds examples" --limit 20
"""

from __future__ import annotations

import os
import warnings

os.environ.setdefault("STREAMLIT_LOGGER_LEVEL", "error")
warnings.filterwarnings("ignore", message=".*ScriptRunContext.*")
warnings.filterwarnings("ignore", message=".*Session state does not function.*")
warnings.filterwarnings("ignore", message=".*pin_memory.*")
warnings.filterwarnings("ignore", message=".*torch_dtype.*")

import argparse
import csv
import sys
import time
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--folder",
        type=Path,
        default=root.parent / "sds examples",
        help="Directory containing SDS PDFs",
    )
    ap.add_argument("--limit", type=int, default=None, help="Max number of PDFs (default: all)")
    ap.add_argument("--out", type=Path, default=root / "artifacts" / "pure_cas_sds_test.csv")
    args = ap.parse_args()

    from utils.cas_extractor import MemoryOptimizedPureCASExtractor, is_pure_cas_bert_available

    print("Status:", is_pure_cas_bert_available())
    folder = args.folder.resolve()
    if not folder.is_dir():
        print("Folder not found:", folder)
        return 1

    pdfs = sorted(folder.glob("*.pdf")) + sorted(folder.glob("*.PDF"))
    if args.limit:
        pdfs = pdfs[: args.limit]
    if not pdfs:
        print("No PDFs in", folder)
        return 1

    extractor = MemoryOptimizedPureCASExtractor(use_streamlit_converter_cache=False)
    args.out.parent.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, str]] = []
    t0_all = time.perf_counter()
    for i, pdf in enumerate(pdfs, start=1):
        t0 = time.perf_counter()
        err = ""
        cas_out: list[str] = []
        try:
            data = pdf.read_bytes()
            results = extractor.extract(data)
            cas_out = [r.cas for r in results]
        except Exception as e:
            err = repr(e)
        elapsed = time.perf_counter() - t0
        rows.append(
            {
                "file": pdf.name,
                "seconds": f"{elapsed:.2f}",
                "n_cas": str(len(cas_out)),
                "cas": "; ".join(cas_out),
                "error": err,
            }
        )
        print(f"[{i}/{len(pdfs)}] {pdf.name}: n_cas={len(cas_out)} ({elapsed:.1f}s){' ERR: ' + err if err else ''}", flush=True)
        # Rewrite CSV after each file so partial results survive long runs / crashes
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["file", "seconds", "n_cas", "cas", "error"])
            w.writeheader()
            w.writerows(rows)

    total = sum(1 for r in rows if int(r["n_cas"]) > 0)
    print(f"\nDone: {total}/{len(rows)} PDFs with >=1 CAS. CSV: {args.out}")
    print(f"Total wall time: {time.perf_counter() - t0_all:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
