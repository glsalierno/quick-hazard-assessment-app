#!/usr/bin/env python3
"""
Benchmark the **supported** SDS extraction pipelines (MarkItDown + regex, Hybrid).

Run from repo root (folder containing ``app.py``)::

    python tests/test_extraction_pipelines.py --folder "../sds examples" --limit 20

Outputs:
  - ``reports/extraction_benchmark.csv``
  - ``reports/extraction_benchmark_summary.md``

Requires: ``markitdown[pdf]``; for Hybrid OCR fallback tests, Poppler + Tesseract/EasyOCR.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _mem_mb() -> float:
    try:
        import psutil

        return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    except Exception:
        return 0.0


def _pubchem_counts(cas_list: list[str]) -> tuple[int, int]:
    ok_pub = 0
    try:
        from utils.pubchem_validator import get_pubchem_validator

        v = get_pubchem_validator()
        for c in cas_list:
            r = v.validate(c)
            if r.get("exists") is True:
                ok_pub += 1
    except Exception:
        pass
    return len(cas_list), ok_pub


def _strategy_markitdown_regex(pdf_bytes: bytes) -> tuple[list[str], float, dict[str, Any]]:
    from utils.alternative_extraction import run_markitdown_pipeline
    from utils.cache_manager import ExtractionCacheManager

    cache = ExtractionCacheManager(ROOT / "cache")
    t0 = time.perf_counter()
    cas, _, md = run_markitdown_pipeline(pdf_bytes, use_bert=False, cache=cache, force_cache=True)
    return cas, time.perf_counter() - t0, {"markdown_chars": len(md or "")}


def _strategy_hybrid(pdf_bytes: bytes) -> tuple[list[str], float, dict[str, Any]]:
    from utils.alternative_extraction import run_hybrid_pipeline
    from utils.cache_manager import ExtractionCacheManager

    cache = ExtractionCacheManager(ROOT / "cache")
    t0 = time.perf_counter()
    cas, method = run_hybrid_pipeline(
        pdf_bytes, use_bert=False, cache=cache, force_cache=True, psm=6
    )
    return cas, time.perf_counter() - t0, {"method": method}


STRATEGIES: dict[str, Callable[[bytes], tuple[list[str], float, dict[str, Any]]]] = {
    "1_markitdown_regex": _strategy_markitdown_regex,
    "2_hybrid_md_ocr": _strategy_hybrid,
}


def main() -> int:
    ap = argparse.ArgumentParser(description="Benchmark supported SDS extraction pipelines")
    ap.add_argument("--folder", type=Path, default=ROOT.parent / "sds examples", help="Folder of PDFs")
    ap.add_argument("--limit", type=int, default=20, help="Max PDFs")
    ap.add_argument("--out-dir", type=Path, default=ROOT / "reports", help="Output directory")
    args = ap.parse_args()

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

    args.out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.out_dir / "extraction_benchmark.csv"
    md_path = args.out_dir / "extraction_benchmark_summary.md"

    rows_out: list[dict[str, Any]] = []
    agg: dict[str, list[float]] = defaultdict(list)
    agg_cas: dict[str, list[int]] = defaultdict(list)

    for pdf in pdfs:
        data = pdf.read_bytes()
        for name, fn in STRATEGIES.items():
            m0 = _mem_mb()
            try:
                cas_list, dt, meta = fn(data)
                err = meta.get("error")
            except Exception as e:
                cas_list, dt, meta = [], 0.0, {}
                err = str(e)
            m1 = _mem_mb()
            chk, pub = _pubchem_counts(cas_list)
            rows_out.append(
                {
                    "pdf": pdf.name,
                    "strategy": name,
                    "time_sec": round(dt, 3),
                    "n_cas_checksum": chk,
                    "n_cas_pubchem": pub,
                    "memory_delta_mb": round(max(0.0, m1 - m0), 2),
                    "error": err or "",
                }
            )
            agg[name].append(dt)
            agg_cas[name].append(chk)

    fieldnames = [
        "pdf",
        "strategy",
        "time_sec",
        "n_cas_checksum",
        "n_cas_pubchem",
        "memory_delta_mb",
        "error",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows_out)

    lines = [
        "# SDS extraction benchmark (supported pipelines only)",
        "",
        f"- Folder: `{folder}`",
        f"- PDFs: {len(pdfs)}",
        "",
        "See [docs/SDS_EXTRACTION_PIPELINES.md](../docs/SDS_EXTRACTION_PIPELINES.md).",
        "",
        "## Mean time (s) and mean CAS count (checksum-valid)",
        "",
        "| Strategy | mean time (s) | mean CAS |",
        "|----------|----------------|----------|",
    ]
    for name in sorted(STRATEGIES.keys()):
        times = agg[name]
        cs = agg_cas[name]
        mt = sum(times) / len(times) if times else 0.0
        mc = sum(cs) / len(cs) if cs else 0.0
        lines.append(f"| {name} | {mt:.3f} | {mc:.2f} |")
    lines.extend(
        [
            "",
            "## Spot-check questions",
            "",
            "1. Hybrid vs MarkItdown-only on scans (OCR path): ",
            "2. False positives / PubChem gate: ",
            "",
        ]
    )
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print("Wrote", csv_path)
    print("Wrote", md_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
