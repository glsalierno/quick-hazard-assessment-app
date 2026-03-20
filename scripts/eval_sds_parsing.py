#!/usr/bin/env python3
"""
Evaluate SDS parsing: text extraction + SDSParserEngine vs engine+Docling merge.

Run from repo root:
  cd quick-hazard-assessment-app
  python scripts/eval_sds_parsing.py --sds-dir "../sds examples"

Docling is slow (models + per-PDF CPU). Use --docling-max N to cap how many PDFs
run the merged pipeline; all PDFs still get the fast regex/engine path.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path


def _setup_path() -> Path:
    app_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(app_root))
    return app_root


def _cas_summary(rows: list) -> str:
    parts: list[str] = []
    for x in rows[:12]:
        name = (getattr(x, "chemical_name", None) or "")[:28]
        conc = (getattr(x, "concentration", None) or "")[:16]
        m = getattr(x, "method", "")
        extra = f" | {name}" if name.strip() else ""
        if conc.strip():
            extra += f" | {conc}"
        parts.append(f"{x.cas} ({m}){extra}")
    if len(rows) > 12:
        parts.append(f"... +{len(rows) - 12} more")
    return "; ".join(parts) if parts else "—"


def main() -> int:
    app_root = _setup_path()

    parser = argparse.ArgumentParser(description="Evaluate SDS PDF parsing methodologies.")
    parser.add_argument(
        "--sds-dir",
        type=Path,
        default=Path(r"C:\Users\glsal\OneDrive - UMass Lowell\TURI\Research\QSAR\hazquery\GHhaz4\sds examples"),
        help="Folder containing SDS PDFs",
    )
    parser.add_argument(
        "--out-csv",
        type=Path,
        default=None,
        help="Write summary CSV (optional)",
    )
    parser.add_argument(
        "--docling-max",
        type=int,
        default=0,
        help="Max PDFs to also run engine+Docling merge (0 = engine only for all; slow)",
    )
    parser.add_argument(
        "--allow-ocr",
        action="store_true",
        help="Allow pdf2image/Tesseract when embedded text is short (slow; needs Poppler)",
    )
    args = parser.parse_args()

    sds_dir: Path = args.sds_dir
    if not sds_dir.is_dir():
        print(f"ERROR: not a directory: {sds_dir}", file=sys.stderr)
        return 1

    from utils import sds_pdf_utils
    from utils.docling_sds_parser import docling_status_message, is_docling_available
    from utils.sds_parser import _merge_docling_cas_extractions
    from utils.sds_parser_engine import SDSParserEngine

    pdfs = sorted(sds_dir.glob("*.pdf"))
    if not pdfs:
        print(f"No PDFs in {sds_dir}")
        return 1

    engine = SDSParserEngine()
    docling_ok = is_docling_available()
    print(f"# SDS parsing evaluation\n")
    print(f"- **SDS directory:** `{sds_dir}`")
    print(f"- **PDF count:** {len(pdfs)}")
    print(f"- **Docling:** {docling_status_message()}")
    print(f"- **Docling merge on first N files:** N={args.docling_max} (0 = skip merge)\n")

    rows_out: list[dict[str, object]] = []

    for i, pdf_path in enumerate(pdfs):
        pdf_bytes = pdf_path.read_bytes()
        name = pdf_path.name

        t0 = time.perf_counter()
        raw_text = sds_pdf_utils.extract_text_from_pdf_bytes(
            pdf_bytes,
            use_ocr_if_needed=args.allow_ocr,
        )
        text = sds_pdf_utils.normalize_whitespace(raw_text or "")
        t_text = time.perf_counter() - t0
        n_chars = len(text)

        t1 = time.perf_counter()
        res_eng = engine.parse(text)
        t_eng = time.perf_counter() - t1

        n_cas_eng = len(res_eng.cas_numbers)
        n_named_eng = sum(1 for x in res_eng.cas_numbers if (x.chemical_name or "").strip())
        n_conc_eng = sum(1 for x in res_eng.cas_numbers if (x.concentration or "").strip())
        methods_eng = "|".join(sorted(set(res_eng.methods_used)))

        n_cas_merged = n_cas_merged_named = n_cas_merged_conc = None
        t_merged = None
        methods_merged = ""
        run_merge = args.docling_max > 0 and i < args.docling_max and docling_ok

        if run_merge:
            t2 = time.perf_counter()
            res_full = engine.parse(text)
            _merge_docling_cas_extractions(res_full, pdf_bytes)
            t_merged = time.perf_counter() - t2
            n_cas_merged = len(res_full.cas_numbers)
            n_named_merged = sum(1 for x in res_full.cas_numbers if (x.chemical_name or "").strip())
            n_conc_merged = sum(1 for x in res_full.cas_numbers if (x.concentration or "").strip())
            methods_merged = "|".join(sorted(set(res_full.methods_used)))
        else:
            n_named_merged = n_conc_merged = None

        row = {
            "file": name,
            "text_chars": n_chars,
            "t_text_s": round(t_text, 3),
            "t_engine_s": round(t_eng, 3),
            "n_cas_engine": n_cas_eng,
            "named_engine": n_named_eng,
            "conc_engine": n_conc_eng,
            "methods_engine": methods_eng,
            "docling_merge": run_merge,
            "t_merge_s": round(t_merged, 3) if t_merged is not None else "",
            "n_cas_merged": n_cas_merged if n_cas_merged is not None else "",
            "named_merged": n_named_merged if n_named_merged is not None else "",
            "conc_merged": n_conc_merged if n_conc_merged is not None else "",
            "methods_merged": methods_merged,
            "sample_cas_engine": _cas_summary(res_eng.cas_numbers),
        }
        rows_out.append(row)

    if args.out_csv:
        args.out_csv.parent.mkdir(parents=True, exist_ok=True)
        with args.out_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
            w.writeheader()
            w.writerows(rows_out)
        print(f"Wrote CSV: {args.out_csv}\n")

    # Markdown table (truncated methods for display)
    print("## Engine (regex + section/heuristics, no Docling merge)\n")
    print("| File | Chars | t_eng s | #CAS | w/ name | w/ conc | Methods |")
    print("|------|------:|--------:|-----:|--------:|--------:|---------|")
    for r in rows_out:
        m = (r["methods_engine"] or "")[:56]
        if len(str(r["methods_engine"])) > 56:
            m += "…"
        print(
            f"| {r['file'][:40]} | {r['text_chars']} | {r['t_engine_s']} | "
            f"{r['n_cas_engine']} | {r['named_engine']} | {r['conc_engine']} | {m} |"
        )

    if args.docling_max > 0:
        print("\n## Docling merge (first N files only)\n")
        print("| File | t_merge s | #CAS | w/ name | w/ conc | Methods |")
        print("|------|----------:|-----:|--------:|--------:|---------|")
        for r in rows_out:
            if not r["docling_merge"]:
                continue
            print(
                f"| {r['file'][:40]} | {r['t_merge_s']} | {r['n_cas_merged']} | "
                f"{r['named_merged']} | {r['conc_merged']} | {str(r['methods_merged'])[:48]}… |"
            )

    print("\n## Sample CAS rows (engine path, first files)\n")
    for r in rows_out[:5]:
        print(f"### {r['file']}\n")
        print(f"{r['sample_cas_engine']}\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
