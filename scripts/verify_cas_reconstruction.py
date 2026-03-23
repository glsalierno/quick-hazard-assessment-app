#!/usr/bin/env python3
"""
Verification protocol for CAS reconstruction.

Runs CAS reconstructor + robust extractor on SDS files and compares expected vs actual.
Usage: python scripts/verify_cas_reconstruction.py [--sds-dir PATH]
"""

from __future__ import annotations

import argparse
import logging
import os

# Suppress Streamlit warnings when running as standalone script
os.environ.setdefault("STREAMLIT_SERVER_HEADLESS", "true")
os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
for _logger in ["streamlit", "streamlit.runtime", "streamlit.runtime.scriptrunner_utils"]:
    logging.getLogger(_logger).setLevel(logging.CRITICAL)
import io
import sys
from pathlib import Path

# Add parent to path for utils
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Expected CAS by SDS filename (partial match)
EXPECTED = {
    "FORANEr 408A": ["75-45-6", "420-46-2", "354-33-6"],
    "FORANE 408A": ["75-45-6", "420-46-2", "354-33-6"],
    "n-octyltriethoxysilane": ["2943-75-1"],
    "n-tetradecyltrichlorosilane": ["18402-22-7"],
    "Butyraldehyde": ["123-72-8"],
    "AMOLEA AS-300": ["1263679-68-0", "1263679-71-5"],
    "FC-770": ["86508-42-1"],
    "FC-3283": ["86508-42-1"],
    "FC-149": ["60805-12-1", "7732-18-5"],
    "FORANEr 1225ye": ["2252-83-7"],
    "FORANE 1225ye": ["2252-83-7"],
    "Dowfrost 30": [],  # mixture, no CAS expected
    "DOWFROST 30": [],
    "OpteonT XL40": ["754-12-6", "75-10-5"],
    "Opteon XL40": ["754-12-6", "75-10-5"],
    "2-Methylfuran": ["534-22-5"],
    "534-22-5": ["534-22-5"],
    "HFO-1336mzz-z": ["692-49-9"],
    "2019-cis-1-1-1-4-4-4-hexafluoro": ["692-49-9"],
    "Acryloyl Chloride": ["814-68-6"],
    "Amine terminated PDMS": ["106214-84-0"],
    "Hydroxylamine hydrochloride": ["5470-11-1"],
    "n-Octadecyltriethoxysilane": ["7399-00-0"],
    "Glycidyl POSS": ["68611-45-0"],
    "Mercaptopropyl terminated PDMS": ["308072-58-4"],
    "Poly(dimethylsiloxane), chlorine terminated": ["67923-13-1"],
    "Poly(dimethylsiloxane), vinyl terminated": ["68083-19-2"],
}


def extract_text_pdfplumber(pdf_bytes: bytes) -> str:
    """Extract text from PDF; fallback to pypdf if pdfplumber fails (e.g. malformed MediaBox)."""
    try:
        import pdfplumber

        parts = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for i, page in enumerate(pdf.pages):
                try:
                    t = page.extract_text()
                    if t:
                        parts.append(t)
                except (TypeError, AttributeError, KeyError):
                    continue
        return "\n".join(parts)
    except Exception:
        try:
            from pypdf import PdfReader

            reader = PdfReader(io.BytesIO(pdf_bytes))
            parts = []
            for page in reader.pages:
                t = page.extract_text()
                if t:
                    parts.append(t)
            return "\n".join(parts)
        except Exception:
            return ""


def find_expected(filename: str) -> list[str]:
    for key, cas_list in EXPECTED.items():
        if key.lower() in filename.lower():
            return cas_list
    return []  # unknown


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sds-dir", default=None, help="Path to SDS examples folder")
    parser.add_argument("--limit", type=int, default=0, help="Max files (0=all)")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--out-json", dest="out_json", default=None, help="Write results to JSON file")
    args = parser.parse_args()

    app_dir = Path(__file__).resolve().parent.parent
    sds_dir = Path(args.sds_dir) if args.sds_dir else app_dir.parent / "sds examples"
    if not sds_dir.exists():
        print(f"SDS dir not found: {sds_dir}")
        return 1

    pdf_files = sorted(sds_dir.glob("*.pdf"))
    if args.limit:
        pdf_files = pdf_files[: args.limit]
    print(f"Processing {len(pdf_files)} PDFs from {sds_dir}\n")

    from utils.cas_reconstructor import CASReconstructor
    from utils.robust_cas_extractor import RobustCASExtractor

    try:
        from config import RECONSTRUCTOR_MAX_GAP
    except Exception:
        RECONSTRUCTOR_MAX_GAP = 15
    recon = CASReconstructor(max_gap=RECONSTRUCTOR_MAX_GAP)
    extractor = RobustCASExtractor(use_docling=False, use_ocr=False)

    results = []
    total_expected = 0
    total_found = 0
    total_correct = 0
    false_positives = 0

    for i, pdf_path in enumerate(pdf_files):
        name = pdf_path.name
        try:
            pdf_bytes = pdf_path.read_bytes()
        except Exception as e:
            print(f"  SKIP {name}: read error - {e}")
            continue

        try:
            text = extract_text_pdfplumber(pdf_bytes)
            debug = recon.reconstruct_with_debug(text)
            full_results = extractor.extract(pdf_bytes)
        except Exception as e:
            print(f"  SKIP {name}: extraction error - {e}")
            continue

        recon_cas = set(debug["valid_cas"])
        full_cas = set(r.cas for r in full_results)
        expected = find_expected(name)

        # Status
        exp_set = set(expected)
        missing = exp_set - full_cas
        extra = full_cas - exp_set
        # If no expected, we can't judge missing; extra would be potential false positives
        if expected:
            status = "OK" if not missing and not extra else ("PARTIAL" if not missing else "FAIL")
        else:
            status = "OK" if full_cas else "OK (no expected)"

        if expected:
            total_expected += len(exp_set)
            total_correct += len(exp_set & full_cas)
        total_found += len(full_cas)
        if expected and extra:
            false_positives += len(extra)  # simplified

        results.append(
            {
                "file": name[:50],
                "expected": expected,
                "reconstructor": sorted(recon_cas),
                "full_pipeline": sorted(full_cas),
                "missing": sorted(missing),
                "extra": sorted(extra),
                "status": status,
            }
        )

        if args.verbose:
            print(f"\n{'='*60}")
            print(f"File: {name}")
            print(f"{'='*60}")
            print(f"Digit sequences: {len(debug['digit_sequences'])}")
            print(f"Candidates: {debug['candidates'][:15]}...")
            print(f"Reconstructor valid: {debug['valid_cas']}")
            print(f"Full pipeline: {[r.cas for r in full_results]}")
            print(f"Expected: {expected} -> {status}")

    # Summary table
    print("\n" + "=" * 90)
    print("EXPECTED vs ACTUAL COMPARISON")
    print("=" * 90)
    print(f"{'SDS File':<50} {'Expected':<30} {'Reconstructed':<25} {'Status':<8}")
    print("-" * 90)

    for r in results:
        exp_str = ",".join(r["expected"][:3]) if r["expected"] else "(none)"
        rec_str = ",".join(r["full_pipeline"][:3]) if r["full_pipeline"] else "(none)"
        print(f"{r['file']:<50} {exp_str:<30} {rec_str:<25} {r['status']:<8}")

    # Metrics
    print("\n" + "=" * 60)
    print("PERFORMANCE METRICS")
    print("=" * 60)
    with_expected = [r for r in results if r["expected"]]
    ok_count_with_exp = sum(1 for r in with_expected if r["status"] == "OK")
    success_rate = ok_count_with_exp / len(with_expected) if with_expected else 0
    precision = total_correct / total_found if total_found else 1.0
    recall = total_correct / total_expected if total_expected else 1.0

    print(f"Success rate (all expected found, no extra): {success_rate:.1%} ({ok_count_with_exp}/{len(with_expected)} files with expected CAS)")
    print(f"Precision (correct/extracted): {precision:.2f}")
    print(f"Recall (correct/expected): {recall:.2f}")
    print(f"Files processed: {len(results)}")

    if args.out_json:
        import json

        out_path = Path(args.out_json)
        payload = {
            "metrics": {
                "success_rate": success_rate,
                "ok_count": ok_count_with_exp,
                "with_expected": len(with_expected),
                "precision": precision,
                "recall": recall,
                "files_processed": len(results),
            },
            "results": results,
        }
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"\nResults written to {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
