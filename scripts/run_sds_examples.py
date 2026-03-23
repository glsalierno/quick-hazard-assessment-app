"""
Run SDS extraction (Phase 1 regex) on PDFs in the sds examples folder.

Usage (from repo root):
  python scripts/run_sds_examples.py [--limit N] [--compare]

  --limit N   Process at most N PDFs (default: all).
  --compare   For each extracted CAS, fetch PubChem and run SDS vs v1.3 comparison.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

# Run from repo root so app imports work
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)

import config
from utils import sds_compare, sds_pdf_utils, sds_regex_extractor

try:
    from utils import pubchem_client
except ImportError:
    pubchem_client = None


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SDS extraction on PDFs in sds examples folder.")
    parser.add_argument("--limit", type=int, default=0, help="Max number of PDFs to process (0 = all)")
    parser.add_argument("--compare", action="store_true", help="Run SDS vs PubChem comparison for extracted CAS")
    parser.add_argument("--dir", type=str, default="", help="Override SDS examples folder path")
    args = parser.parse_args()

    sds_dir = os.path.abspath(args.dir or config.SDS_EXAMPLES_DIR)
    if not os.path.isdir(sds_dir):
        print(f"SDS examples folder not found: {sds_dir}")
        print("Set SDS_EXAMPLES_DIR or pass --dir path.")
        sys.exit(1)

    pdfs = sorted(
        [f for f in os.listdir(sds_dir) if f.lower().endswith(".pdf")],
        key=lambda x: x.lower(),
    )
    if not pdfs:
        print(f"No PDFs found in {sds_dir}")
        sys.exit(0)

    if args.limit:
        pdfs = pdfs[: args.limit]
    print(f"Processing {len(pdfs)} PDF(s) from {sds_dir}\n")

    for i, name in enumerate(pdfs, 1):
        path = os.path.join(sds_dir, name)
        print(f"--- [{i}/{len(pdfs)}] {name} ---")
        try:
            with open(path, "rb") as f:
                raw_text = sds_pdf_utils.extract_text_from_pdf_bytes(f.read())
            raw_text = sds_pdf_utils.normalize_whitespace(raw_text)
        except Exception as e:
            print(f"  Error reading PDF: {e}")
            print()
            continue

        if not raw_text.strip():
            print("  No text extracted (scanned PDF or empty).")
            print()
            continue

        result = sds_regex_extractor.extract_sds_fields_from_text(raw_text)
        cas_list = result.get("cas_numbers") or []
        ghs = result.get("ghs") or {}
        quant = result.get("quantitative") or {}

        print(f"  CAS: {cas_list or '(none)'}")
        print(f"  H-codes: {len(ghs.get('h_codes') or [])}  P-codes: {len(ghs.get('p_codes') or [])}  Signal: {ghs.get('signal_word') or '-'}")
        if quant.get("flash_point"):
            print(f"  Flash point: {len(quant['flash_point'])} value(s)")
        if quant.get("vapor_pressure"):
            print(f"  Vapor pressure: {len(quant['vapor_pressure'])} value(s)")
        if quant.get("aquatic_toxicity"):
            print(f"  Aquatic LC50/EC50: {len(quant['aquatic_toxicity'])} value(s)")

        if args.compare and cas_list and pubchem_client:
            cas = cas_list[0]
            pubchem_data = pubchem_client.get_compound_data(cas, input_type="cas")
            if pubchem_data:
                comp = sds_compare.compare_sds_to_pubchem(result, pubchem_data)
                match = comp.get("match_summary", {})
                ghs_m = match.get("ghs", {})
                print(f"  vs PubChem ({cas}): H overlap={ghs_m.get('h_overlap', 0)}  P overlap={ghs_m.get('p_overlap', 0)}  signal_match={match.get('signal_word', {}).get('match', False)}")
                q = comp.get("quantitative_comparison", {})
                for k in ("flash_point", "vapor_pressure", "aquatic_toxicity"):
                    if k in q:
                        print(f"    {k}: matches={q[k].get('matches_count', 0)}  mismatches={q[k].get('mismatches_count', 0)}")
            else:
                print(f"  PubChem lookup failed for {cas}")
        elif args.compare and not cas_list:
            print("  (Skipping comparison: no CAS extracted from SDS.)")
        print()

    print("Done.")


if __name__ == "__main__":
    main()
