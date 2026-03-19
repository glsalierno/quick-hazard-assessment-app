#!/usr/bin/env python3
"""
CLI for the unified hazard scraper (ECHA, Danish QSAR, VEGA, ICE).
Outputs a P2OASys-ready CSV and optionally prints extra_sources for build_hazard_data().

Usage:
  python scripts/run_unified_hazard_scraper.py --cas 71-43-2 50-00-0 --output results.csv
  python scripts/run_unified_hazard_scraper.py --smiles "CCO" --output out.csv
  python scripts/run_unified_hazard_scraper.py --file cas_list.txt --id-type cas --output batch.csv

Environment:
  VEGA_API_KEY, ICE_API_KEY: optional API keys for those services.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# App root (parent of scripts/)
APP_ROOT = Path(__file__).resolve().parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from utils.hazard_scrapers import (
    HazardDataAggregator,
    scraper_results_to_extra_sources,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Unified hazard scraper for ECHA, Danish QSAR, VEGA, ICE → P2OASys CSV"
    )
    parser.add_argument(
        "--cas",
        nargs="*",
        default=[],
        help="CAS numbers to query",
    )
    parser.add_argument(
        "--smiles",
        nargs="*",
        default=[],
        help="SMILES strings to query",
    )
    parser.add_argument(
        "--file",
        type=Path,
        default=None,
        help="File with one identifier per line (CAS or SMILES)",
    )
    parser.add_argument(
        "--id-type",
        choices=("cas", "smiles"),
        default="cas",
        help="Identifier type when using --file",
    )
    parser.add_argument(
        "--sources",
        nargs="*",
        default=None,
        choices=["ECHA", "Danish_QSAR", "VEGA", "ICE"],
        help="Sources to use (default: all)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Output CSV path (P2OASys-ready)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=APP_ROOT / "hazard_cache",
        help="Cache directory for responses",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Seconds between chemicals in batch",
    )
    parser.add_argument(
        "--extra-sources",
        action="store_true",
        help="Print extra_sources dict for first chemical (for build_hazard_data)",
    )
    args = parser.parse_args()

    identifiers: list[str] = []
    id_type = "cas"
    if args.cas:
        identifiers = [c.strip() for c in args.cas if c.strip()]
        id_type = "cas"
    elif args.smiles:
        identifiers = [s.strip() for s in args.smiles if s.strip()]
        id_type = "smiles"
    elif args.file:
        path = args.file if args.file.is_absolute() else (APP_ROOT / args.file)
        if not path.is_file():
            print(f"File not found: {path}", file=sys.stderr)
            sys.exit(1)
        identifiers = [
            line.strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        id_type = args.id_type
    else:
        parser.print_help()
        print("\nProvide --cas, --smiles, or --file.", file=sys.stderr)
        sys.exit(1)

    vega_key = os.environ.get("VEGA_API_KEY")
    ice_key = os.environ.get("ICE_API_KEY")

    aggregator = HazardDataAggregator(
        cache_dir=str(args.cache_dir),
        vega_api_key=vega_key or None,
        ice_api_key=ice_key or None,
    )

    if len(identifiers) == 1:
        chemical_data = aggregator.search_chemical(identifiers[0], id_type=id_type, sources=args.sources)
        df = aggregator.aggregate_for_p2oasys(chemical_data)
        if args.extra_sources:
            extra = scraper_results_to_extra_sources(chemical_data)
            print("extra_sources (for build_hazard_data):", extra)
        if args.output and not df.empty:
            out_path = args.output if args.output.is_absolute() else (APP_ROOT / args.output)
            df.to_csv(out_path, index=False)
            print(f"Wrote {out_path}")
    else:
        df = aggregator.batch_process(
            identifiers,
            id_type=id_type,
            sources=args.sources,
            output_file=args.output,
            delay_between=args.delay,
        )
        if args.output and not df.empty:
            print(f"Wrote {args.output}")

    if df.empty:
        print("No data returned.", file=sys.stderr)
    else:
        print(f"Shape: {df.shape}")


if __name__ == "__main__":
    main()
