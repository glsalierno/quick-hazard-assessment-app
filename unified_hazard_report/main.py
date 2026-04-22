#!/usr/bin/env python3
"""CLI: unified hazard report (legacy + IUCLID offline). Run from ``quick-hazard-assessment-app`` root."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from unified_hazard_report.data_context import OfflineDataContext
from unified_hazard_report.report_generator import generate_report


def _read_cas_file(path: Path) -> list[str]:
    lines: list[str] = []
    for ln in path.read_text(encoding="utf-8", errors="replace").splitlines():
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        lines.append(s.split()[0])
    return lines


def main() -> None:
    p = argparse.ArgumentParser(description="Unified hazard report: PubChem/DSSTox/ToxVal + REACH IUCLID offline.")
    p.add_argument("--cas-list", type=str, default="", help="Comma-separated CAS numbers")
    p.add_argument("--cas-file", type=str, default="", help="File with one CAS per line")
    p.add_argument("--output", "-o", type=str, required=True, help="Output file path")
    p.add_argument("--format", choices=("csv", "json", "excel"), default="csv")
    args = p.parse_args()

    cas_numbers: list[str] = []
    if args.cas_list.strip():
        cas_numbers.extend(x.strip() for x in args.cas_list.split(",") if x.strip())
    if args.cas_file.strip():
        cf = Path(args.cas_file).expanduser()
        if not cf.is_file():
            print(f"CAS file not found: {cf}", file=sys.stderr)
            sys.exit(2)
        cas_numbers.extend(_read_cas_file(cf))

    if not cas_numbers:
        print("Provide --cas-list and/or --cas-file.", file=sys.stderr)
        sys.exit(2)

    print("Loading offline IUCLID snapshots + indexing .i6z (first run can be slow)…", flush=True)
    ctx = OfflineDataContext()

    dfs: list[pd.DataFrame] = []
    for cas in tqdm(cas_numbers, desc="CAS"):
        dfs.append(generate_report([cas], ctx, output_path=None))

    out_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    fmt = args.format
    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        out_df.to_csv(outp, index=False)
    elif fmt == "json":
        out_df.to_json(outp, orient="records", indent=2, force_ascii=False)
    else:
        try:
            out_df.to_excel(outp, index=False, engine="openpyxl")
        except ImportError:
            print("pip install openpyxl for Excel output.", file=sys.stderr)
            sys.exit(3)
    print(f"Wrote {len(out_df)} rows -> {outp.resolve()}", flush=True)


if __name__ == "__main__":
    main()
