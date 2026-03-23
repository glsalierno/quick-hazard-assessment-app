#!/usr/bin/env python3
"""
Build a single CAS -> DTXSID CSV from all DSS/*.xlsx files.
The output is stored in Git (not LFS) so Streamlit Community Cloud can load it.

Run from repo root:
    python scripts/build_dss_csv.py

Requires: pandas, openpyxl (pip install pandas openpyxl)
"""

from __future__ import annotations

import os
import sys

try:
    import pandas as pd
except ImportError:
    print("Need pandas: pip install pandas openpyxl", file=sys.stderr)
    sys.exit(1)

DSS_DIR = "DSS"
OUTPUT_CSV = os.path.join(DSS_DIR, "cas_dtxsid_mapping.csv")


def main() -> None:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(repo_root)
    dss = os.path.join(repo_root, DSS_DIR)
    if not os.path.isdir(dss):
        print(f"DSS folder not found: {dss}", file=sys.stderr)
        sys.exit(1)

    xlsx_files = sorted(
        os.path.join(dss, f)
        for f in os.listdir(dss)
        if f.lower().endswith(".xlsx")
    )
    if not xlsx_files:
        print(f"No .xlsx files in {dss}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading {len(xlsx_files)} Excel files...")
    merged = []
    for path in xlsx_files:
        try:
            df = pd.read_excel(path)
        except Exception as e:
            print(f"  Skip {os.path.basename(path)}: {e}", file=sys.stderr)
            continue
        cols_lower = {c.strip().lower(): c for c in df.columns}
        cas_col = cols_lower.get("casrn") or cols_lower.get("cas")
        dtxsid_col = cols_lower.get("dtxsid") or cols_lower.get("dsstox_substance_id")
        if not cas_col or not dtxsid_col:
            print(f"  Skip {os.path.basename(path)}: no CAS/DTXSID columns", file=sys.stderr)
            continue
        sub = df[[cas_col, dtxsid_col]].dropna(how="all")
        sub = sub.astype(str).apply(lambda s: s.str.strip())
        sub = sub[~sub.iloc[:, 0].str.lower().isin(("nan", "none", ""))]
        sub.columns = ["CASRN", "DTXSID"]
        merged.append(sub)
        print(f"  {os.path.basename(path)}: {len(sub)} rows")

    if not merged:
        print("No data extracted.", file=sys.stderr)
        sys.exit(1)

    out = pd.concat(merged, ignore_index=True).drop_duplicates(subset=["CASRN"], keep="first")
    out.to_csv(OUTPUT_CSV, index=False)
    size_mb = os.path.getsize(OUTPUT_CSV) / (1024 * 1024)
    print(f"Wrote {OUTPUT_CSV} ({len(out)} rows, {size_mb:.1f} MB)")
    if size_mb > 100:
        print("Warning: file > 100 MB; GitHub may reject. Consider keeping only a subset of CAS.", file=sys.stderr)
    else:
        print("Commit this file (without LFS) so Streamlit Cloud can load DTXSID.")


if __name__ == "__main__":
    main()
