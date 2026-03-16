"""
One-time setup to build the unified chemical SQLite database.
Run from repo root: python scripts/setup_chemical_db.py

Uses:
  - DSS/cas_dtxsid_mapping.csv (or first valid DSS CSV) for DSSTox
  - COMPTOX_Public (Data Excel Files Folder)/Data Excel Files/*.xlsx for ToxValDB
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import COMPTOX_EXCEL_DIR, DSS_PATH, DSSTOX_MAPPING_FILENAMES
from utils.chemical_db import create_dsstox_table, create_toxvaldb_table, get_db_stats


def main() -> None:
    print("Chemical database setup")
    print("=" * 50)

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(repo, "data")
    db_path = os.path.join(data_dir, "chemical_db.sqlite")
    os.makedirs(data_dir, exist_ok=True)

    # DSSTox: try configured CSV then any DSS CSV
    dsstox_csv = None
    for name in DSSTOX_MAPPING_FILENAMES:
        p = os.path.join(DSS_PATH, name)
        if os.path.isfile(p):
            dsstox_csv = p
            break
    if not dsstox_csv and os.path.isdir(DSS_PATH):
        for f in sorted(os.listdir(DSS_PATH)):
            if f.lower().endswith(".csv"):
                dsstox_csv = os.path.join(DSS_PATH, f)
                break

    if dsstox_csv:
        print(f"DSSTox CSV: {dsstox_csv}")
        n = create_dsstox_table(dsstox_csv, db_path=db_path)
        print(f"  -> {n} DSSTox records")
    else:
        print("DSSTox CSV not found (skip).")

    # ToxValDB: COMPTOX Excel folder
    if os.path.isdir(COMPTOX_EXCEL_DIR):
        print(f"ToxValDB Excel folder: {COMPTOX_EXCEL_DIR}")
        n = create_toxvaldb_table(COMPTOX_EXCEL_DIR, db_path=db_path)
        print(f"  -> {n} ToxValDB records")
    else:
        print("ToxValDB Excel folder not found (skip).")

    stats = get_db_stats()
    print()
    print("Database stats:")
    print(f"  DSSTox:  {stats['dsstox']['records']} records")
    print(f"  ToxValDB: {stats['toxvaldb']['records']} records, {stats['toxvaldb']['chemicals']} chemicals")
    print(f"  DB path: {db_path}")


if __name__ == "__main__":
    main()
