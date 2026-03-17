"""
One-time setup to build the unified chemical SQLite database.
Run from repo root: python scripts/setup_chemical_db.py

Uses:
  - DSS/cas_dtxsid_mapping.csv (or first valid DSS CSV) for DSSTox
  - COMPTOX_Public (Data Excel Files Folder)/Data Excel Files/*.xlsx for ToxValDB
  - data/raw_databases/ for ECOTOX, ToxRefDB, CPDB (run scripts/download_databases.py first)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import COMPTOX_EXCEL_DIR, DSS_PATH, DSSTOX_MAPPING_FILENAMES, RAW_DATABASES_DIR
from utils.chemical_db import (
    create_dsstox_table,
    create_toxvaldb_table,
    create_ecotox_table,
    create_toxrefdb_table,
    create_cpdb_table,
    get_db_stats,
)

# Optional: parsers from download script (same directory; insert after repo so config/utils still resolve)
_scripts_dir = os.path.dirname(os.path.abspath(__file__))
if _scripts_dir not in sys.path:
    sys.path.insert(1, _scripts_dir)
try:
    from download_databases import (
        parse_ecotox_to_dataframe,
        parse_toxrefdb_to_dataframe,
        parse_cpdb_to_dataframe,
    )
except ImportError:
    parse_ecotox_to_dataframe = parse_toxrefdb_to_dataframe = parse_cpdb_to_dataframe = None


def main() -> None:
    print("Chemical database setup")
    print("=" * 50)

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(repo, "data")
    db_path = os.path.join(data_dir, "chemical_db.sqlite")
    raw_dir = Path(RAW_DATABASES_DIR) if RAW_DATABASES_DIR else Path(data_dir) / "raw_databases"
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

    # ECOTOX, ToxRefDB, CPDB from raw_databases (optional)
    if parse_ecotox_to_dataframe and raw_dir.is_dir():
        ecotox_path = raw_dir / "ecotox"
        if ecotox_path.is_dir():
            for p in list(ecotox_path.glob("*.xlsx")) + list(ecotox_path.glob("*.csv")):
                df = parse_ecotox_to_dataframe(p)
                if not df.empty:
                    print(f"ECOTOX: {p.name}")
                    n = create_ecotox_table(df, db_path=db_path)
                    print(f"  -> {n} ECOTOX records")
                    break
        toxref_file = raw_dir / "toxrefdb_v2_0.xlsx"
        if toxref_file.is_file():
            df = parse_toxrefdb_to_dataframe(toxref_file)
            if not df.empty:
                print(f"ToxRefDB: {toxref_file.name}")
                n = create_toxrefdb_table(df, db_path=db_path)
                print(f"  -> {n} ToxRefDB records")
        cpdb_dir = raw_dir / "cpdb"
        if cpdb_dir.is_dir():
            for p in list(cpdb_dir.glob("*.xlsx")) + list(cpdb_dir.glob("*.csv")):
                df = parse_cpdb_to_dataframe(p)
                if not df.empty:
                    print(f"CPDB: {p.name}")
                    n = create_cpdb_table(df, db_path=db_path)
                    print(f"  -> {n} CPDB records")
                    break
        elif (raw_dir / "cpdb.zip").is_file():
            # Already extracted by download script
            pass

    stats = get_db_stats()
    print()
    print("Database stats:")
    print(f"  DSSTox:   {stats['dsstox']['records']} records")
    print(f"  ToxValDB: {stats['toxvaldb']['records']} records, {stats['toxvaldb']['chemicals']} chemicals")
    if stats.get("ecotox", {}).get("exists"):
        print(f"  ECOTOX:   {stats['ecotox']['records']} records")
    if stats.get("toxrefdb", {}).get("exists"):
        print(f"  ToxRefDB: {stats['toxrefdb']['records']} records")
    if stats.get("cpdb", {}).get("exists"):
        print(f"  CPDB:     {stats['cpdb']['records']} records")
    print(f"  DB path:  {db_path}")


if __name__ == "__main__":
    main()
