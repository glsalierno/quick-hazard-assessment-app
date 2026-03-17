"""
Build a local Carcinogenic Potency Database SQLite from CPDB tab files.

Inputs (read-only, already downloaded by the user):
  - GHhaz3/CPDB/cpdb.chemname.tab.txt
  - GHhaz3/CPDB/cpdb.ncintp.tab.txt
  - GHhaz3/CPDB/cpdb.lit.tab.txt
  - GHhaz3/CPDB/cpdb.ncntdose.tab.txt
  - GHhaz3/CPDB/cpdb.litdose.tab.txt
  - GHhaz3/CPDB/cpdb.species.tab.txt
  - GHhaz3/CPDB/cpdb.strain.tab.txt
  - GHhaz3/CPDB/cpdb.route.tab.txt
  - GHhaz3/CPDB/cpdb.tissue.tab.txt
  - GHhaz3/CPDB/cpdb.tumor.tab.txt
  - GHhaz3/CPDB/cpdb.journal.tab.txt

Output:
  - GHhaz2/data/carcinogenic_potency.sqlite

Schema (high level):
  - cpdb_chemname(chemcode, name, sortordr, cas)
  - cpdb_experiments(idnum, chemcode, name, cas, source, ... all ncintp/lit columns ...)
  - cpdb_doses(idnum, source, dose, dose_order, tumors, total)
  - cpdb_species, cpdb_strain, cpdb_route, cpdb_tissue, cpdb_tumor, cpdb_journal
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
CPDB_DIR = REPO_ROOT.parent / "GHhaz3" / "CPDB"
OUT_DB = REPO_ROOT / "data" / "carcinogenic_potency.sqlite"


def _read_tab(name: str) -> pd.DataFrame:
    """Read a CPDB tab file from GHhaz3/CPDB by base name."""
    path = CPDB_DIR / f"{name}.tab.txt"
    if not path.is_file():
        raise FileNotFoundError(f"CPDB source file not found: {path}")
    return pd.read_csv(path, sep="\t", dtype=str, low_memory=False)


def build_database() -> None:
    """Build the local SQLite database from CPDB tab files."""
    print(f"CPDB directory: {CPDB_DIR}")
    if not CPDB_DIR.is_dir():
        raise SystemExit("CPDB folder not found. Expected at ../GHhaz3/CPDB relative to GHhaz2.")

    OUT_DB.parent.mkdir(parents=True, exist_ok=True)

    print("Reading cpdb.chemname ...")
    chemname = _read_tab("cpdb.chemname")
    # Normalize column names
    chemname.columns = [c.strip().lower() for c in chemname.columns]

    print("Reading main experiment tables (ncintp, lit) ...")
    ncintp = _read_tab("cpdb.ncintp")
    lit = _read_tab("cpdb.lit")

    # Tag sources
    ncintp["source"] = "ncintp"
    lit["source"] = "lit"

    # Normalize column names to lowercase for consistency
    ncintp.columns = [c.strip().lower() for c in ncintp.columns]
    lit.columns = [c.strip().lower() for c in lit.columns]

    # Merge chemname (name, cas) into experiments via chemcode
    print("Merging chemname into experiment tables ...")
    experiments_ncintp = ncintp.merge(
        chemname[["chemcode", "name", "cas"]],
        on="chemcode",
        how="left",
    )
    experiments_lit = lit.merge(
        chemname[["chemcode", "name", "cas"]],
        on="chemcode",
        how="left",
    )
    experiments = pd.concat([experiments_ncintp, experiments_lit], ignore_index=True)

    print(f"Total experiments: {len(experiments):,}")

    print("Reading dose–incidence tables (ncntdose, litdose) ...")
    ncntdose = _read_tab("cpdb.ncntdose")
    litdose = _read_tab("cpdb.litdose")

    # Normalize
    ncntdose.columns = [c.strip().lower() for c in ncntdose.columns]
    litdose.columns = [c.strip().lower() for c in litdose.columns]

    # Rename 'order' -> 'dose_order' to avoid SQL keyword conflicts and clarify meaning
    for df in (ncntdose, litdose):
        if "order" in df.columns:
            df.rename(columns={"order": "dose_order"}, inplace=True)

    ncntdose["source"] = "ncintp"
    litdose["source"] = "lit"

    doses = pd.concat([ncntdose, litdose], ignore_index=True)
    print(f"Total dose rows: {len(doses):,}")

    print("Reading code/appendix tables (species/strain/route/tissue/tumor/journal) ...")
    species = _read_tab("cpdb.species")
    strain = _read_tab("cpdb.strain")
    route = _read_tab("cpdb.route")
    tissue = _read_tab("cpdb.tissue")
    tumor = _read_tab("cpdb.tumor")
    journal = _read_tab("cpdb.journal")

    for df in (species, strain, route, tissue, tumor, journal):
        df.columns = [c.strip().lower() for c in df.columns]

    print(f"Writing SQLite database to: {OUT_DB}")
    conn = sqlite3.connect(str(OUT_DB))

    # Main tables
    chemname.to_sql("cpdb_chemname", conn, if_exists="replace", index=False)
    experiments.to_sql("cpdb_experiments", conn, if_exists="replace", index=False)
    doses.to_sql("cpdb_doses", conn, if_exists="replace", index=False)

    # Code tables
    species.to_sql("cpdb_species", conn, if_exists="replace", index=False)
    strain.to_sql("cpdb_strain", conn, if_exists="replace", index=False)
    route.to_sql("cpdb_route", conn, if_exists="replace", index=False)
    tissue.to_sql("cpdb_tissue", conn, if_exists="replace", index=False)
    tumor.to_sql("cpdb_tumor", conn, if_exists="replace", index=False)
    journal.to_sql("cpdb_journal", conn, if_exists="replace", index=False)

    # Helpful indexes
    print("Creating indexes ...")
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_cpdb_chemname_chemcode ON cpdb_chemname(chemcode);
        CREATE INDEX IF NOT EXISTS idx_cpdb_chemname_cas ON cpdb_chemname(cas);

        CREATE INDEX IF NOT EXISTS idx_cpdb_experiments_idnum ON cpdb_experiments(idnum);
        CREATE INDEX IF NOT EXISTS idx_cpdb_experiments_chemcode ON cpdb_experiments(chemcode);
        CREATE INDEX IF NOT EXISTS idx_cpdb_experiments_cas ON cpdb_experiments(cas);
        CREATE INDEX IF NOT EXISTS idx_cpdb_experiments_source ON cpdb_experiments(source);

        CREATE INDEX IF NOT EXISTS idx_cpdb_doses_idnum ON cpdb_doses(idnum);
        CREATE INDEX IF NOT EXISTS idx_cpdb_doses_source ON cpdb_doses(source);
        """
    )

    conn.commit()
    conn.close()

    print("Done building carcinogenic_potency.sqlite")


if __name__ == "__main__":
    build_database()

