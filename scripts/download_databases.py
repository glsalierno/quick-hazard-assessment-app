"""
Download and process QSAR Toolbox databases for local integration.
Run this to fetch latest versions before building SQLite with scripts/setup_chemical_db.py.

Sources:
  - ECOTOX: https://cfpub.epa.gov/ecotox/ (may require registration)
  - ToxRefDB: https://www.epa.gov/sites/default/files/2016-10/toxrefdb_v2_0.xlsx
  - CPDB: https://files.toxplanet.com/cpdb/cpdb_excel.zip (or current CPDB source)
"""

from __future__ import annotations

import os
import zipfile
from pathlib import Path

import pandas as pd
import requests

# Use repo root (parent of scripts/)
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "raw_databases"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def download_toxrefdb() -> Path | None:
    """Download ToxRefDB from EPA. Returns path to saved file or None on failure."""
    url = "https://www.epa.gov/sites/default/files/2016-10/toxrefdb_v2_0.xlsx"
    out_path = DATA_DIR / "toxrefdb_v2_0.xlsx"
    print("Downloading ToxRefDB...")
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        out_path.write_bytes(resp.content)
        print(f"  -> {out_path}")
        return out_path
    except Exception as e:
        print(f"  Failed: {e}")
        return None


def download_cpdb() -> Path | None:
    """Download CPDB from ToxPlanet (or fallback). Returns path to extracted folder or None."""
    url = "https://files.toxplanet.com/cpdb/cpdb_excel.zip"
    zip_path = DATA_DIR / "cpdb.zip"
    out_dir = DATA_DIR / "cpdb"
    print("Downloading CPDB...")
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        zip_path.write_bytes(resp.content)
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(out_dir)
        print(f"  -> {out_dir}")
        return out_dir
    except Exception as e:
        print(f"  Failed: {e}")
        return None


def download_ecotox() -> Path | None:
    """
    ECOTOX requires registration at https://cfpub.epa.gov/ecotox/.
    Place downloaded Excel/CSV in data/raw_databases/ecotox/ and return that path.
    """
    ecotox_dir = DATA_DIR / "ecotox"
    ecotox_dir.mkdir(parents=True, exist_ok=True)
    # Check for user-placed file
    for name in ("ecotox_results.xlsx", "ecotox_results.csv", "ECOTOX*.xlsx", "ECOTOX*.csv"):
        for p in ecotox_dir.glob(name.replace("*", "*")):
            if p.is_file():
                print(f"Using existing ECOTOX file: {p}")
                return p
    print("ECOTOX: No local file found. Download from https://cfpub.epa.gov/ecotox/ and place in data/raw_databases/ecotox/")
    return None


def parse_ecotox_to_dataframe(path: Path) -> pd.DataFrame:
    """
    Parse ECOTOX Excel or CSV into a DataFrame with standard column names.
    Adjust sheet_name/columns to match actual ECOTOX export format.
    """
    path = Path(path)
    if not path.is_file():
        return pd.DataFrame()
    try:
        if path.suffix.lower() in (".xlsx", ".xls"):
            # Try common sheet names
            xl = pd.ExcelFile(path)
            sheet = xl.sheet_names[0] if xl.sheet_names else None
            df = pd.read_excel(path, sheet_name=sheet, header=0)
        else:
            df = pd.read_csv(path, low_memory=False)
    except Exception as e:
        print(f"  Parse error: {e}")
        return pd.DataFrame()
    df.columns = [str(c).strip() for c in df.columns]
    # Map common ECOTOX column names to schema
    rename = {}
    for col in df.columns:
        c = col.lower()
        if "cas" in c and "number" in c:
            rename[col] = "cas"
        elif col.lower() == "species":
            rename[col] = "species"
        elif "endpoint" in c or col.lower() == "endpoint":
            rename[col] = "endpoint"
        elif "value" in c and "numeric" not in c:
            rename[col] = "value_numeric"
        elif "value" in c:
            rename[col] = "value_numeric"
        elif "unit" in c:
            rename[col] = "units"
        elif "duration" in c and "day" in c:
            rename[col] = "duration_days"
        elif "media" in c or "medium" in c:
            rename[col] = "media"
        elif "organism" in c and "group" in c:
            rename[col] = "organism_group"
        elif "effect" in c:
            rename[col] = "effect"
        elif "reference" in c:
            rename[col] = "reference"
    df = df.rename(columns=rename)
    if "value_numeric" in df.columns:
        df["value_numeric"] = pd.to_numeric(df["value_numeric"], errors="coerce")
    if "duration_days" in df.columns:
        df["duration_days"] = pd.to_numeric(df["duration_days"], errors="coerce")
    if "cas" not in df.columns and "CAS Number" in df.columns:
        df["cas"] = df["CAS Number"]
    return df


def parse_toxrefdb_to_dataframe(path: Path) -> pd.DataFrame:
    """Parse ToxRefDB Excel into DataFrame with standard column names."""
    path = Path(path)
    if not path.is_file():
        return pd.DataFrame()
    try:
        df = pd.read_excel(path, sheet_name=0, header=0)
    except Exception as e:
        print(f"  Parse error: {e}")
        return pd.DataFrame()
    df.columns = [str(c).strip() for c in df.columns]
    rename = {
        "CASRN": "cas",
        "CAS": "cas",
        "DTXSID": "dtxsid",
        "Species": "species",
        "Exposure Route": "route",
        "Study Type": "study_type",
        "Critical Effect": "critical_effect",
        "NOAEL (mg/kg-day)": "NOAEL",
        "NOAEL": "NOAEL",
        "LOAEL (mg/kg-day)": "LOAEL",
        "LOAEL": "LOAEL",
        "Study Duration": "study_duration",
        "Tumor Site": "tumor_site",
        "Reference": "reference",
    }
    for old, new in list(rename.items()):
        if old in df.columns:
            df = df.rename(columns={old: new})
    df.columns = [c.lower().strip() for c in df.columns]
    return df


def parse_cpdb_to_dataframe(path: Path) -> pd.DataFrame:
    """Parse CPDB Excel or CSV into DataFrame. path can be file or directory."""
    path = Path(path)
    if path.is_dir():
        files = list(path.glob("*.xlsx")) + list(path.glob("*.xls")) + list(path.glob("*.csv"))
        path = files[0] if files else None
    if not path or not path.is_file():
        return pd.DataFrame()
    try:
        if path.suffix.lower() in (".xlsx", ".xls"):
            df = pd.read_excel(path, sheet_name=0, header=0)
        else:
            df = pd.read_csv(path, low_memory=False)
    except Exception as e:
        print(f"  Parse error: {e}")
        return pd.DataFrame()
    df.columns = [str(c).strip() for c in df.columns]
    rename = {}
    for col in df.columns:
        c = col.lower()
        if "cas" in c:
            rename[col] = "cas"
        elif "species" in c:
            rename[col] = "species"
        elif "strain" in c:
            rename[col] = "strain"
        elif "sex" in c:
            rename[col] = "sex"
        elif "route" in c:
            rename[col] = "route"
        elif "tumor" in c and "site" in c:
            rename[col] = "tumor_site"
        elif "td50" in c and "mg" in c:
            rename[col] = "TD50_mg_per_kg"
        elif "lower" in c and "ci" in c:
            rename[col] = "TD50_lower"
        elif "upper" in c and "ci" in c:
            rename[col] = "TD50_upper"
        elif "carcinogen" in c:
            rename[col] = "carcinogenicity_rating"
        elif "reference" in c:
            rename[col] = "reference"
        elif "name" in c and "chemical" in c:
            rename[col] = "name"
    df = df.rename(columns=rename)
    df.columns = [c.lower().strip() if c != "TD50_mg_per_kg" else "TD50_mg_per_kg" for c in df.columns]
    return df


def main() -> None:
    print("Downloading QSAR Toolbox databases")
    print("=" * 50)
    download_toxrefdb()
    download_cpdb()
    download_ecotox()
    print("Done. Run scripts/setup_chemical_db.py to build SQLite.")


if __name__ == "__main__":
    main()
