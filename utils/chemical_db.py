"""
Unified SQLite database for chemical information.
Combines DSSTox identifiers, ToxValDB, ECOTOX, ToxRefDB, and CPDB for fast local lookups.
Use scripts/setup_chemical_db.py to build the DB from DSS CSV, COMPTOX Excel, and raw database files.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Any, Optional

import pandas as pd
import streamlit as st

from config import CHEMICAL_DB_PATH, REPO_ROOT


def _db_path() -> str:
    """Resolve DB path: prefer config, then repo/data."""
    if CHEMICAL_DB_PATH and os.path.isfile(CHEMICAL_DB_PATH):
        return CHEMICAL_DB_PATH
    alt = os.path.join(REPO_ROOT, "data", "chemical_db.sqlite")
    return alt if os.path.isfile(alt) else CHEMICAL_DB_PATH


@st.cache_resource
def get_db_connection() -> Optional[sqlite3.Connection]:
    """Cached database connection (shared across sessions)."""
    path = _db_path()
    if not path or not os.path.isfile(path):
        return None
    try:
        return sqlite3.connect(path, check_same_thread=False)
    except Exception:
        return None


@contextmanager
def get_cursor():
    """Context manager for safe database operations."""
    conn = get_db_connection()
    if conn is None:
        yield None
        return
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    finally:
        cursor.close()


# ----------------------------------------------------------------------
# DSSTox (Identifiers)
# ----------------------------------------------------------------------


def create_dsstox_table(csv_path: str, db_path: Optional[str] = None) -> int:
    """
    One-time setup: convert DSSTox CSV to SQLite table.
    """
    db_path = db_path or _db_path() or os.path.join(REPO_ROOT, "data", "chemical_db.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    df.columns = [c.strip().upper() for c in df.columns]

    # Map to standard names; only one source per target (prefer CASRN for cas)
    cas_col = None
    for col in ("CASRN", "CAS", "CAS NUMBER"):
        if col in df.columns:
            cas_col = col
            break
    rename = {}
    if cas_col:
        rename[cas_col] = "cas"
    for std in ("DTXSID", "PREFERRED_NAME", "SYSTEMATIC_NAME", "MOLECULAR_FORMULA",
                "AVERAGE_MASS", "MONOISOTOPIC_MASS", "INCHI", "INCHIKEY", "SMILES"):
        if std in df.columns:
            rename[std] = std.lower()
    df = df.rename(columns=rename)
    keep = [c for c in ["cas", "dtxsid", "preferred_name", "systematic_name", "molecular_formula",
                        "average_mass", "monoisotopic_mass", "inchi", "inchikey", "smiles"] if c in df.columns]
    df = df[keep]

    conn = sqlite3.connect(db_path)
    df.to_sql("dsstox", conn, if_exists="replace", index=False)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dsstox_cas ON dsstox(cas)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dsstox_dtxsid ON dsstox(dtxsid)")
    count = conn.execute("SELECT COUNT(*) FROM dsstox").fetchone()[0]
    conn.close()
    return count


def get_dsstox_by_cas(cas: str) -> Optional[dict[str, Any]]:
    """Get all DSSTox fields for a CAS number."""
    with get_cursor() as cursor:
        if cursor is None:
            return None
        cas = (cas or "").strip()
        cursor.execute("SELECT * FROM dsstox WHERE cas = ?", (cas,))
        row = cursor.fetchone()
        if row:
            cols = [d[0] for d in cursor.description]
            return dict(zip(cols, row))
        cursor.execute("SELECT * FROM dsstox WHERE REPLACE(REPLACE(cas, '-', ''), ' ', '') = ?", (cas.replace("-", "").replace(" ", ""),))
        row = cursor.fetchone()
        if row:
            cols = [d[0] for d in cursor.description]
            return dict(zip(cols, row))
        return None


def get_dsstox_by_dtxsid(dtxsid: str) -> Optional[dict[str, Any]]:
    """Get DSSTox record by DTXSID."""
    with get_cursor() as cursor:
        if cursor is None:
            return None
        cursor.execute("SELECT * FROM dsstox WHERE dtxsid = ?", (dtxsid.strip(),))
        row = cursor.fetchone()
        if row:
            return dict(zip([d[0] for d in cursor.description], row))
        return None


# ----------------------------------------------------------------------
# ToxValDB
# ----------------------------------------------------------------------


def create_toxvaldb_table(excel_path_or_folder: str, db_path: Optional[str] = None) -> int:
    """
    One-time setup: load ToxValDB from a single Excel file or a folder of Excel files.
    Each file can have one or more sheets; we look for columns like dtxsid, casrn, toxval_numeric, study_type.
    """
    db_path = db_path or _db_path() or os.path.join(REPO_ROOT, "data", "chemical_db.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    frames = []
    if os.path.isfile(excel_path_or_folder):
        paths = [excel_path_or_folder]
    elif os.path.isdir(excel_path_or_folder):
        paths = [
            os.path.join(excel_path_or_folder, f)
            for f in sorted(os.listdir(excel_path_or_folder))
            if f.lower().endswith((".xlsx", ".xls"))
        ]
    else:
        return 0

    for path in paths:
        try:
            xl = pd.ExcelFile(path)
            for sheet in xl.sheet_names:
                df = pd.read_excel(path, sheet_name=sheet, dtype=str)
                df.columns = [str(c).lower().strip() for c in df.columns]
                if "dtxsid" not in df.columns and "casrn" not in df.columns:
                    continue
                if "dtxsid" not in df.columns and "casrn" in df.columns:
                    df["dtxsid"] = ""
                frames.append(df)
        except Exception:
            continue

    if not frames:
        return 0
    combined = pd.concat(frames, ignore_index=True)
    if "toxval_numeric" in combined.columns:
        combined["toxval_numeric"] = pd.to_numeric(combined["toxval_numeric"], errors="coerce")
    conn = sqlite3.connect(db_path)
    combined.to_sql("toxvaldb", conn, if_exists="replace", index=False)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_toxval_dtxsid ON toxvaldb(dtxsid)")
    if "casrn" in combined.columns:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_toxval_cas ON toxvaldb(casrn)")
    if "study_type" in combined.columns:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_toxval_study ON toxvaldb(study_type)")
    count = conn.execute("SELECT COUNT(*) FROM toxvaldb").fetchone()[0]
    conn.close()
    return count


def get_toxicity_by_dtxsid(dtxsid: str, numeric_only: bool = True) -> list[dict[str, Any]]:
    """Get toxicity records for a DTXSID."""
    with get_cursor() as cursor:
        if cursor is None:
            return []
        dtxsid = (dtxsid or "").strip()
        if not dtxsid:
            return []
        if numeric_only:
            cursor.execute(
                """
                SELECT * FROM toxvaldb
                WHERE dtxsid = ? AND toxval_numeric IS NOT NULL AND CAST(toxval_numeric AS TEXT) != ''
                ORDER BY study_type
                """,
                (dtxsid,),
            )
        else:
            cursor.execute("SELECT * FROM toxvaldb WHERE dtxsid = ? ORDER BY study_type", (dtxsid,))
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


def get_toxicity_by_cas(cas: str, numeric_only: bool = True) -> list[dict[str, Any]]:
    """Get toxicity by CAS (resolves DTXSID from dsstox first)."""
    dsstox = get_dsstox_by_cas(cas)
    if dsstox and dsstox.get("dtxsid"):
        return get_toxicity_by_dtxsid(dsstox["dtxsid"], numeric_only)
    return []


def get_toxicity_summary(dtxsid: str) -> list[dict[str, Any]]:
    """Summary by study_type for a DTXSID."""
    with get_cursor() as cursor:
        if cursor is None:
            return []
        cursor.execute(
            """
            SELECT study_type,
                   COUNT(*) AS record_count,
                   COUNT(toxval_numeric) AS numeric_count,
                   MIN(CAST(toxval_numeric AS REAL)) AS min_value,
                   MAX(CAST(toxval_numeric AS REAL)) AS max_value
            FROM toxvaldb WHERE dtxsid = ?
            GROUP BY study_type ORDER BY record_count DESC
            """,
            (dtxsid.strip(),),
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ----------------------------------------------------------------------
# ECOTOX (Aquatic / terrestrial toxicity)
# ----------------------------------------------------------------------


def _ensure_ecotox_schema(conn: sqlite3.Connection) -> None:
    """Create ECOTOX table and indexes if they do not exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ecotox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cas TEXT,
            dtxsid TEXT,
            species TEXT,
            endpoint TEXT,
            value_numeric REAL,
            units TEXT,
            duration_days REAL,
            media TEXT,
            organism_group TEXT,
            effect TEXT,
            reference TEXT,
            quality_score INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_ecotox_cas ON ecotox(cas);
        CREATE INDEX IF NOT EXISTS idx_ecotox_dtxsid ON ecotox(dtxsid);
    """)


def create_ecotox_table(df: pd.DataFrame, db_path: Optional[str] = None) -> int:
    """
    Load ECOTOX DataFrame into SQLite. Creates table if not exists.
    df should have columns: cas, dtxsid (optional), species, endpoint, value_numeric, units,
    duration_days, media, organism_group, effect, reference, quality_score (optional).
    """
    db_path = db_path or _db_path() or os.path.join(REPO_ROOT, "data", "chemical_db.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    _ensure_ecotox_schema(conn)
    schema_cols = ["cas", "dtxsid", "species", "endpoint", "value_numeric", "units",
                   "duration_days", "media", "organism_group", "effect", "reference", "quality_score"]
    out = df.copy()
    out.columns = [str(c).lower().strip().replace(" ", "_") for c in out.columns]
    keep = [c for c in schema_cols if c in out.columns]
    out[keep].to_sql("ecotox", conn, if_exists="replace", index=False)
    count = conn.execute("SELECT COUNT(*) FROM ecotox").fetchone()[0]
    conn.close()
    return count


def get_ecotox_data(cas: Optional[str] = None, dtxsid: Optional[str] = None, organism_group: Optional[str] = None) -> list[dict[str, Any]]:
    """Retrieve ECOTOX data for a chemical. Filter by organism_group if provided."""
    with get_cursor() as cursor:
        if cursor is None:
            return []
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ecotox'")
        if not cursor.fetchone():
            return []
        query = "SELECT * FROM ecotox WHERE "
        params: list[Any] = []
        if dtxsid:
            query += "dtxsid = ?"
            params.append(dtxsid)
        elif cas:
            query += "cas = ?"
            params.append(cas)
        else:
            return []
        if organism_group:
            query += " AND organism_group = ?"
            params.append(organism_group)
        query += " ORDER BY endpoint, species LIMIT 100"
        cursor.execute(query, params)
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


# ----------------------------------------------------------------------
# ToxRefDB (Chronic / cancer studies)
# ----------------------------------------------------------------------


def _ensure_toxrefdb_schema(conn: sqlite3.Connection) -> None:
    """Create ToxRefDB table and indexes if they do not exist."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS toxrefdb (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cas TEXT,
            dtxsid TEXT,
            study_type TEXT,
            species TEXT,
            route TEXT,
            critical_effect TEXT,
            NOAEL REAL,
            NOAEL_units TEXT,
            LOAEL REAL,
            LOAEL_units TEXT,
            study_duration TEXT,
            tumor_site TEXT,
            reference TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_toxrefdb_cas ON toxrefdb(cas);
        CREATE INDEX IF NOT EXISTS idx_toxrefdb_dtxsid ON toxrefdb(dtxsid);
        """
    )


def create_toxrefdb_table(df: pd.DataFrame, db_path: Optional[str] = None) -> int:
    """
    Load a ToxRefDB-like DataFrame into SQLite. Creates table if not exists.
    The DataFrame may come from the original Excel (v2.x) or from the v3.0 POD CSV.
    Expected columns (where available): cas, dtxsid, study_type, species, route,
    critical_effect, NOAEL, NOAEL_units, LOAEL, LOAEL_units, study_duration,
    tumor_site, reference.
    """
    db_path = db_path or _db_path() or os.path.join(REPO_ROOT, "data", "chemical_db.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    _ensure_toxrefdb_schema(conn)
    out = df.copy()
    # Normalize column names
    out.columns = [str(c).strip().lower() for c in out.columns]
    # If coming from v3.0 POD CSV, derive our schema columns
    if "calc_pod_type" in out.columns and "mg_kg_day_value" in out.columns:
        # Map DSSTox substance ID to dtxsid
        if "dsstox_substance_id" in out.columns and "dtxsid" not in out.columns:
            out["dtxsid"] = out["dsstox_substance_id"]
        # NOAEL / LOAEL from calc_pod_type
        out["noael"] = out.apply(
            lambda r: r.get("mg_kg_day_value")
            if str(r.get("calc_pod_type") or "").upper() == "NOAEL"
            else None,
            axis=1,
        )
        out["loael"] = out.apply(
            lambda r: r.get("mg_kg_day_value")
            if str(r.get("calc_pod_type") or "").upper() == "LOAEL"
            else None,
            axis=1,
        )
        out["noael_units"] = out.get("mg_kg_day_value").map(lambda _: "mg/kg-day")
        out["loael_units"] = out.get("mg_kg_day_value").map(lambda _: "mg/kg-day")
        # Critical effect from toxval_effect_list (string of endpoints)
        if "toxval_effect_list" in out.columns and "critical_effect" not in out.columns:
            out["critical_effect"] = out["toxval_effect_list"]
        # Species and route columns already present in v3.0 POD
        # Study duration as a simple text range
        def _duration(row: pd.Series) -> str:
            start = row.get("dose_start")
            start_unit = row.get("dose_start_unit")
            end = row.get("dose_end")
            end_unit = row.get("dose_end_unit")
            if pd.isna(start) and pd.isna(end):
                return ""
            parts = []
            if pd.notna(start):
                parts.append(f"{start} {start_unit or ''}".strip())
            if pd.notna(end):
                parts.append(f"{end} {end_unit or ''}".strip())
            return " – ".join(parts)

        out["study_duration"] = out.apply(_duration, axis=1)
        # Reference from study_citation
        if "study_citation" in out.columns and "reference" not in out.columns:
            out["reference"] = out["study_citation"]

    # Keep only schema columns
    keep_cols = [
        "cas",
        "dtxsid",
        "study_type",
        "species",
        "route",
        "critical_effect",
        "noael",
        "noael_units",
        "loael",
        "loael_units",
        "study_duration",
        "tumor_site",
        "reference",
    ]
    present = [c for c in keep_cols if c in out.columns]
    out = out[present]
    out.to_sql("toxrefdb", conn, if_exists="replace", index=False)
    count = conn.execute("SELECT COUNT(*) FROM toxrefdb").fetchone()[0]
    conn.close()
    return count


def get_toxrefdb_data(cas: Optional[str] = None, dtxsid: Optional[str] = None) -> list[dict[str, Any]]:
    """Get ToxRefDB chronic/cancer data for a chemical."""
    with get_cursor() as cursor:
        if cursor is None:
            return []
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='toxrefdb'")
        if not cursor.fetchone():
            return []
        query = "SELECT * FROM toxrefdb WHERE "
        params: list[Any] = []
        if dtxsid:
            query += "dtxsid = ?"
            params.append(dtxsid)
        elif cas:
            query += "cas = ?"
            params.append(cas)
        else:
            return []
        cursor.execute(query, params)
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


# ----------------------------------------------------------------------
# CPDB (Carcinogenic Potency Database)
# ----------------------------------------------------------------------


def _ensure_cpdb_schema(conn: sqlite3.Connection) -> None:
    """Create CPDB table and indexes if they do not exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cpdb (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cas TEXT,
            name TEXT,
            species TEXT,
            strain TEXT,
            sex TEXT,
            route TEXT,
            tumor_site TEXT,
            TD50_mg_per_kg REAL,
            TD50_lower REAL,
            TD50_upper REAL,
            carcinogenicity_rating TEXT,
            reference TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_cpdb_cas ON cpdb(cas);
    """)


def create_cpdb_table(df: pd.DataFrame, db_path: Optional[str] = None) -> int:
    """Load CPDB DataFrame into SQLite. Creates table if not exists."""
    db_path = db_path or _db_path() or os.path.join(REPO_ROOT, "data", "chemical_db.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    _ensure_cpdb_schema(conn)
    out = df.copy()
    out.columns = [str(c).lower().strip() for c in out.columns]
    out.to_sql("cpdb", conn, if_exists="replace", index=False)
    count = conn.execute("SELECT COUNT(*) FROM cpdb").fetchone()[0]
    conn.close()
    return count


def get_cpdb_data(cas: Optional[str] = None) -> list[dict[str, Any]]:
    """Get CPDB carcinogenicity data for a chemical."""
    if not cas:
        return []
    with get_cursor() as cursor:
        if cursor is None:
            return []
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cpdb'")
        if not cursor.fetchone():
            return []
        cursor.execute("SELECT * FROM cpdb WHERE cas = ?", (cas.strip(),))
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_all_toxicity_data(cas: Optional[str] = None, dtxsid: Optional[str] = None) -> dict[str, Any]:
    """
    Unified function to get all toxicity data from ToxValDB, ECOTOX, ToxRefDB, and CPDB.
    Resolves DTXSID from CAS if only CAS is provided.
    """
    if not cas and not dtxsid:
        return {}
    if cas and not dtxsid:
        dsstox = get_dsstox_by_cas(cas)
        dtxsid = dsstox.get("dtxsid") if dsstox else None
    return {
        "toxvaldb": get_toxicity_by_dtxsid(dtxsid or "", numeric_only=False) if dtxsid else [],
        "ecotox": get_ecotox_data(cas=cas, dtxsid=dtxsid),
        "toxrefdb": get_toxrefdb_data(cas=cas, dtxsid=dtxsid),
        "cpdb": get_cpdb_data(cas=cas),
        "sources": {
            "toxvaldb": bool(dtxsid),
            "ecotox": bool(cas or dtxsid),
            "toxrefdb": bool(cas or dtxsid),
            "cpdb": bool(cas),
        },
    }


# ----------------------------------------------------------------------
# Status
# ----------------------------------------------------------------------


def get_db_stats() -> dict[str, Any]:
    """Database table existence and row counts (including ECOTOX, ToxRefDB, CPDB)."""
    stats = {
        "dsstox": {"exists": False, "records": 0},
        "toxvaldb": {"exists": False, "records": 0, "chemicals": 0},
        "ecotox": {"exists": False, "records": 0},
        "toxrefdb": {"exists": False, "records": 0},
        "cpdb": {"exists": False, "records": 0},
    }
    with get_cursor() as cursor:
        if cursor is None:
            return stats
        for table, key in [("dsstox", "dsstox"), ("toxvaldb", "toxvaldb"), ("ecotox", "ecotox"), ("toxrefdb", "toxrefdb"), ("cpdb", "cpdb")]:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            if cursor.fetchone():
                stats[key]["exists"] = True
                stats[key]["records"] = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if stats["toxvaldb"]["exists"]:
            stats["toxvaldb"]["chemicals"] = cursor.execute("SELECT COUNT(DISTINCT dtxsid) FROM toxvaldb").fetchone()[0]
    return stats


def format_toxicity_for_display(records: list[dict]) -> dict[str, list[dict]]:
    """Group toxicity records by study_type for display."""
    out: dict[str, list[dict]] = {}
    for r in records:
        study = r.get("study_type") or "Other"
        out.setdefault(study, []).append(r)
    return out
