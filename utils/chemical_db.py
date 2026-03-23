"""
Unified SQLite database for chemical information.
Combines DSSTox identifiers and ToxValDB toxicity data for fast local lookups.
Use scripts/setup_chemical_db.py to build the DB from DSS CSV and COMPTOX Excel folder.
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
# Status
# ----------------------------------------------------------------------


def get_db_stats() -> dict[str, Any]:
    """Database table existence and row counts."""
    stats = {"dsstox": {"exists": False, "records": 0}, "toxvaldb": {"exists": False, "records": 0, "chemicals": 0}}
    with get_cursor() as cursor:
        if cursor is None:
            return stats
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dsstox'")
        if cursor.fetchone():
            stats["dsstox"]["exists"] = True
            stats["dsstox"]["records"] = cursor.execute("SELECT COUNT(*) FROM dsstox").fetchone()[0]
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='toxvaldb'")
        if cursor.fetchone():
            stats["toxvaldb"]["exists"] = True
            stats["toxvaldb"]["records"] = cursor.execute("SELECT COUNT(*) FROM toxvaldb").fetchone()[0]
            stats["toxvaldb"]["chemicals"] = cursor.execute("SELECT COUNT(DISTINCT dtxsid) FROM toxvaldb").fetchone()[0]
    return stats


def format_toxicity_for_display(records: list[dict]) -> dict[str, list[dict]]:
    """Group toxicity records by study_type for display."""
    out: dict[str, list[dict]] = {}
    for r in records:
        study = r.get("study_type") or "Other"
        out.setdefault(study, []).append(r)
    return out
