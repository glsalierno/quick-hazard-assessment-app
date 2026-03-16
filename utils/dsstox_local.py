"""
DSSTox local mapping loader with Streamlit caching.
Loads CAS → DTXSID from the DSS/ folder (CSV or Excel). No API key required.
Returns None if no file is found (PubChem-only mode).

Note: On Streamlit Community Cloud, Git LFS files are not pulled by default,
so .xlsx in the repo may be LFS pointers. The loader skips pointer files and
can use a non-LFS CSV (e.g. cas_dtxsid_mapping.csv) if committed without LFS.
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd
import streamlit as st

from config import DSS_DIR, DSS_PATH, DSSTOX_MAPPING_FILENAMES


def _dss_dir_resolved() -> str:
    """Resolve DSS directory: prefer repo root (for Streamlit Cloud), then cwd, then utils parent."""
    for base in (DSS_PATH, os.path.join(os.getcwd(), DSS_DIR), os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), DSS_DIR)):
        candidate = os.path.abspath(base) if base else ""
        if candidate and os.path.isdir(candidate):
            return candidate
    return os.path.abspath(DSS_DIR)


def _is_lfs_pointer(path: str) -> bool:
    """Return True if the file is a Git LFS pointer (not real content)."""
    try:
        with open(path, "rb") as f:
            head = f.read(200).decode("utf-8", errors="ignore")
        return "git-lfs" in head or "oid sha256" in head
    except Exception:
        return False


def _find_mapping_files() -> list[str]:
    """Return paths to all DSSTox mapping files in DSS/ (CSV and Excel), sorted by name."""
    dss = _dss_dir_resolved()
    if not os.path.isdir(dss):
        return []
    paths = []
    # Prefer configured filenames first (e.g. cas_dtxsid_mapping.csv – often committed without LFS for Cloud)
    for name in DSSTOX_MAPPING_FILENAMES:
        path = os.path.join(dss, name)
        if os.path.isfile(path):
            paths.append(path)
    # Then any .csv or .xlsx in DSS/ (sorted so order is deterministic)
    for name in sorted(os.listdir(dss)):
        if name.lower().endswith(".csv") or name.lower().endswith(".xlsx"):
            path = os.path.join(dss, name)
            if path not in paths:
                paths.append(path)
    return sorted(paths)


# Preferred name column variants (from EPA/DSSTox dumps)
NAME_COLS = ("preferred_name", "preferredname", "substance_name", "preferred chemical name", "chemical_name", "chemical name")


def _load_one_mapping(path: str) -> dict[str, dict] | None:
    """Load a single CSV/Excel file into CAS -> {dtxsid, preferred_name}. Returns None on skip/error."""
    try:
        if _is_lfs_pointer(path):
            return None
        if path.lower().endswith(".xlsx"):
            df = pd.read_excel(path)
            return _df_to_cas_dtxsid(df)
        result = {}
        for chunk in pd.read_csv(path, chunksize=100_000, dtype=str, on_bad_lines="skip"):
            chunk_dict = _df_to_cas_dtxsid(chunk)
            if chunk_dict:
                result.update(chunk_dict)
        return result if result else None
    except Exception:
        return None


def _df_to_cas_dtxsid(df: pd.DataFrame) -> dict[str, dict] | None:
    """Extract CAS -> {dtxsid, preferred_name} from a dataframe. Returns None if CAS/DTXSID columns missing."""
    cols_lower = {c.strip().lower(): c for c in df.columns}
    cas_col = cols_lower.get("casrn") or cols_lower.get("cas")
    dtxsid_col = cols_lower.get("dtxsid") or cols_lower.get("dsstox_substance_id")
    name_col = next((cols_lower.get(c) for c in NAME_COLS if c in cols_lower), None)
    if cas_col is None or dtxsid_col is None:
        return None
    cas_series = df[cas_col].astype(str).str.strip()
    dtxsid_series = df[dtxsid_col].astype(str).str.strip()
    name_series = df[name_col].astype(str).str.strip() if name_col else pd.Series([""] * len(df))
    mask = (cas_series.str.len() > 0) & (~cas_series.str.lower().isin(("nan", "none", "")))
    cas_series = cas_series[mask]
    dtxsid_series = dtxsid_series[mask]
    name_series = name_series[mask]
    out = {}
    for cas, dtxsid, name in zip(cas_series, dtxsid_series, name_series):
        if cas not in out:
            out[cas] = {"dtxsid": dtxsid, "preferred_name": name if name and name.lower() not in ("nan", "none", "") else None}
    return out


@st.cache_data
def load_dsstox_mapping():
    """
    Load DSSTox mapping from all CSV/Excel files in DSS/ with caching.
    Returns dict mapping CAS (str) -> {dtxsid, preferred_name}, or None if no valid file.
    """
    paths = _find_mapping_files()
    if not paths:
        return None
    merged = {}
    for mapping_path in paths:
        one = _load_one_mapping(mapping_path)
        if one:
            for cas, rec in one.items():
                if cas not in merged:
                    merged[cas] = dict(rec)
                elif isinstance(merged[cas], dict) and rec.get("preferred_name") and not merged[cas].get("preferred_name"):
                    merged[cas]["preferred_name"] = rec["preferred_name"]
    return merged if merged else None


def _get_record(cas_number: str, mapping_dict: Optional[dict]) -> Optional[dict]:
    """Resolve CAS to record {dtxsid, preferred_name}. Handles dashed/undashed CAS."""
    if not mapping_dict or not cas_number:
        return None
    key = str(cas_number).strip()
    if key in mapping_dict:
        return mapping_dict[key] if isinstance(mapping_dict[key], dict) else {"dtxsid": mapping_dict[key], "preferred_name": None}
    key_compact = key.replace("-", "")
    for k, v in mapping_dict.items():
        if k.replace("-", "") == key_compact:
            return v if isinstance(v, dict) else {"dtxsid": v, "preferred_name": None}
    return None


def get_dtxsid(cas_number: str, mapping_dict: Optional[dict]) -> Optional[str]:
    """Look up DTXSID from local mapping."""
    rec = _get_record(cas_number, mapping_dict)
    return rec.get("dtxsid") if isinstance(rec, dict) else rec if isinstance(rec, str) else None


def get_preferred_name(cas_number: str, mapping_dict: Optional[dict]) -> Optional[str]:
    """Look up preferred name from local DSSTox mapping (when column present)."""
    rec = _get_record(cas_number, mapping_dict)
    return rec.get("preferred_name") if isinstance(rec, dict) else None
